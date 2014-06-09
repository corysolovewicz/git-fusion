#! /usr/bin/env python3.3
"""copy_git_changes_to_p4"""

import os
from   collections                  import defaultdict, namedtuple
import copy
import pprint
import shutil
import logging
import re
import traceback
import sys

import pygit2

import P4

import p4gf_branch
import p4gf_call_git
import p4gf_config
import p4gf_const
from   p4gf_changelist_data_file    import ChangelistDataFile
import p4gf_depot_branch
from   p4gf_desc_info               import DescInfo
import p4gf_fastexport
from   p4gf_fastimport_mark         import Mark
from   p4gf_g2p_matrix              import G2PMatrix
from   p4gf_g2p_matrix2             import G2PMatrix as G2PMatrix2
import p4gf_g2p_matrix_dump
import p4gf_gc
from   p4gf_l10n                    import _, NTR
import p4gf_lock
import p4gf_log
from   p4gf_object_type             import ObjectType
from   p4gf_p4changelist            import P4Changelist
import p4gf_p4filetype
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_path
import p4gf_proc
from   p4gf_profiler                import Timer
import p4gf_progress_reporter as ProgressReporter
import p4gf_protect
import p4gf_usermap
import p4gf_util
import p4gf_version


LOG = logging.getLogger(__name__)

UNICODE_TYPES = ['unicode', 'xunicode', 'utf16', 'xutf16']

# Assume the cached objects will not be mapped into the repository, and
# skip permission checks on those paths since they're submitted as the
# git-fusion-user anyway.
P4GF_DEPOT_OBJECTS_RE = re.compile('//' + p4gf_const.P4GF_DEPOT + '/objects/')
P4GF_DEPOT_BRANCHES_RE = re.compile('//' + p4gf_const.P4GF_DEPOT + '/branches/')

# pylint: disable=R0912
# pylint: disable=R0914
# pylint: disable=R0915
# R0912 Too many branches
# R0914 Too many local variables
# R0915 Too many statements
#
# Yeah, there are some huge functions in here that might benefit from a refactor.
# Or not: sometimes it's easier to follow one giant, mostly linear, function,
# than to hop around from function to function. We'll see. Later.

def _print_error(msg):
    """Print the given message to the error stream, as well as to the log.
    """
    sys.stderr.write(msg + '\n')
    LOG.error(msg)


def extract_jobs(desc):
    """Scan the commit description looking for "Jobs:" and extracting the job
    identifiers following the field label. Returns None if no jobs found.
    """
    if not desc:
        return None
    lines = desc.splitlines()
    for i in range(0, len(lines)):
        line = lines[i].strip()
        if line.startswith("Jobs:"):
            jobs = []
            line = line[5:]
            if line:
                jobs.append(line.strip())
            for i in range(i + 1, len(lines)):
                line = lines[i].strip()
                if not line or ' ' in line or ':' in line:
                    # reached the end of the job identifiers
                    break
                # whatever is left of the line is a job identifier
                jobs.append(line)
            return jobs
    return None


def is_p4d_printable(c):
    '''
    P4D rejects "non-printable" characters with
      ErrorId MsgDm::IdNonPrint = { ErrorOf( ES_DM, 6, E_FAILED, EV_USAGE, 1 )
      "Non-printable characters not allowed in '%id%'." } ;

    Where "character" here means C "char" aka "byte"
    and non-printable means 0x00-0x1f or 0x7f
    '''
    if ord(c) < 0x20:
        return False
    if ord(c) == 0x7F:
        return False
    return True


def check_valid_filename(name, ctx):
    """Test the given name for illegal characters, returning None if okay,
    otherwise an error message. Illegal characters and sequences include:
    [...]
    """
    for c in name:
        if not is_p4d_printable(c):
            return _("Non-printable characters not allowed in Perforce: {}")\
                   .format(name)
    if '...' in name:
        return _("bad filename: '{}'").format(name)
    if 'P4D/NT' in ctx.server_version:
        if ':' in name:
            return _("unsupported filename on windows: {}").format(name)
    # This should usually be en_US.UTF-8 which also needs to be defined
    # on the os
    # pylint: disable=W0612
    encoding = sys.getfilesystemencoding()
    try:
        encoded_name = name.encode(encoding, "strict")
    except UnicodeEncodeError:
        return _("Cannot convert filename to '{}': {}").format(encoding, name)
    return None


# p4d treats many failures to open a file for {add, edit, delete, others}
# not as an E_FAILED error, but as an E_INFO "oh by the way I totally failed
# to do what you want.
#
MSGID_CANNOT_OPEN = [ p4gf_p4msgid.MsgDm_LockSuccess
                    , p4gf_p4msgid.MsgDm_LockAlready
                    , p4gf_p4msgid.MsgDm_LockAlreadyOther
                    , p4gf_p4msgid.MsgDm_LockNoPermission
                    , p4gf_p4msgid.MsgDm_LockBadUnicode
                    , p4gf_p4msgid.MsgDm_LockUtf16NotSupp
                    , p4gf_p4msgid.MsgDm_UnLockSuccess
                    , p4gf_p4msgid.MsgDm_UnLockAlready
                    , p4gf_p4msgid.MsgDm_UnLockAlreadyOther
                    , p4gf_p4msgid.MsgDm_OpenIsLocked
                    , p4gf_p4msgid.MsgDm_OpenXOpened
                    , p4gf_p4msgid.MsgDm_IntegXOpened
                    , p4gf_p4msgid.MsgDm_OpenWarnOpenStream
                    , p4gf_p4msgid.MsgDm_IntegMovedUnmapped
                    , p4gf_p4msgid.MsgDm_ExVIEW
                    , p4gf_p4msgid.MsgDm_ExVIEW2
                    , p4gf_p4msgid.MsgDm_ExPROTECT
                    , p4gf_p4msgid.MsgDm_ExPROTECT2
                    ]

# This subset of MSGID_CANNOT_OPEN identifies which errors are "current
# user lacks permission" errors. But the Git user doesn't know _which_
# user lacks permission. Tell them.
MSGID_EXPLAIN_P4USER   = [ p4gf_p4msgid.MsgDm_ExPROTECT
                         , p4gf_p4msgid.MsgDm_ExPROTECT2
                         ]
MSGID_EXPLAIN_P4CLIENT = [ p4gf_p4msgid.MsgDm_ExVIEW
                         , p4gf_p4msgid.MsgDm_ExVIEW2
                         ]

# timer/counter names
OVERALL         = NTR('Git to P4 Overall')
FAST_EXPORT     = NTR('FastExport')
PREFLIGHT       = NTR('Preflight')
COPY            = NTR('Copy')
P4_SUBMIT       = NTR('p4 submit')
CHECK_PROTECTS  = NTR('Check Protects')
CHECK_OVERLAP   = NTR('Check Overlap')
MIRROR          = NTR('Mirror Git Objects')

N_BLOBS = NTR('Number of Blobs')


class ProtectsChecker:
    """class to handle filtering a list of paths against view and protections"""
    def __init__(self, ctx, author, pusher):
        """init P4.Map objects for author, pusher, view and combination"""
        self.ctx = ctx
        self.author = author
        self.pusher = pusher

        config = p4gf_config.get_repo(ctx.p4gf, ctx.config.view_name)
        self.ignore_author_perms = config.get(p4gf_config.SECTION_REPO,
                                              p4gf_config.KEY_IGNORE_AUTHOR_PERMS,
                                              fallback='no') == 'yes'

        self.view_map = None
        self.read_protect_author = None
        self.read_protect_pusher = None
        self.read_filter = None
        self.write_protect_author = None
        self.write_protect_pusher = None
        self.write_filter = None

        self.init_view()
        self.init_read_filter()
        self.init_write_filter()

        self.author_denied = []
        self.pusher_denied = []
        self.unmapped = []

    def init_view(self):
        """init view map for client"""
        self.view_map = self.ctx.clientmap

    def init_read_filter(self):
        """init read filter"""
        self.read_protect_author = self.ctx.user_to_protect(self.author
                                        ).map_for_perm(p4gf_protect.READ)
        if not self.author == self.pusher:
            self.read_protect_pusher = self.ctx.user_to_protect(self.pusher
                                        ).map_for_perm(p4gf_protect.READ)
            self.read_filter = P4.Map.join(self.read_protect_author,
                                           self.read_protect_pusher)
        else:
            self.read_filter = self.read_protect_author
        self.read_filter = P4.Map.join(self.read_filter, self.view_map)

    def init_write_filter(self):
        """init write filter"""
        self.write_protect_author = self.ctx.user_to_protect(self.author
                                        ).map_for_perm(p4gf_protect.WRITE)
        if not self.author == self.pusher:
            self.write_protect_pusher = self.ctx.user_to_protect(self.pusher
                                        ).map_for_perm(p4gf_protect.WRITE)
            self.write_filter = P4.Map.join(self.write_protect_author,
                                            self.write_protect_pusher)
        else:
            self.write_filter = self.write_protect_author
        self.write_filter = P4.Map.join(self.write_filter, self.view_map)

    def filter_paths(self, blobs):
        """run list of paths through filter and set list of paths that don't pass"""
        # check against one map for read, one for write
        # if check fails, figure out if it was the view map or the protects
        # that caused the problem and report accordingly
        self.author_denied = []
        self.pusher_denied = []
        self.unmapped = []
        c2d = P4.Map.RIGHT2LEFT

        for blob in blobs:
            topath_c = self.ctx.gwt_path(blob['path']).to_client()
            topath_d = self.ctx.gwt_path(blob['path']).to_depot()

            # for all actions, need to check write access for dest path
            result = "  "   # zum loggen
            if topath_d and P4GF_DEPOT_OBJECTS_RE.match(topath_d):
                continue
            # do not require user write access to //.git-fusion/branches
            if topath_d and P4GF_DEPOT_BRANCHES_RE.match(topath_d):
                continue
            if not self.write_filter.includes(topath_c, c2d):
                if not self.view_map.includes(topath_c, c2d):
                    self.unmapped.append(topath_c)
                    result = NTR('unmapped')
                elif not (self.ignore_author_perms or
                          self.write_protect_author.includes(topath_d)):
                    self.author_denied.append(topath_c)
                    result = NTR('author denied')
                elif not self.write_protect_pusher.includes(topath_d):
                    self.pusher_denied.append(topath_c)
                    result = NTR('pusher denied')
                else:
                    result = "?"
                LOG.debug('filter_paths() {:<13} {}, {}'
                          .format(result, blob['path'], topath_d))

    def has_error(self):
        """return True if any paths not passed by filters"""
        return len(self.unmapped) or len(self.author_denied) or len(self.pusher_denied)

    def error_message(self):
        """return message indicating what's blocking the push"""
        if len(self.unmapped):
            return _('file(s) not in client view')
        if len(self.author_denied):
            restricted_user = self.author if self.author else _('<author>')
        elif len(self.pusher_denied):
            restricted_user = self.pusher if self.pusher else _('<pusher>')
        else:
            restricted_user = _('<unknown>')
        return _("user '{}' not authorized to submit file(s) in git commit").format(restricted_user)

# pylint:disable=R0902
# Too many instance attributes
# Probably true.
class G2P:
    """class to handle batching of p4 commands when copying git to p4"""
    def __init__(self, ctx, assigner, gsreview_coll):
        self.ctx = ctx

        self.usermap = p4gf_usermap.UserMap(ctx.p4gf)

            # list of strings [":<p4_change_num> <commit_sha1> <branch_id>"]
            #
            # Fake marks! NOT the marks from git-fast-export!
            #
            # Later code in GitMirror.add_commits() requires the changelist
            # number, NOT the git-fast-export mark number.
        self.marks = []

            # dict { "mark-num" : "commit sha1" }
            #
            # Real marks from git-fast-export.
        self.fast_export_mark_to_sha1 = None
            # dict { "commit sha1" : "mark-num" }
            #
            # Reverse of fast_export_mark_to_sha1
        self.sha1_to_fast_export_mark = None

        self._current_branch = None
        self.sha1_to_depot_branch_info = dict()
        self.depot_branch_info_index = ctx.depot_branch_info_index()
        self.__branch_id_to_head_changenum = {}
        self.assigner = assigner

            # submitted changelist number (as string) ==> sha1 of commit
        self.submitted_change_num_to_sha1 = {}
        self.submitted_revision_count     = 0

            # Is there some Git-specific data (usually a link to a parent Git
            # commit) that can only be detected by reading the Git commit from
            # our object store, not deduced from Perforce integ history? Reset
            # once per _copy_commit(), latches True if commit needs it.
        self._contains_p4_extra           = False

        config = p4gf_config.get_repo(ctx.p4gf, ctx.config.view_name)
        self.ignore_author_perms = config.getboolean(p4gf_config.SECTION_REPO,
                                                     p4gf_config.KEY_IGNORE_AUTHOR_PERMS,
                                                     fallback=False)

        self._matrix                      = None

            # List of Sha1ChangeNum tuples accumulated after each
            # successful 'p4 submit'. Used for debugging _dump_on_failure().
        self._submit_history              = []

            # Current git-fast-export 'commit'.
        self._curr_fe_commit              = None

            # Git Swarm reviews if any. None, or sometimes empty, if not.
        self.gsreview_coll                = gsreview_coll

            # Local file paths of ChangelistDataFile files written.
            # Will later pass to GitMirror to write.
        self.changelist_data_file_list     = []

            # list of known stream depots
            # used by preflight to check for impossible pushes
        self.stream_depots                 = None

    def _dump_on_failure(self, errmsg, is_exception):
        '''
        Something has gone horribly wrong and we've ended up in
        _revert_and_raise()

        Dump what we know about the push. Maybe it'll help.
        '''
        log = logging.getLogger('failures')
        if not log.isEnabledFor(logging.ERROR):
            return

        log.error(errmsg)
        with self.ctx.p4.at_exception_level(self.ctx.p4.RAISE_NONE):
            p4gf_log.create_failure_file('push-')

            version = p4gf_version.as_string()
            log.error('Git Fusion version:\n{}'.format(version))

            info = self.ctx.p4.run(['info'])
            log.error('p4 info:\n{}'.format(pprint.pformat(info)))

            if is_exception:
                stack_msg = ''.join(traceback.format_exc())
            else:
                stack_msg = ''.join(traceback.format_stack())
            log.error('stack:\n{}'.format(stack_msg))

            log.error('os.environ subset:')
            for k, v in os.environ.items():
                if (   k.startswith('P4')
                    or k in ['LANG', 'PATH', 'PWD', 'HOSTNAME']):
                    log.error('{:<20} : {}'.format(k, v))

            opened = self.ctx.p4.run(['opened'])
            log.error('p4 opened:\n{}'.format(pprint.pformat(opened)))

            have   = self.ctx.p4.run(['have'])
            log.error('p4 have:\n{}'.format(pprint.pformat(have)))

            client = self.ctx.p4.run(['client', '-o'])
            log.error('p4 client -o:\n{}'.format(pprint.pformat(client)))

            temp_branch = self.ctx.temp_branch(create_if_none=False)
            if temp_branch and temp_branch.written:
                client = self.ctx.p4.run(['branch', '-o', temp_branch.name])
                log.error('p4 branch -o {}:\n{}'
                          .format(temp_branch.name, pprint.pformat(client)))
            else:
                log.error('p4 branch not yet written.')

            log.error('git-fast-export commit:\n{}'
                      .format(pprint.pformat(self._curr_fe_commit)))

            log.error('pre-receive tuples:\n{}'
                      .format('\n'.join([str(prt)
                                   for prt in self.assigner.pre_receive_list])))

            cmd = [ 'git', 'log', '--graph', '--format=%H' ]
            sha1_list = [prt.new_sha1 for prt in self.assigner.pre_receive_list]
            p = p4gf_proc.popen_no_throw(cmd + sha1_list)
            log_lines = self.assigner.annotate_lines(p['out'].splitlines())
            log_lines = self._annotate_lines(log_lines)
            log.error(' '.join(cmd + sha1_list))
            log.error('\n' + '\n'.join(log_lines))

            if self._matrix:
                log.error('matrix:\n'
                          + '\n'.join(p4gf_g2p_matrix_dump.dump( self._matrix
                                                               , wide = True )))

                log.error('matrix integ/branch history:')
                for e in self._matrix.integ_batch_history:
                    log.error(pprint.pformat(e))

            for branch in self.ctx.branch_dict().values():
                log.error('branch view :' + repr(branch))
                log.error('depot branch:' + repr(branch.depot_branch))

            cmd = [ 'ls', '-RalF', self.ctx.view_dirs.p4root]
            log.error(' '.join(cmd))
            d = p4gf_proc.popen_no_throw(cmd)
            log.error(d['out'])

            log.error('Recent p4run history count: {}'
                      .format(len(self.ctx.p4run_history)))
            for i, cmd in enumerate(self.ctx.p4run_history):
                log.error('{i:<2}: {cmd}'.format(i=i, cmd=cmd))
            log.error('Recent p4gfrun history count: {}'
                      .format(len(self.ctx.p4gfrun_history)))
            for i, cmd in enumerate(self.ctx.p4gfrun_history):
                log.error('{i:<2}: {cmd}'.format(i=i, cmd=cmd))

        p4gf_log.close_failure_file()

    def _annotate_lines(self, lines):
        '''
        Any line that contains a sha1 gets "@nnn" appended if we hold that sha1
        in our _submit_history. Could have multiple @nnn appended if copied to
        multiple changelists.
        '''

        # Inflate our history list into a dict for faster lookup. Pre-assemble
        # annotation strings, including possible multi-changelist sha1s. One
        # less thing to hassle with in the loop below.
        sha1_to_annotation = defaultdict(str)
        for sc in self._submit_history:
            sha1_to_annotation[p4gf_util.abbrev(sc.sha1)] \
                                                += ' @{}'.format(sc.change_num)

        if self._curr_fe_commit and 'sha1' in self._curr_fe_commit:
            curr_sha1 = p4gf_util.abbrev(self._curr_fe_commit['sha1'])
            sha1_to_annotation[p4gf_util.abbrev(curr_sha1)] += ' <== FAILED HERE'

        re_sha1 = re.compile('([0-9a-f]{7})')

        for l in lines:
            m = re_sha1.search(l)
            if not m:
                yield l
                continue

            sha1 = m.group(1)
            if sha1 not in sha1_to_annotation:
                yield l
                continue

            yield l + sha1_to_annotation[sha1]

    def _parent_branch(self):
        '''
        Return a string suitable for use as a DescInfo "parent-branch:" value.

        If current commit lists a different branch for Git first-parent commit GPARN0,
        return "{depot-branch-id}@{change-num}".

        If not, return None. Don't include in DescInfo
        '''
                        # If no GPARN0, or if GPARN0 is on same branch, then no
                        # reason to clutter DescInfo with "parent is the
                        # previous commit on this branch."
                        #
        first_parent_col = self._matrix.first_parent_column()
        if not first_parent_col:
            return None
        if first_parent_col.branch == self._current_branch:
            return None

        if first_parent_col.depot_branch:
            par_dbid = first_parent_col.depot_branch.depot_branch_id
        else:
            par_dbid = None
        return (NTR("{depot_branch_id}@{change_num}")
                .format( depot_branch_id = par_dbid
                       , change_num      = first_parent_col.change_num
                       ))

    def _change_description(self, commit):
        """Construct a changelist description from a git commit.

        Keyword arguments:
            commit  -- commit data from Git
        """
        di = DescInfo()
        for key in ('author', 'committer'):
            datum = commit[key]
            di[key] = { 'fullname' : datum['user']
                      , 'email'    : datum['email']
                      , 'time'     : datum['date']
                      , 'timezone' : datum['timezone'] }
        di.clean_desc        = commit['data']
        di.author_p4         = commit['author_p4user']
        di.pusher            = commit['pusher_p4user']
        di.sha1              = commit['sha1']
        di.push_state        = NTR('complete') if 'last_commit' in commit else NTR('incomplete')
        di.contains_p4_extra = self._contains_p4_extra
        parents = self._parents_for_commit(commit)
        if len(parents) > 1:
            # For now, skip recording parentage for single-parent commits
            di.parents = parents
        if 'files' in commit:
            # Scan for possible submodule/gitlink entries
            di.gitlinks = [(f.get('sha1'), f.get('path')) for f in commit['files']
                    if f.get('mode') == '160000']
        if 'gitlinks' in commit:
            # Hackish solution to removal of submodules, which are indistinquishable
            # from the removal of any other element in Git.
            if not di.gitlinks:
                di.gitlinks = []
            di.gitlinks += commit['gitlinks']
        if self._current_branch.depot_branch:
            di.depot_branch_id = self._current_branch.depot_branch.depot_branch_id

        di.parent_branch = self._parent_branch()

        return di.to_text()

                        # pylint:disable=C0301
                        # Line too long
                        # Keep tabular code tabular.
    def _ghost_change_description(self):
        '''
        Return a string to use as the changelist description for
        a ghost changelist.
        '''
        header = _("Git Fusion branch management")
        kv     = { p4gf_const.P4GF_DESC_KEY_GHOST_OF_SHA1       : self._matrix.ghost_column.sha1
                 , p4gf_const.P4GF_DESC_KEY_GHOST_OF_CHANGE_NUM : self._matrix.ghost_column.change_num
                 , p4gf_const.P4GF_DESC_KEY_GHOST_PRECEDES_SHA1 : self._curr_fe_commit['sha1']
                 , p4gf_const.P4GF_DESC_KEY_PUSH_STATE          : NTR('incomplete')
                 }
        parent_branch = self._parent_branch()
        if parent_branch:
            kv[p4gf_const.P4GF_DESC_KEY_PARENT_BRANCH] = parent_branch

        lines = [header, "", p4gf_const.P4GF_IMPORT_HEADER]
        for k in sorted(kv.keys()):
            v = kv[k]
            lines.append(" {}: {}".format(k, v))

        return '\n'.join(lines)
                        # pylint:enable=C0301

    def _revert_and_raise(self, errmsg, is_exception):
        """An error occurred while attempting to submit the incoming change
        to Perforce. As a result, revert all modifications, log the error,
        and raise an exception."""
        self._dump_on_failure( errmsg       = errmsg
                             , is_exception = is_exception )

        # If this counter is set  - skip the usual cleanup.
        # This disabling is intended for use only by dev.
        value = p4gf_util.first_value_for_key(
                  self.ctx.p4run(['counter', '-u', p4gf_const.P4GF_COUNTER_DISABLE_ERROR_CLEANUP]),
                  'value')
        if not (value == 'True' or value == 'true'):
            try:
                # Undo any pending Perforce operations.
                opened = self.ctx.p4run(['opened'])
                if opened:
                    self.ctx.p4run(['revert', '-k', self.ctx.client_view_path()])
                self.ctx.p4run(['sync', '-kq', '{}#none'.format(self.ctx.client_view_path())])

                # Undo any dirty files laying around in our P4 work area.
                if not self.ctx.view_dirs.p4root:
                    LOG.error('Bug: who called p4gf_copy_to_p4'
                              ' without ever setting ctx.view_dirs?')
                else:
                    shutil.rmtree(self.ctx.view_dirs.p4root)
                    p4gf_util.ensure_dir(self.ctx.view_dirs.p4root)
            except RuntimeError as e:
                # Failed to clean up, log that as well, but do not
                # lose the original error message in spite of this.
                LOG.error(str(e))
        else:
            LOG.debug("_revert_and_raise skipping Git Fusion cleanup as {0}={1}".format(
                p4gf_const.P4GF_COUNTER_DISABLE_ERROR_CLEANUP, value))

        if not errmsg:
            errmsg = traceback.format_stack()
        msg = _('import failed: {}').format(errmsg)
        LOG.error(msg)
        raise RuntimeError(msg)

    def _p4_message_to_text(self, msg):
        '''
        Convert a list of P4 messages to a single string.

        Annotate some errors with additional context such as P4USER.
        '''
        txt = str(msg)
        if msg.msgid in MSGID_EXPLAIN_P4USER:
            txt += ' P4USER={}.'.format(self.ctx.p4.user)
        if msg.msgid in MSGID_EXPLAIN_P4CLIENT:
            txt += ' P4CLIENT={}.'.format(self.ctx.p4.client)
        return txt

    def _check_p4_messages(self):
        """If the results indicate a file is locked by another user,
        raise an exception so that the overall commit will fail. The
        changes made so far will be reverted.
        """
        msgs = p4gf_p4msg.find_all_msgid(self.ctx.p4, MSGID_CANNOT_OPEN)
        if not msgs:
            return

        lines = [self._p4_message_to_text(m) for m in msgs]
        self._revert_and_raise('\n'.join(lines), is_exception = False)

    def _bulldoze(self):
        '''
        Bulldoze over any locked files (or those opened exclusively) by
        overriding the locks using our admin privileges. Notify the Git
        user if such an action is performed (with file and user information
        included). Returns True if any locks were overridden. The caller
        will need to perform the command again to effect any lasting change.
        '''
        exclusive_files = []
        locked_files = []
        # other_users: depotFile => user (used in reporting)
        other_users = dict()
        # other_clients: client => [depotFile...] (used in unlocking)
        other_clients = dict()

        def capture_details(m):
            '''Capture details on the locked file.'''
            depot_file = m.dict['depotFile']
            if depot_file not in other_users:
                other_users[depot_file] = []
            other_users[depot_file].append(m.dict['user'])
            client = m.dict['client']
            if client not in other_clients:
                other_clients[client] = []
            other_clients[client].append(depot_file)

        # Scan messages for signs of locked files, capturing the details.
        for m in self.ctx.p4.messages:
            if m.msgid == p4gf_p4msgid.MsgDm_OpenXOpened:
                # Will be paired with "also opened by" message
                exclusive_files.append(m.dict['depotFile'])
            elif m.msgid == p4gf_p4msgid.MsgDm_OpenIsLocked:
                locked_files.append(m.dict['depotFile'])
                capture_details(m)
            elif m.msgid == p4gf_p4msgid.MsgDm_AlsoOpenedBy:
                capture_details(m)

        # Unlock any exclusively opened or locked files, and report results.
        if exclusive_files or locked_files:
            if locked_files:
                self.ctx.p4run(['unlock', '-f', locked_files])
            if exclusive_files:
                for other_client, depot_files in other_clients.items():
                    client = self.ctx.p4.fetch_client(other_client)
                    host = client.get('Host')
                    user = p4gf_const.P4GF_USER
                    with p4gf_util.UserClientHost(self.ctx.p4, user, other_client, host):
                        # Override the locked option on the other client, if needed.
                        with p4gf_util.ClientUnlocker(self.ctx.p4, client):
                            # don't use self.ctx.p4run() for these, because we're
                            # monkeying with the other user's changelist, not ours
                            # and self.ctx.p4run() will helpfully insert -c changenum
                            # which will cause these commands to fail
                            p4gf_util.p4run_logged(self.ctx.p4, ['reopen', depot_files])
                            p4gf_util.p4run_logged(self.ctx.p4, ['revert', '-k', depot_files])

            for depot_file in exclusive_files + locked_files:
                users = ", ".join(other_users[depot_file])
                sys.stderr.write(_("warning: overrode lock on '{}' by '{}'\n")
                                 .format(depot_file, users))
            sys.stderr.write(_('warning: it is advisable to contact them in this regard\n'))
            return True
        return False

    def _revert(self):
        '''
        If an attempt to add/edit/delete a file failed because that file is
        already open for X, then revert it so that we can try again.

        Return list of depot_file paths that we reverted, empty
        if nothing reverted.
        '''
        msg_list = p4gf_p4msg.find_msgid(self.ctx.p4, p4gf_p4msgid.MsgDm_OpenBadAction)
        depot_file_list = [m.dict['depotFile'] for m in msg_list]
        if depot_file_list:
            LOG.debug2('_revert(): cannot open file(s) already open for delete'
                       ' from some other branch. Reverting.')
            self.ctx.p4run(['revert'] + depot_file_list)
            # We just reverted a delete, probably a delete integrated from some
            # other branch. Might have just reverted our only link from that
            # branch. Can't trust integ for parent calcs in later p4-to-git.
            self._contains_p4_extra = True
        return depot_file_list

    def _p4run(self, cmd, bulldoze=False, revert=False):
        '''
        Run one P4 command, logging cmd and results.
        '''
        results = self.ctx.p4run(cmd)
        if bulldoze and self._bulldoze():
            # Having overridden any locks, try again and fall through
            # to the message validator code below.
            results = self.ctx.p4run(cmd)
        if revert and self._revert():
            results = self.ctx.p4run(cmd)
        self._check_p4_messages()
        return results

    def _handle_unicode(self, results):
        '''
        Scan the results of a P4 command, looking for files whose type was
        detected as being Unicode. This means they (may) have a byte order
        mark, and this needs to be preserved, which is accomplished by
        storing the file using type 'ctext'.
        '''
        for result in results:
            if not isinstance(result, dict):
                continue

            base_mods = p4gf_p4filetype.to_base_mods(result['type'])
            #if base_mods[0] in UNICODE_TYPES:
            #    # Switch UTF16 files to ctext to avoid byte order changing.
            #    base_mods[0] = 'text'
            #    if not 'C' in base_mods:
            #        base_mods.append('C')
            #    filetype = p4gf_p4filetype.from_base_mods( base_mods[0]
            #                                             , base_mods[1:])
            #    self._p4run(['reopen', '-t', filetype, result['depotFile']])
            # XXX: force ctext to unicode - sulee 20140609
            if 'text' in base_mods and 'C' in base_mods:
                base_mods = ['unicode', '']
                filetype = p4gf_p4filetype.from_base_mods( base_mods[0]
                                                         , base_mods[1:])
                self._p4run(['reopen', '-t', filetype, result['depotFile']])

    def _opened_dict(self):
        '''
        What do we have open right now?
        Return as a dict depot_path ==> 'p4 opened' response dict.
        '''
        opened = self.ctx.p4run(['opened', self.ctx.client_view_path()])
        opened_dict = { o['depotFile'] : o
                        for o in opened
                        if isinstance(o, dict) and 'depotFile' in o}
        return opened_dict

    def _check_protects(self, p4user, blobs):
        """check if author is authorized to submit files"""
        pc = ProtectsChecker(self.ctx, p4user, self.ctx.authenticated_p4user)
        pc.filter_paths(blobs)
        if pc.has_error():
            self._revert_and_raise(pc.error_message(), is_exception = False)

    @staticmethod
    def _log_fe_file(fe_file):
        '''
        Return loggable string for a single fe_commit['files'] element.
        '''

        mode = '      '
        if 'mode' in fe_file:
            mode = fe_file['mode']
        sha1 = '       '
        if 'sha1' in fe_file:
            sha1 = p4gf_util.abbrev(fe_file['sha1'])

        return NTR('{mode} {action} {sha1} {path}') \
               .format( mode   = mode
                      , action = fe_file['action']
                      , sha1   = sha1
                      , path   = fe_file['path'])

    def _get_acp(self, fecommit):
        '''
        Return a dict with values set to the Perforce user id for
        - author
        - committer
        - pusher

        Values set to None if no corresponding Perforce user id.

        Separate from and superset of _get_author_pusher_owner(). Called only
        for Git Swarm reviews because only Git Swarm reviews care about
        Git committer.
        '''
        return { 'author'    : self._git_to_p4_user(fecommit, 'author')
               , 'committer' : self._git_to_p4_user(fecommit, 'committer')
               , 'pusher'    : self.ctx.authenticated_p4user
               }

    def _git_to_p4_user(self, fecommit, fecommit_key):
        '''
        Return the Perforce user that corresponds to a given Git commit
        key 'author' or 'committer'.
        '''
        email = fecommit[fecommit_key]['email'].strip('<>')
        user = self.usermap.lookup_by_email(email)
        LOG.debug2("_git_to_p4_user() for email {} found user {}".format(email, user))
        if (user is None) or (not self.usermap.p4user_exists(user[0])):
            return None
        return user[0]

    def _get_author_pusher_owner(self, commit):
        '''
        Add to commit: p4 user id for: Git author, Git pusher and p4 change owner

        Retrieve the Perforce user performing the push, and the original
        author of the Git commit, if a known Perforce user, or unknown_git
        if that user is available.

        If the ignore-author-permissions config setting is false, or the
        change-owner is set to 'author', then the commit author must be a
        valid Perforce user.
        '''
        pusher = self.ctx.authenticated_p4user
        if self.ctx.owner_is_author:
            author = self._git_to_p4_user(commit, 'author')
        else:
            author = pusher
        if self.ctx.owner_is_author:
            change_owner = author
        else:
            change_owner = pusher
        commit['author_p4user'] = author
        commit['pusher_p4user'] = pusher
        commit['owner'] = change_owner


    def _preflight_check_commit(self, commit):
        """
        Prior to copying a commit, perform a set of checks to ensure the commit
        will (likely) go through successfully.
        This includes:
            * verifying permission to commit for author p4 user
            * screening for merge commits
            * screening for submodules
            * checking valid filenames
            * checking write permissions for each file
        """
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug('_preflight_check_commit() Checking mark={} sha1={} file-ct={} -- {}'
                      .format(  commit['mark']
                              , p4gf_util.abbrev(commit['sha1'])
                              , len(commit['files'])
                              , repr(commit['data'])[:20].splitlines()[0]))

        if not commit['author_p4user']:
            self._revert_and_raise( _("User '{}' not permitted to commit")
                                    .format(commit['author']['email'].strip('<>'))
                                  , is_exception = False )

        if 'merge' in commit:
            if not self.ctx.merge_commits:
                raise RuntimeError(_('Merge commits are not enabled for this repo.'))
            if not self.ctx.branch_creation  and  self.assigner.have_anonymous_branches:
                raise RuntimeError(_('Git branch creation is prohibited for this repo.'))
            if LOG.isEnabledFor(logging.DEBUG):
                for parent_mark in commit['merge']:
                    parent_sha1 = self.fast_export_mark_to_sha1[parent_mark][:7]
                    LOG.debug("_preflight_check_commit() merge mark={} sha1={}"
                              .format(parent_mark, parent_sha1))

        if not self.ctx.submodules and 'files' in commit:
            for f in commit['files']:
                if f.get('mode') == '160000':
                    raise RuntimeError(
                        _('Git submodules not permitted: path={} commit={}')
                        .format(f.get('path'), p4gf_util.abbrev(commit['sha1'])))

        for f in commit['files']:
            LOG.debug3("_preflight_check_commit : commit files: " + self._log_fe_file(f))
            err = check_valid_filename(f['path'], self.ctx)
            if err:
                self._revert_and_raise(err, is_exception=False)

    def _preflight_check_commit_for_branch(self, commit, branch_id, any_locked_files):
        """
        Prior to copying a commit, perform a set of checks for a specific branch
        to ensure the commit will (likely) go through successfully.
        """
        rev = commit['sha1']
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("_preflight_check_commit_for_branch() "
                      "Checking branch={} mark={} sha1={} file-ct={} -- {}"
                      .format(  branch_id
                              , commit['mark']
                              , p4gf_util.abbrev(rev)
                              , len(commit['files'])
                              , repr(commit['data'])[:20].splitlines()[0]))

        if self._already_copied_commit(rev, branch_id):
            return

        # following checks assume client has bee set for branch
        self._ensure_branch_preflight(commit, branch_id)

        # Empty commits require root-level .p4gf_placeholder to be mapped
        # in the current branch view.
        if not commit['files'] and not self._is_placeholder_mapped():
            raise RuntimeError(
                _("Empty commit {sha1} not permitted. Git Fusion branch views"
                  " must include root to permit empty commits.")
                .format(sha1=p4gf_util.abbrev(rev)))

        with Timer(CHECK_PROTECTS):
            self._check_protects(commit['author_p4user'], commit['files'])

        with Timer(CHECK_OVERLAP):
            self._check_overlap(commit)

        self._check_stream_writable(commit)
        self._check_stream_in_classic(commit)

        LOG.debug('checking locked files under //{}/...'.format(self.ctx.p4.client))
        if any_locked_files:
            # Scan for either locked files or files opened for exclusive edit.
            files_in_commit = ['//{}/{}'.format(self.ctx.p4.client
                                                , p4gf_util.escape_path(f['path']))
                               for f in commit['files']]
            if files_in_commit:
                fstat_flags = NTR('otherLock | otherOpen0 & headType=*+l')
                locked_files = self.ctx.p4run(['fstat', '-F', fstat_flags, '-m1', files_in_commit]
                                              , log_warnings=logging.DEBUG)
                if locked_files:
                    lf1 = locked_files[0]
                    user = lf1['otherOpen'][0] if 'otherOpen' in lf1 else NTR('<unknown>')
                    # Collect the names (and clients) of users with locked files.
                    # Report back to the pusher so they can take appropriate action.
                    msg = NTR('{} - locked by {}').format(lf1['depotFile'], user)
                    LOG.info(msg)
                    self._revert_and_raise(msg, is_exception = False)

                # +++ Spend time extracting Jobs and P4Changelist owner
                #     here if we actually do need to call
                #     the preflight-commit hook.
        if self.ctx.preflight_hook.is_callable():
            jobs = extract_jobs(commit['data'])
            self.ctx.preflight_hook(
                 ctx                 = self.ctx
               , fe_commit           = commit
               , branch_id           = branch_id
               , jobs                = jobs
               )

    def _check_overlap(self, fe_commit):
        '''
        If any of the files in this commit intersect any fully populated branch
        (other than the current branch), then reject this commit.

        Shared/common/overlapping paths in branch views must be read-only from
        Git. Otherwise you end up with a Git push of commit on one Git branch
        inserting  changes into other Git branches behind Git's back.

        To modify shared paths, either do so from Perforce, or create a Git
        Fusion repo with no more than one branch that maps that shared path.
        '''
        for fe_file in fe_commit['files']:
            gwt_path   = fe_file['path']
            depot_path = self.ctx.gwt_path(gwt_path).to_depot()

            for branch in p4gf_branch.iter_fp(self.ctx.branch_dict()):
                if branch == self._current_branch:
                    continue
                if not branch.intersects_depot_path(depot_path):
                    continue

                human_msg = (
                    _("Cannot commit {sha1} '{gwt_path}' to '{depot_path}'."
                      " Paths that overlap multiple Git Fusion branches are read-only."
                      " Branches: '{b1}', '{b2}'")
                    .format( sha1       = p4gf_util.abbrev(fe_commit['sha1'])
                           , gwt_path   = gwt_path
                           , depot_path = depot_path
                           , b1 = self._current_branch.branch_id
                           , b2 = branch.branch_id ))
                self._revert_and_raise(human_msg, is_exception = False)

    def _check_stream_writable(self, fe_commit):
        '''
        If this is a stream branch, check that all files in the commit are
        writable.  If any of the files is not writable then reject this commit.
        '''
        if not self._current_branch.stream_name:
            return
        prefix = self._current_branch.writable_stream_name + '/'
        for fe_file in fe_commit['files']:
            gwt_path   = fe_file['path']
            depot_path = self.ctx.gwt_path(gwt_path).to_depot()
            if depot_path.startswith(prefix):
                continue

            human_msg = (
                _("Cannot commit {sha1} '{gwt_path}' to '{depot_path}'."
                  " Paths not in stream '{stream}' are read-only for branch '{b}'.")
                .format( sha1       = p4gf_util.abbrev(fe_commit['sha1'])
                       , gwt_path   = gwt_path
                       , depot_path = depot_path
                       , stream     = self._current_branch.writable_stream_name
                       , b          = self._current_branch.branch_id ))
            self._revert_and_raise(human_msg, is_exception = False)

    def _check_stream_in_classic(self, fe_commit):
        '''
        If this is a classic branch, check that none of the files in the commit
        are in stream depots and thus not writable.  If any of the files is not
        writable then reject this commit.
        '''
        if self._current_branch.stream_name:
            return

        depot_re = re.compile(r'^//([^/]+)/([^/]+)/.*$')
        for fe_file in fe_commit['files']:
            gwt_path   = fe_file['path']
            depot_path = self.ctx.gwt_path(gwt_path).to_depot()
            m          = depot_re.match(depot_path)
            if m:
                depot = m.group(1)
                if depot in self.stream_depots:
                    stream = '//{}/{}'.format(m.group(1), m.group(2))
                    human_msg = (
                        _("Cannot commit {sha1} '{gwt_path}' to '{depot_path}'."
                          " Paths in stream '{stream}' are read-only for branch '{b}'.")
                        .format( sha1       = p4gf_util.abbrev(fe_commit['sha1'])
                               , gwt_path   = gwt_path
                               , depot_path = depot_path
                               , stream     = stream
                               , b          = self._current_branch.branch_id ))
                    self._revert_and_raise(human_msg, is_exception = False)

    def _create_matrix(self, fe_commit):
        '''
        Factory for G2PMatrix discover/decide/do.

        Allows us to switch to Matrix 2, depending on feature flag.
        '''
        if self.ctx.is_feature_enabled(p4gf_config.FEATURE_MATRIX2):
            return G2PMatrix2(
                              ctx            = self.ctx
                            , current_branch = self._current_branch
                            , fe_commit      = fe_commit
                            , g2p            = self
                            )
        else:
            return G2PMatrix(
                              ctx            = self.ctx
                            , current_branch = self._current_branch
                            , fe_commit      = fe_commit
                            , g2p            = self
                            )

    def _copy_commit_matrix_gsreview(self, commit, gsreview):
        '''
        Copy a single Git commit to Perforce, shelve as a pending
        Perforce changelist, as a new or amended Swarm review.
        '''

                        # Find destination branch. All we have is the
                        # Git branch name, as a portion of the pushed
                        # Git reference.
        dest_branch = self.ctx.git_branch_name_to_branch(gsreview.git_branch_name)

        LOG.debug('_copy_commit_matrix_gsreview() commit={sha1}'
                  ' gsreview={gsreview} dest_branch={dest_branch}'
                  .format( sha1        = commit['sha1']
                         , gsreview    = gsreview
                         , dest_branch = dest_branch ))

        nc = p4gf_util.NumberedChangelist( ctx = self.ctx
                               , description = commit['data']
                               , change_num  = gsreview.review_id )
        result = self._copy_commit_matrix(
                                 commit              = commit
                               , branch_id           = dest_branch.branch_id
                               , gsreview            = gsreview
                               , finish_func         = self._p4_shelve_for_review
                               , numbered_changelist = nc )

                        # When writing p4gf_config2, don't write 'review/xxx' as
                        # this branch's ref. Do this NOW, before we write
                        # p4gf_config2, to avoid unnecessary updates to that
                        # file just to change a git-branch-name.
        if gsreview.needs_rename:
            review_branch = self.ctx.git_branch_name_to_branch(gsreview.old_ref_name())
            if review_branch:
                review_branch.git_branch_name = gsreview.new_ref_name()
                LOG.debug('_copy_commit_matrix_gsreview() new {}'.format(review_branch))

        return result

                        # pylint:disable=R0913
                        # Too many arguments (6/5)
                        # Yeah, we probably should create a per-commit x branch
                        # data object to carry around all this state. Not today.

    def _copy_commit_matrix( self
                           , commit
                           , branch_id
                           , gsreview
                           , finish_func
                           , numbered_changelist ):
        """Copy a single Git commit to Perforce, returning the Perforce
        changelist number of the newly submitted change. If the commit
        resulted in an empty change, nothing is submitted and None is
        returned.
        """
        if LOG.isEnabledFor(logging.INFO):
            sha1 = commit['sha1'][:7]
            desc = repr(commit['data'][:20]).splitlines()[0]
            # Odd spacing here to line up commit sha1 with "Submitted"
            # info message at bottom of this function.
            LOG.info('Copying   commit {}        {} {}'
                     .format(sha1, p4gf_util.abbrev(branch_id), desc))
        if LOG.isEnabledFor(logging.DEBUG) and 'merge' in commit:
            for parent_mark in commit['merge']:
                parent_sha1 = self.fast_export_mark_to_sha1[parent_mark][:7]
                LOG.debug("_copy_commit() merge mark={} sha1={}"
                          .format(parent_mark, parent_sha1))

        self._ensure_branch(commit, branch_id)

        with numbered_changelist:

            # Debugging a push with a known bad changelist number?
            #
            # if self.ctx.numbered_change.change_num == 50:
            #     logging.getLogger('p4')              .setLevel(logging.DEBUG3)
            #     logging.getLogger('p4gf_g2p_matrix2').setLevel(logging.DEBUG3)
            #     LOG                                  .setLevel(logging.DEBUG3)
            #     LOG.debug3('#################################################')

            try:
                self._matrix = self._create_matrix(commit)
                self._matrix.discover()

                self._matrix.ghost_decide()
                if self._matrix.ghost_do_it():
                    self._ghost_submit(numbered_changelist)

                        # Rare double-ghost-changelist: add_delete
                        # was added and submitted above. Now delete and submit.
                    if self._matrix.convert_for_second_ghost_changelist():
                        if self._matrix.ghost_do_it():
                            self._ghost_submit(numbered_changelist)

                        # Create a brand new matrix, re-discover everything,
                        # building on the ghost changelist.
                        # +++ Ideally we could reuse much of the original
                        # +++ matrix. 'p4 integ' previews are expensive.
                    self._matrix = self._create_matrix(commit)
                    self._matrix.discover()

                self._matrix.decide()
                self._matrix.do_it()

            except P4.P4Exception as e:
                self._revert_and_raise(str(e), is_exception = True)

            except Exception as e:
                self._revert_and_raise(str(e), is_exception = True)

            with Timer(P4_SUBMIT):
                LOG.debug("Pusher is: {}, author is: {}"
                          .format(commit['pusher_p4user'], commit['author_p4user']))
                desc = self._change_description(commit)

                try:
                    changenum = finish_func( desc      = desc
                                           , owner     = commit['owner']
                                           , sha1      = commit['sha1']
                                           , branch_id = branch_id
                                           , gsreview  = gsreview
                                           , fecommit  = commit )
                except P4.P4Exception as e:
                    self._revert_and_raise(str(e), is_exception = True)

        if changenum and self._current_branch:
            self.__branch_id_to_head_changenum[self._current_branch.branch_id] = changenum
        return changenum
                        # pylint:enable=R0913

    @staticmethod
    def _pretty_print_submit_results(submit_result):
        '''
        500-column-wide list-of-dict dumps are not so helpful.
        '''
        r = []
        for sr in submit_result:
            if not isinstance(sr, dict):
                r.append(repr(sr))
                continue

            if (     ('depotFile' in sr)
                 and ('action'    in sr)
                 and ('rev'       in sr) ):
                r.append(NTR('{action:<10} {depotFile}#{rev}')
                         .format( action    = sr['action']
                                , depotFile = sr['depotFile']
                                , rev       = sr['rev']))
                continue

            r.append(' '.join(['{}={}'.format(k, v) for k, v in sr.items()]))
        return r

                        # pylint:disable=R0913
                        # Too many arguments (6/5)
                        # Yep. And I'm okay with that.

                        # pylint:disable=W0613
                        # Unused argument 'gsreview'
                        # Name cannnot be _ prefixed because
                        # calling code passes by keyword.
    def _p4_submit(self, desc, owner, sha1, branch_id, gsreview, fecommit):
        """This is the function called once for each git commit as it is
        submitted to Perforce. If you need to customize the submit or change
        the description, here is where you can do so safely without
        affecting the rest of Git Fusion.

        Since p4 submit does not allow submitting on behalf of another user
        we must first submit as git-fusion-user and then edit the resulting
        changelist to set the 'User' field to the actual author of the change.

        Implements CALL#3507045/job055710 "Allow for a user-controlled
        submit step."

        author_date can be either integer "seconds since the epoch" or a
        Perforce-formatted timestamp string YYYY/MM/DD hh:mm:ss. Probably needs to
        be in the server's timezone.

        branch_id used only for logging.
        """
        # Avoid fetch_change() and run_submit() since that exposes us to the
        # issue of filenames with double-quotes in them (see job015259).
        #
        # Retry the try/catch submit once and only once if and only if
        # a translation error occurs
        # p4 reopen -t binary on the problem files for the second attempt
        #
        # Translation error warning format:
        #'Translation of file content failed near line 10 file /path/some/file'
        # Set the job number prior to calling submit
        self._add_jobs_to_curr_changelist(sha1=sha1, desc=desc)

        retry = True
        while retry:
            retry = False
            try:
                r = self.ctx.numbered_change.submit()
            except P4.P4Exception:
                if p4gf_p4msg.find_msgid(self.ctx.p4, p4gf_p4msgid.MsgServer_NoSubmit):
                    LOG.error('Ignored commit {} empty'
                              .format(p4gf_util.abbrev(sha1)))
                    # Empty changelist is now worthy of a raised exception,
                    # no longer just a silent skip.

                # A p4 client submit may be rejected by our view lock during this copy_to_p4.
                # If so the submit_trigger will unlock its opened files before returning.
                # However, for the small interval between determing to reject the submit
                # and unlocking the files, we may get a lock failure here with our submit.
                # So retry, expecting the trigger to unlock the files.
                # Yes. Forever. With no logging.
                # Note bene: Ctl-C by the git user will not interrupt this retry loop.
                if p4gf_p4msg.find_msgid(self.ctx.p4, p4gf_p4msgid.MsgDm_LockAlreadyOther):
                    retry = True
                if not retry:
                    raise

        # add count of revs submitted in this change to running total
        for rr in r:
            if isinstance(rr, dict) and 'rev' in rr:
                self.submitted_revision_count += 1

        changenum = self.ctx.numbered_change.change_num
        LOG.info('Submitted commit {sha1} @{changenum}  {branch_id}'
                 .format( sha1=p4gf_util.abbrev(sha1)
                        , changenum=changenum
                        , branch_id=p4gf_util.abbrev(branch_id)))
        self._submit_history.append(Sha1ChangeNum( sha1       = sha1
                                                 , change_num = changenum ))

        self.submitted_change_num_to_sha1[str(changenum)] = sha1
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('\n'.join(self._pretty_print_submit_results((r))))

        self._set_changelist_owner(change_num=changenum, owner=owner, desc=desc)
        return changenum

    def _p4_shelve_for_review( self, desc, owner, sha1, branch_id
                             , gsreview, fecommit):
        '''
        Shelve the current numbered pending changelist for Swarm review.

        Revert all open files.

        Do not submit or delete this changelist.

        Create a review data file for this changelist for later
        GitMirror submit.
        '''
                        # Succeed or fail, never attempt to shelve this
                        # Git Swarm review again during this push.
        gsreview.handled = True
        change_num = self.ctx.numbered_change.change_num

                        # New review? Assign now.
        is_new_review = (not gsreview.review_id)
        if is_new_review:
            gsreview.review_id    = change_num
            gsreview.needs_rename = True

                        # Tell Git client about the assigned ID. This is our
                        # only chance to get this ID back to the Git user.
            sys.stderr.write(_('\nPerforce: Swarm review assigned:'
                               ' review/{gbn}/{review_id}\n')
                             .format( gbn        = gsreview.git_branch_name
                                    , review_id  = gsreview.review_id ))

        review_suffix_d = { 'review-repo'   : self.ctx.config.view_name
                          , 'review-id'     : gsreview.review_id
                          , 'review-status' : NTR('new') if is_new_review else NTR('amend')
                          , 'review-git-branch-name'
                                            : self._current_branch.git_branch_name
                          }

        acp = self._get_acp(fecommit)
        LOG.error('acp={}'.format(acp))
        if acp.get('author'):
            review_suffix_d['review-author'   ] = acp.get('author')
        if acp.get('committer'):
            review_suffix_d['review-committer'] = acp.get('committer')
        if acp.get('pusher'):
            review_suffix_d['review-pusher'   ] = acp.get('pusher')

        review_suffix = '\n' + '\n'.join([NTR(' {k}: {v}').format(k=k, v=v)
                                          for k, v in review_suffix_d.items()])

        self._add_jobs_to_curr_changelist(sha1=sha1, desc=desc)
        self.ctx.numbered_change.shelve(replace = not is_new_review)
        self.ctx.p4run(['revert', '-k', '//...'])

                        # set_changelist_owner() also updates changelist
                        # description, it's our last chance to tell Swarm
                        # that we've got a review.
        desc_r = desc + review_suffix
        self._set_changelist_owner(change_num, owner, desc_r)

                        # Create a data file describing this review
                        # and schedule for later GitMirror add.
        self._create_review_data_file(gsreview)

                        # pylint:enable=W0613
                        # pylint:enable=R0913

    def _create_review_data_file(self, _gsreview):
        '''
        Create a new file that explains things about
        the current numbered pending changelist such as a list
        of ancestor Git commits/Perforce changelists,
        which files were deleted in which Git commit.

        GitMirror will eventually add and submit this file along
        with the rest of GitMirror commits and branch-info files:
          //.git-fusion/repos/{repo}/changelists/{change_num}
        '''
                        # No need to check for whether or not we have anything
                        # to write. All Git Swarm reviews will have at least ONE
                        # second-parent commit that is the pushed review head,
                        # and not part of the destination.

                        # List of ancestor commits.
        ancestor_commit_otl = list(self._ancestor_commit_otl_iter())

                        # List of file deletions.
        deletion_list = list(self._to_deletion_iter(ancestor_commit_otl))

        datafile = ChangelistDataFile(
                              ctx        = self.ctx
                            , change_num = self.ctx.numbered_change.change_num )
        datafile.ancestor_commit_otl = ancestor_commit_otl
        datafile.deletion_list       = deletion_list
        datafile.write()

        self.changelist_data_file_list.append(datafile.local_path())

    def _ghost_submit(self, numbered_changelist):
        '''
        Submit one or more file actions to create a Ghost changelist: branch
        files that our commit wants to delete, update contents to match what our
        commit wants to edit, and so on.

        Once submitted, swap in a NEW numbered pending changelist to
        house actions for the impending real non-ghost changelist.
        '''
                        # Save original description before we
                        # replace it with ghost description.
        orig_changelist = self.ctx.p4.fetch_change(numbered_changelist.change_num)

        desc  = self._ghost_change_description()
        self._p4_submit( desc      = desc
                       , owner     = p4gf_const.P4GF_USER
                       , sha1      = self._matrix.ghost_column.sha1
                       , branch_id = self._current_branch.branch_id
                       , gsreview  = None
                       , fecommit  = None
                       )

                        # Create a new numbered pending changelist,
                        # restoring original description.
        numbered_changelist.second_open(orig_changelist['Description'])


    def _ancestor_commit_otl_iter(self):
        '''
        Iterator/generator to produce an ObjectType list of Git commits that
        contribute to the history being merged into the destination branch by
        the current commit.

        Returns Git commits that are ancestors ("are reachable by")
        the current Git fe_commit, but are not ancestors ("are not reachable
        by") the current Git commit's first-parent.

        MUST be a merge commit with exactly 2 parents (which is exactly what
        Swarm reviews create).
        '''
        sha1 = self._curr_fe_commit['sha1']
        cmd = ['git', 'rev-list'
                        # Include all history contributing to what's merging in.
              , '{}^2'.format(sha1)
                        # Exclude all history already merged in.
              , '--not', '{}^1'.format(sha1)
              ]
        p = p4gf_proc.popen_no_throw(cmd)
        for par_sha1 in p['out'].splitlines():
            par_otl = self.commit_sha1_to_otl(par_sha1)
            for ot in par_otl:
                yield ot

    def _to_deletion_iter(self, ancestor_commit_otl):
        '''
        Iterator/generator to produce list of Perforce depot paths that
        were "deleted" by any of the given commits/changelists.

        "deleted" here includes files that _would_ have been deleted if
        JIT-branch-for-delete was a legal Perforce operation.

        "deleted" here _may_ include deletions that were indeed recorded as 'p4
        delete' actions. It costs more to filter those out and I'm not sure we
        care. If we do care, I'll add code to filter 'em out rather than let
        them slip into the branch-info file.
        '''
        for ancestor_commit_ot in ancestor_commit_otl:
            branch_id = ancestor_commit_ot.details.branch_id
            branch    = self.ctx.branch_dict().get(branch_id)

                        # No depot branch info ==> not lightweight
                        # ==> no JIT ==> no untracked delete actions.
            dbi = branch.depot_branch
            if not dbi:
                continue

                        # Read this from DepotBranchInfo's data file the list
                        # of depot_path#revision_numbers deleted by this
                        # changelist number.
            del_depot_path_list = dbi.change_num_to_deleted_depot_path_list(
                    ancestor_commit_ot.details.changelist)
            if not del_depot_path_list:
                continue

                        # Map all those ancestor commit depot paths to the
                        # review destination branch.
                        #
                        #
                        # +++ COULD sort ancestor_commit_otl by branch_id which
                        # +++ would permit re-use of this joined map. Wait until
                        # +++ profiling proves it's worthwhile.
            a2r = P4.Map.join( branch.view_p4map
                             , self._current_branch.view_p4map.reverse() )

            for del_depot_path in del_depot_path_list:
                review_depot_path = a2r.translate(del_depot_path)

                yield DeletionElement( ot = ancestor_commit_ot
                                     , depot_path        = del_depot_path
                                     , review_depot_path = review_depot_path )


    def _add_jobs_to_curr_changelist(self, sha1, desc):
        '''
        Run 'p4 change -f' to attach any Jobs mentioned in the
        commit description to the current numbered pending changelist.
        '''
        jobs = extract_jobs(desc)
        if not jobs:
            return

        changenum = self.ctx.numbered_change.change_num
        change = self.ctx.p4.fetch_change(changenum)
        LOG.debug("Fixing jobs: {}".format(' '.join(jobs)))
        change['Jobs'] = jobs
        try:
            self.ctx.p4.save_change(change, '-f')
        except P4.P4Exception as e:
            # on error - p4 still saves the client without the invalid Job:
            # and since all we are updating is the job - nothing else to do
            LOG.debug("failed trying to jobs to change {}".format(' '.join(jobs)))
            err = e.errors[0] if isinstance(e.errors, list) and len(e.errors) > 0 else str(e)
            _print_error(_('Commit {} jobs ignored: {}').format(sha1, err))

    def _set_changelist_owner(self, change_num, owner, desc):
        '''
        Run 'p4 change -f' to reassign changelist ownership to
        a Perforce user associated with the Git author or pusher,
        not git-fusion-user.
        '''
        LOG.debug("Changing change owner to: {}".format(owner))
        change = self.ctx.p4.fetch_change(change_num)
        change['User'] = owner
        change['Description'] = desc
        self.ctx.p4.save_change(change, '-f')
        self._fix_ktext_digests(change['Change'])

    def _fix_ktext_digests(self, change):
        """Update digests for any ktext or kxtext revs in the change.

        This is necessary after setting the author of the change.

        """
        # Use -e to avoid scanning too many rows.
        cmd = ['fstat', '-F', 'headType=ktext|headType=kxtext', '-e', change, '//...']
        r = self.ctx.p4run(cmd)
        ktfiles = ["{}#{}".format(f['depotFile'], f['headRev']) for f in r if 'headRev' in f]
        if ktfiles:
            self.ctx.p4run(['verify', '-v', ktfiles])

    def _change_num_to_sha1(self, change_num, branch_id):
        '''
        If change_num is a changelist previously submitted to Perforce on the
        given branch_id, return the sha1 of the commit that corresponds to that
        change.
        '''
        # First check to see if change_num was a changelist that we submitted as
        # part of this git push, have not yet submitted its ObjectType mirror to
        # //P4GF_DEPOT/objects/...
        sha1 = self.submitted_change_num_to_sha1.get(str(change_num))
        if sha1:
            return sha1

        # Not one of ours. Have to go to the ObjectType store.
        commit = ObjectType.commit_for_change(self.ctx,
                                              change_num,
                                              branch_id)
        if commit:
            return commit.sha1
        return None

    def _parents_for_commit(self, commit):
        '''
        For the given Git commit, find the SHA1 values for its parents.
        '''
        if 'from' in commit:
            # Use the fast-export information to get the parents
            pl = [self.fast_export_mark_to_sha1[commit['from']]]
            if 'merge' in commit:
                for parent in commit['merge']:
                    pl.append(self.fast_export_mark_to_sha1[parent])
        else:
            # Make the call to git to get the information we don't have
            LOG.debug3('_parents_for_commit() sha1={}'
                       .format(p4gf_util.abbrev(commit['sha1'])))
            pl = p4gf_util.git_sha1_to_parents(commit['sha1'])

        return pl

    # Output type for _find_new_depot_branch_parent()
    NewDBIParent = namedtuple('NewDBIParent', [ 'parent_otl'
                                              , 'depot_branch_id_list'
                                              , 'change_num_list'])

    def _find_new_depot_branch_parent(self, commit):
        '''
        Find the parent commits for a new child commit, map those to
        depot branches and changelists on those depot branches, return 'em.
        '''
        # Build up two parallel lists of parent depot branch IDs and the
        # Perforce changelist numbers that define the point in time
        # from which we branch off into this new depot branch.
        parent_depot_branch_id_list = []
        parent_changelist_list      = []
        parent_otl                  = []

        commit_sha1 = commit['sha1']
        if LOG.isEnabledFor(logging.DEBUG):
            sha1 = commit_sha1[:7]
            desc = commit['data'][:10].replace('\n', '..')
            LOG.debug("_find_new_depot_branch_parent() commit {} '{}'".format(sha1, desc))
        parent_list = self._parents_for_commit(commit)
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("_find_new_depot_branch_parent() child sha1={} par sha1={}"
                      .format(commit_sha1[:7], ' '.join([s[:7] for s in parent_list])))

        for parent_sha1 in parent_list:
            #parent_commits = ObjectType.commits_for_sha1(self.ctx, parent_sha1)
            parent_commits = self.commit_sha1_to_otl(parent_sha1)
            if parent_commits:
                parent_otl.extend(parent_commits)
                LOG.debug("_find_new_depot_branch_parent() for par sha1={}..."
                          .format(parent_sha1[:7]))
                for parent_commit in parent_commits:
                    LOG.debug("_find_new_depot_branch_parent() ...par ot={}".format(parent_commit))
                    par_cl_num = parent_commit.details.changelist
                    p4cl = P4Changelist.create_using_change( self.ctx.p4gf
                                                           , par_cl_num)
                    desc_info = DescInfo.from_text(p4cl.description)
                    par_dbid = desc_info.depot_branch_id if desc_info else None
                    LOG.debug("_find_new_depot_branch_parent() ...dbid={}".format(par_dbid))
                    parent_depot_branch_id_list.append(par_dbid)
                    parent_changelist_list     .append(par_cl_num)
            elif parent_sha1 in self.sha1_to_depot_branch_info:
                par_dbi  = self.sha1_to_depot_branch_info[parent_sha1]
                par_dbid = par_dbi.depot_branch_id
                parent_depot_branch_id_list.append(par_dbid)
                # Use the colon prefix to set marks apart from changelist numbers
                mark_num = ':' + self.sha1_to_fast_export_mark[parent_sha1]
                parent_changelist_list.append(mark_num)

                # Must fill in output ObjectType list, even if with temp/fake
                # instances, so that calling code can find parent branch views
                # from which to derive this depot branch's branch view.
                for parent_branch_view \
                        in p4gf_branch.depot_branch_to_branch_view_list(
                                               self.ctx.branch_dict(), par_dbi):
                    parent_otl.append(ObjectType.create_commit(
                                  parent_sha1
                                , self.ctx.config.view_name
                                , mark_num
                                , parent_branch_view.branch_id ))
            else:
                # No parent objects at all? Probably pushing new branches.
                # That's okay, too.
                pass

        return self.NewDBIParent( parent_otl           = parent_otl
                           , depot_branch_id_list = parent_depot_branch_id_list
                           , change_num_list      = parent_changelist_list )

    def _define_new_depot_branch(self, commit, out_parent_otl):
        '''
        Return a new DepotBranchInfo object. Claims a not-yet-populated subtree
        of depot space, and knows which other depot branches are immediate
        parents.

        Records in our index of all known DepotBranchInfo.

        out_parent_otl : An output list that receives each parent commit
                         ObjectType instance. Pass in a list pointer. Here
                         solely as an optimization to avoid reloading the
                         same ObjectType list again in a few seconds when
                         we need a parent's branch_id/view to calculate a
                         new child branch's view.

                         Returned unchanged if no parents yet for this branch,
                         such as when pushing new history into empty Perforce.

                         ### Return this in a namedtuple, not an output
                         ### parameter.
        '''
        ndpar = self._find_new_depot_branch_parent(commit)

        out_parent_otl.extend(ndpar.parent_otl)

        # Define depot branch with above tuples as parents.
        r = p4gf_depot_branch.new_definition(self.ctx)
        r.parent_depot_branch_id_list = ndpar.depot_branch_id_list
        r.parent_changelist_list      = ndpar.change_num_list

        commit_sha1 = commit['sha1']
        LOG.debug('_define_new_depot_branch() new {} (sha1 {})'
                  .format(r, p4gf_util.abbrev(commit_sha1)))
        self.depot_branch_info_index.add(r)
        self.sha1_to_depot_branch_info[commit_sha1] = r

        return r

    @staticmethod
    def _replace_depot_root_one(old_lhs, root_list, new_root):
        '''
        Find and replace one depot root from old_lhs and replace it with
        new_root.
        '''
        result = None
        old_lhs = p4gf_path.dequote(old_lhs)
        for old_root in root_list:
            if old_lhs.startswith(old_root):
                result = new_root + old_lhs[len(old_root):]
                break
            if old_lhs.startswith("-" + old_root):
                result = "-" + new_root + old_lhs[len(old_root)+1:]
                break

        if result:
            return p4gf_path.enquote(result)
        raise RuntimeError(_('_replace_depot_root_one() found no matching root'
                             ' in old_lhs={}').format(old_lhs))

    def _replace_depot_root(self, orig_p4map, new_depot_root):
        '''
        Build a new branch view mapping, same as our parent's branch
        view mapping, but with the parent's // root replaced with
        new_depot_root.

        Returns a P4.Map() object with the new view mapping.
        '''
        # Find a lightweight depot branch root within the RHS of the parent's view.
        # Append trailing slash so that "//rob" does not match "//robert".
        new_root = new_depot_root + '/'
        root_list = [dbi.root_depot_path + '/'
                 for dbi in self.depot_branch_info_index.by_id.values()]
        # If none of the above known lightweight branch depot roots match,
        # then we have a fully populated parent. Use depot root "//".
        root_list.append('//')
        r  = P4.Map()
        client_prefix = '//{}/'.format(self.ctx.config.p4client)
        for lhs, rhs in zip(orig_p4map.lhs(), orig_p4map.rhs()):
            depot_root = self._replace_depot_root_one(lhs, root_list, new_root)
            new_rhs = p4gf_path.dequote(rhs)[len(client_prefix):]
            r.insert(depot_root, p4gf_path.enquote(new_rhs))
        return r

    def commit_sha1_to_otl(self, sha1):
        '''
        Return a list of zero or more ObjectType instances that correspond to
        the given commit sha1, both from Perforce and from pending GitMirror
        data in our (fake) marks list.

        Yes, you need to check both: commits can be copied multiple times, in
        multiple pushes, to different branch views.

        Return empty list if no results.
        '''
        otl = ObjectType.commits_for_sha1(self.ctx, sha1)
        # +++ O(n) scan of split strings
        #
        # Probably should index by sha1 if performance testing shows this to
        # matter, especially for really large pushes with hundreds of
        # thousands of commits.
        copied = False
        for line in self.marks:
            mark = Mark.from_line(line)
            if mark.sha1 != sha1:
                continue
            change_num = mark.mark
            ot = ObjectType.create_commit( sha1
                                         , self.ctx.config.view_name
                                         , change_num
                                         , mark.branch )
            if not ot in otl:
                if not copied:
                    # Do not modify the list that ObjectType.commits_for_sha1()
                    # gave us. Copy-on-write.
                    otl = copy.copy(otl)
                    copied = True
                otl.append(ot)

        return otl

    def _find_parent_for_new_branch(self, parent_otl):
        '''
        Return one parent Branch instance that would be a suitable base
        from which to derive a new branch's view.

        parent_otl -- ObjectType list that _define_new_depot_branch() outputs.

        Return None if no parents (orphan Git branch?).
        '''
        for parent_ot in parent_otl:
            if parent_ot.details.branch_id:
                parent_branch = self.ctx.branch_dict().get(parent_ot.details.branch_id)
                if parent_branch and parent_branch.view_p4map:
                    return parent_branch
                else:
                    LOG.debug('_find_parent_for_new_branch() skipping:'
                              'view branch_id={} returned branch={}'
                              .format( parent_ot.details.branch_id[:7]
                                     , parent_branch))
            else:
                LOG.debug('_find_parent_for_new_branch() skipping:'
                          ' ObjectType missing details.branch_id: {}'
                          .format(parent_ot))
        return None

    def _define_branch_view(self, branch, depot_branch, parent_otl):
        '''
        Define a branch view that maps lightweight branch storage into the repo.

        Take a parent's branch view and replace its root with our own.

        If no parent, use the master-ish branch's view.
        '''
        parent_branch = self._find_parent_for_new_branch(parent_otl)
        LOG.debug('_define_branch_view() branch={} parent={}'
                  .format( p4gf_branch.abbrev(branch)
                         , p4gf_branch.abbrev(parent_branch)))
        ObjectType.log_otl(parent_otl, log=LOG)

        if not parent_branch:
            # No parent? Probably pushing an orphan. Assume it may one day
            # merge into fully-populated Perforce, so base the new view on
            # any old branch view that maps fully-populated Perforce.
            parent_branch = p4gf_branch.most_equal(self.ctx.branch_dict())

            # Not even ONE branch in our p4gf_config[2]? Bug in config validator
            # or init_repo, should have rejected this push before you got here.
            if not parent_branch:
                raise RuntimeError(_('No Git Fusion branches defined for repo.'))

        branch.view_p4map = self._replace_depot_root(
                                              parent_branch.view_p4map
                                            , depot_branch.root_depot_path)
        branch.view_lines = branch.view_p4map.as_array()

        LOG.debug('_define_branch_view() returning: {}'
                  .format(branch.to_log(LOG)))

    def _ensure_branch_preflight(self, commit, branch_id):
        '''
        If not already switched to and synced to the correct branch for the
        given commit, do so.

        If this is a new lightweight branch, perform whatever creation we can do
        at preflight time. We don't have commits/marks for any not-yet-submitted
        parent commits, so the depot_branch_info will often lack a correct
        parent or fully populated basis.

        * depot tree, along with a branch-info file
        * branch mapping, along with entry in p4gf_config2 (if not anonymous)

        Return True if switching from current branch
        '''
        if      self._current_branch \
            and self._current_branch.branch_id == branch_id:
            LOG.debug("_ensure_branch() sha={} want branch_id={} curr branch_id={} NOP"
                      .format( commit['sha1'][:7]
                             , branch_id[:7]
                             , self._current_branch.branch_id[:7]))
            LOG.debug("_ensure_branch() staying on  branch {}".
                    format(self.ctx.branch_dict().get(branch_id)))

            return False

        cbid = self._current_branch.branch_id if self._current_branch else 'None'
        LOG.debug("_ensure_branch() sha={} want branch_id={} curr branch_id={} switch"
                  .format(commit['sha1'][:7], branch_id[:7], cbid[:7]))

        branch = self.ctx.branch_dict().get(branch_id)
        # branch should never be None here. p4gf_branch_id.Assigner() must
        # create Branch objects for each assignment.

        if not branch.view_lines:
            parent_otl = [] # _define_new_depot_branch() loads this from Perforce,
                            # _define_branch_view() needs it. Omit needless reads.
            LOG.debug("_ensure_branch() no mapping (yet) for branch id={}".format(branch_id[:7]))
            depot_branch = self._define_new_depot_branch(commit, parent_otl)
            branch.is_new         = True
            branch.depot_branch   = depot_branch
            branch.is_dbi_partial = not depot_branch.parent_depot_branch_id_list

            self._define_branch_view(branch, depot_branch, parent_otl)
            # Prepend client name to view RHS so that
            # Context.switch_client_view_to_branch() can use the view.
            branch.set_rhs_client(self.ctx.config.p4client)

        elif branch.view_p4map:
            # if this is a stream branch, check for mutation of the stream's
            # view by comparing with the original view saved in p4gf_config2
            if branch.original_view_lines:
                original_view_lines = '\n'.join(branch.original_view_lines)
                view_lines = p4gf_config.convert_view_to_no_client_name(branch.view_lines)
                if not view_lines == original_view_lines:
                    raise RuntimeError(_('Unable to push.  Stream view changed from:\n{}\nto:\n{}')
                                       .format(original_view_lines, view_lines))
            # Find existing depot branch for branch view's LHS.
            lhs = branch.view_p4map.lhs()
            branch.depot_branch = self.ctx.depot_branch_info_index()    \
                                                    .find_depot_path(lhs[0])

        LOG.debug("_ensure_branch() switching to branch {}".format(branch))

        # By now we should have a branch and a branch.view_lines.
        # First remove current branch's files from workspace
        # Client spec is set to normdir
        self.ctx.switch_client_view_to_branch(branch)
        self._current_branch = branch
        changenum = self.__branch_id_to_head_changenum.get(branch_id)
        if changenum:
            branch.is_new = False
        else:
            branch.is_new = True
        return True

    def _is_placeholder_mapped(self):
        '''
        Does this branch map our placeholder file?

        Returns non-False if mapped, None or empty string if not.
        '''
        return self.ctx.gwt_path(
                       p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER).to_depot()

    def _ensure_branch(self, commit, branch_id):
        '''
        If not already switched to and synced to the correct branch for the
        given commit, do so.

        If this is a new lightweight branch, NOW it is save to create
        a depot_branch_info for this branch, since any parent commits
        now exist as marks.

        Return True if switching from current branch
        '''
        # Preflight version does most of what we need.
        switched = self._ensure_branch_preflight(commit, branch_id)

        # If current branch has a partial DBI, now's the first time we can
        # fill it in.
        branch = self.ctx.branch_dict().get(branch_id)
        dbi    = branch.depot_branch
        if branch.is_dbi_partial and dbi:
            ndpar = self._find_new_depot_branch_parent(commit)
            dbi.parent_depot_branch_id_list = ndpar.depot_branch_id_list
            dbi.parent_changelist_list      = ndpar.change_num_list
            branch.is_dbi_partial           = False

        return switched

    def branch_to_p4head_sha1(self, branch):
        '''
        Return the Git commit sha1 of the most recent changelist on branch_id.

        Requires that there are actual submitted changelists in branch_id's view,
        and that the most recent changelist maps to a Git commit already within our Git repo.

        Return None if branch is empty.
        Return 0 if branch holds changelists, but no corresponding Git commit.
        '''
                        # Usually we're the source of the most recent changelist.
        change_num = self.__branch_id_to_head_changenum.get(branch.branch_id)

                        # If not, ask Perforce.
        if not change_num:
            with self.ctx.switched_to_branch(branch):
                r = self.ctx.p4run(
                        [ 'changes'
                        , '-m1'
                        , '-s', 'submitted'
                        , '//{client}/...'.format(client=self.ctx.p4.client)])
                rr = p4gf_util.first_dict_with_key(r, 'change')
                if not rr:
                    return None     # Branch is empty, no changelists.
                change_num = rr.get('change')

                        # Convert to associated Git commit's sha1.
        sha1 = self._change_num_to_sha1(change_num, branch.branch_id)
        if not sha1:
            return 0    # Changelist found, but lacks Git commit sha1.

        return sha1

    def _already_copied_commit(self, commit_sha1, branch_id):
        '''
        Do we already have a Perforce changelist for this commit, this branch_id?
        '''
        ### Works for already-submitted, not yet for stuff we created as an
        ### earlier part of the current 'git push' but not yet submitted to
        ### the //P4GF_DEPOT/objects/... hierarchy.

        ### Zig suspects that if we copy a commit for ANY lightweight branch,
        ### that should match for ALL lightweight branches. If so, check
        ### branch_id and details.branch_id against ctx.branch_dict() and
        ### see if both lightweight. Needs proof.
        return bool(ObjectType.change_for_sha1(self.ctx,
                                               commit_sha1,
                                               branch_id))

    def _update_depot_branch_info(self, change_num, commit_sha1):
        """
        In support of lightweight branching, find any depot branch info
        structures that are holding fast-export marks instead of Perforce
        changelist numbers, and update those entries based on the newly
        submitted change.
        """
        log = LOG.getChild('_update_depot_branch_info')
        ### Need to scan all of the involved branches because the assigner
        ### may have assigned the commit to the "wrong" branch? Or maybe
        ### Nathan just isn't getting what's going on. :(
        for branch in self.ctx.branch_dict().values():
            if not branch.depot_branch:
                # If not a newly created branch, nothing for us to do.
                continue
            depot_branch = branch.depot_branch
            log.debug("considering branch {}".format(
                depot_branch.depot_branch_id))
            mark = self.sha1_to_fast_export_mark[commit_sha1]
            if mark:
                log.debug("finding mark :{}".format(mark))
                colon_mark = ':' + mark
                cll = [change_num if cl == colon_mark else cl
                    for cl in depot_branch.parent_changelist_list]
                if log.isEnabledFor(logging.DEBUG):
                    for old, new in zip(depot_branch.parent_changelist_list, cll):
                        if old != new:
                            log.debug("replaced {} with {}".format(old, new))
                depot_branch.parent_changelist_list = cll

    def _copy_commits(self, commits):
        """Copy the given commits from Git to Perforce.

        Arguments:
            commits -- commits from FastExport class

        Returns:
            self.marks will be populated for use in object cache.
        """
        last_copied_change_num = 0
        for commit in commits:
            ProgressReporter.increment(_('Copying changelists...'))
            self.ctx.heartbeat()
            commit_sha1 = commit['sha1']
            self._curr_fe_commit = commit

            ### Zig hasn't fully thought this through, but skipping
            ### fast-export-ed commits that the branch assigner chose
            ### not to assign to branches seems to bypass a problem
            ### when multiple overlapping branches attempt to re-push
            ### an old p4->git->p4 changelist.
            ###
            ### See also repeat of this in _preflight_check()
            if commit_sha1 not in self.assigner.assign_dict:
                LOG.debug('_copy_commits() {} no branch_id. Skipping.'
                          .format(p4gf_util.abbrev(commit_sha1)))
                continue

            for branch_id in self.assigner.assign_dict[commit_sha1] \
                                                        .branch_id_list():
                with Timer(COPY):
                    if self._already_copied_commit(commit_sha1, branch_id):
                        LOG.debug('_copy_commits() {} {} '
                                  ' Commit already copied to Perforce. Skipping.'
                                  .format( p4gf_util.abbrev(commit_sha1)
                                         , p4gf_util.abbrev(branch_id)))
                        continue

                    nc = p4gf_util.NumberedChangelist( ctx = self.ctx
                                                     , description = commit['data'] )
                    change_num = self._copy_commit_matrix( commit
                                                         , branch_id
                                                         , gsreview            = None
                                                         , finish_func         = self._p4_submit
                                                         , numbered_changelist = nc )

                    if change_num is None:
                        LOG.warn("copied nothing for {} on {}"
                                 .format( commit_sha1
                                        , self._current_branch.branch_id ))
                        continue
                    self._update_depot_branch_info(change_num, commit_sha1)
                    last_copied_change_num = change_num

                self.marks.append(':{} {} {}'
                                  .format(change_num, commit_sha1,
                    self._current_branch.branch_id))
                label = NTR('at g={} p={}').format(commit_sha1[:7], change_num)
                p4gf_gc.process_garbage(label)
                p4gf_gc.report_growth(label)
                # end of for branch_id in assign_dict(sha1)

                    # Once this commit is fully copied and submitted to all
                    # Git Fusion branches, also copy and shelve as pending
                    # changelist for any Git Swarm reviews.
            self._copy_commit_gsreviews(commit)

        if last_copied_change_num:
            self.ctx.write_last_copied_change(last_copied_change_num)

    def _copy_commit_gsreviews(self, fe_commit):
        '''
        If current commit is the head commit of one or more Git Swarm reviews,
        copy those to Swarm.
        '''
        if not self.gsreview_coll:
            return
        gsreview_list = self.gsreview_coll.unhandled_review_list(
                                                             fe_commit['sha1'])
        LOG.debug3('_copy_commit_gsreviews() commit={} reviews={}'
                   .format( p4gf_util.abbrev(fe_commit['sha1'])
                          , gsreview_list ))
        if not gsreview_list:
            return
        with Timer(COPY):
            for gsreview in gsreview_list:
                self._copy_commit_matrix_gsreview(fe_commit, gsreview)

    def _preflight_check(self, commits):
        """
        Ensure the entire sequence of commits will (likely) go through
        without any errors related to permissions or locks. Raises an
        exception if anything goes wrong.

        Arguments:
            commits -- commits from FastExport class
        """
        LOG.info('Checking Perforce permissions and locks')
        with Timer(PREFLIGHT):

            # Stop if files are opened in our repo client
            # We expect this to be none, since we have the view lock
            opened = self.ctx.p4.run(['opened', '-m1'])
            if opened:
                raise RuntimeError(_('There are files opened by Git Fusion for this repo.'))

            # get a list of stream depots for later checks for read-only paths
            depots = self.ctx.p4.run(['depots'])
            self.stream_depots = set([d['name'] for d in depots if d['type'] == 'stream'])

            fstat_flags = NTR('otherLock | otherOpen0 & headType=*+l')
            #any_locked_files = True
            with self.ctx.switched_to_union():
                any_locked_files = self.ctx.p4run(['fstat', '-F', fstat_flags, '-m1'
                                                   , '//{}/...'.format(self.ctx.p4.client)]
                                                  , log_warnings=logging.DEBUG)
            with ProgressReporter.Determinate(len(commits)):
                for commit in commits:
                    ProgressReporter.increment(_('Checking commits...'))

                    self._get_author_pusher_owner(commit)

                    rev = commit['sha1']
                    if commit['sha1'] not in self.assigner.assign_dict:
                        continue

                    self._preflight_check_commit(commit)

                    for branch_id in self.assigner.assign_dict[rev].branch_id_list():
                        self.ctx.heartbeat()
                        self._preflight_check_commit_for_branch(commit, branch_id
                                                                , any_locked_files)


    def _push_start_counter_name(self):
        '''
        Return the counter where we record the last known changelist number
        before we start a 'git push'.
        '''
        return p4gf_const.P4GF_COUNTER_PUSH_STARTED \
                                  .format(repo_name=self.ctx.config.view_name)

    def _record_push_start_counter(self):
        '''
        Set a counter with the last known good changelist number before
        this 'git push' started. Gives the Git Fusion administrator a
        place to start if rolling back.
        '''
        last_change_num = 0
        r = self.ctx.p4run(['changes', '-m1'])
        if r:
            last_change_num = r[0]['change']

        counter_name = self._push_start_counter_name()
        counter_value = NTR('{change} {p4user} {time}') \
                        .format( change = last_change_num
                               , p4user = self.ctx.authenticated_p4user
                               , time   = p4gf_util.gmtime_str_iso_8601())
        self.ctx.p4run(['counter', '-u', counter_name, counter_value])

    def _clear_push_start_counter(self):
        '''
        Remove any counter created by _record_push_start_counter().
        '''
        counter_name = self._push_start_counter_name()
        self.ctx.p4run(['counter', '-u', '-d', counter_name])

    def copy(self, prt):
        """Copy a set of commits from Git into Perforce.

        Arguments:
            ctx -- P4GF context
            prt -- pre-receive tuple

        Returns error message, or None if okay.
        """
        err = None
        with Timer(OVERALL):
            self.ctx.view_repo = pygit2.Repository(self.ctx.view_dirs.GIT_DIR)

            with p4gf_util.HeadRestorer():
                try:
                    self._record_push_start_counter()

                    LOG.debug("copy() begin copying from {} to {} on {}".format(
                        prt.old_sha1, prt.new_sha1, prt.ref))
                    branch = self.ctx.git_branch_name_to_branch(prt.ref)

                    with Timer(FAST_EXPORT):
                        LOG.info(NTR('Running git-fast-export...'))
                        ProgressReporter.increment(_('Running git fast-export...'))
                        fe = p4gf_fastexport.FastExport(self.ctx, prt.old_sha1, prt.new_sha1,
                                self.ctx.tempdir.name)
                        fe.force_export_last_new_commit \
                                = (   (branch and (branch.view_lines == None))
                                   or p4gf_util.sha1_exists(prt.new_sha1))
                        if fe.force_export_last_new_commit:
                            LOG.debug2('copy() force_export_last_new_commit=True')
                        fe.run()
                        self.fast_export_mark_to_sha1 = fe.marks
                        self.sha1_to_fast_export_mark = {v:k for k, v in fe.marks.items()}
                        LOG.debug2('copy() FastExport produced mark_ct={}'.format(len(fe.marks)))

                    self._preflight_check(fe.commits)
                    self.marks = []
                    try:
                        p4gf_call_git.prohibit_interrupt(self.ctx.config.view_name, os.getpid())
                        with ProgressReporter.Determinate(len(fe.commits)):
                            self._copy_commits(fe.commits)
                        p4gf_gc.report_objects(NTR('after copying commits'))
                    finally:
                        # we want to write mirror objects for any commits that made it through
                        # any exception will still be alive after this
                        if self.marks:
                            with Timer(MIRROR):
                                LOG.info('Copying Git and Git Fusion data to //{}/...'.format(
                                    p4gf_const.P4GF_DEPOT))
                                self.ctx.mirror.add_depot_branch_infos(self.ctx)
                                self.ctx.mirror.add_branch_config2(self.ctx)
                                self.ctx.mirror.add_changelist_data_file_list(
                                                       self.changelist_data_file_list)
                                self.ctx.mirror.add_objects_to_p4(self.marks, None, None, self.ctx)

                                p4gf_gc.process_garbage(NTR('after mirroring'))
                        else:
                            LOG.warn("no marks to commit for {}".format(prt))

                finally:
                    temp_branch = self.ctx.temp_branch(create_if_none=False)
                    if temp_branch:
                        temp_branch.delete(self.ctx.p4)

        if not err:
            self._clear_push_start_counter()
            LOG.getChild("time").debug("\n" + str(self))
            LOG.info('Done. Changelists: {}  File Revisions: {}  Seconds: {}'
                     .format( len(self.submitted_change_num_to_sha1)
                            , self.submitted_revision_count
                            , int(Timer(OVERALL).time)))
        return err


def copy_git_changes_to_p4(ctx, prt, assigner, gsreview_coll):
    """Copy a set of commits from Git into Perforce.

    Arguments:
        ctx -- P4GF context
        prt -- pre-receive tuple
        assigner -- commit-to-branch assignments

    Returns error message, or None if okay.
    """
    g2p = G2P( ctx
             , assigner      = assigner
             , gsreview_coll = gsreview_coll )
    try:
        return g2p.copy(prt)
    except p4gf_lock.LockCanceled as lc:
        LOG.warning(str(lc))
        return _('Lock lost due to cancellation.')

# G2P._submit_history value
Sha1ChangeNum = namedtuple('Sha1ChangeNum', ['sha1', 'change_num'])

# G2P._to_deletion_iter() result.
DeletionElement = namedtuple('DeletionElement', ['ot', 'depot_path', 'review_depot_path'])
