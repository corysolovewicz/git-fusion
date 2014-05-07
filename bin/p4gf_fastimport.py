#! /usr/bin/env python3.3
"""FastImport class"""

import datetime
import logging
import os
from   subprocess import CalledProcessError
import tempfile

import pytz

import p4gf_const
from   p4gf_desc_info import DescInfo
from   p4gf_l10n      import _, NTR
import p4gf_proc
from   p4gf_profiler  import Timer
import p4gf_usermap
import p4gf_util

LOG = logging.getLogger(__name__)
LOG_SCRIPT = LOG.getChild("script")

OVERALL      = NTR('FastImport Overall')
BUILD        = NTR('Build')
RUN          = NTR('Run')
SCRIPT_LINES = NTR('Script length')
SCRIPT_BYTES = NTR('Script size')


def _log_crash_report():
    """
    Read the .git/fast_import_crash_NNN file and dump it to the log.
    """
    # For each crash report we find, dump its contents.
    # In theory we clean up after a crash so there should be only one.
    cwd = os.getcwd()
    for entry in os.listdir('.git'):
        if entry.startswith('fast_import_crash_'):
            with open(os.path.join(cwd, '.git', entry)) as f:
                report = f.read()
                # Keep the message free of repetition.
                LOG.error("git {}:\n{}".format(entry, report))


class FastImport:
    """Create a git-fast-import script and use it to import from Perforce.

    Steps to use FastImport:

    1) For each Perforce changelist to import:
            Call add_commit() with changelist and files.
            This adds everything necessary for one commit to the fast-import
            script.  The mark for the commit will be the change number.
    2) Call run_fast_import() to run git-fast-import with the script produced
       by steps 1-4.
    3) 'git checkout' or 'git branch -f' to put HEAD and branch refs where
       you want them.
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.script = tempfile.NamedTemporaryFile(dir=self.ctx.tempdir.name, prefix='fastimport-')
        self.timezone = ctx.timezone
        self.__tzname = None
        self.project_root_path_length = len(ctx.contentlocalroot)
        self._line_count = 0
        self._byte_count = 0
        self.username_map = dict()
        self.usermap = p4gf_usermap.UserMap(ctx.p4gf)

    def __get_timezone_offset(self, timestamp):
        """
        Determine the time zone offset for the given timestamp, using the.
        time zone name set in the P4GF time zone counter. For example, with
        time zone 'US/Pacific' and timestamp 1371663968, the offset returned
        will be the string '-0700'.
        """
        try:
            ts = int(timestamp)
        except ValueError:
            LOG.error("__get_timezone_offset() given non-numeric input {}".format(timestamp))
        if self.__tzname is None:
            r = self.ctx.p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_TIME_ZONE_NAME)
            value = p4gf_util.first_value_for_key(r, 'value')
            if value == '0' or value is None:
                # Upgrade from an EA system, perhaps, in which the upgrade counters
                # have been set but the later changes where not applied during init.
                msg = _("Counter '{}' not set, using UTC as default."
                        " Change this to your Perforce server's time zone.") \
                      .format(p4gf_const.P4GF_COUNTER_TIME_ZONE_NAME)
                LOG.warn(msg)
                value = NTR('UTC')
            self.__tzname = value
        try:
            mytz = pytz.timezone(self.__tzname)
        except pytz.exceptions.UnknownTimeZoneError:
            LOG.warn("Time zone name '{}' unrecognized, using UTC as default".format(self.__tzname))
            mytz = pytz.utc
        LOG.debug("__get_timezone_offset({}) with {}".format(ts, mytz))
        dt = datetime.datetime.fromtimestamp(ts, tz=pytz.utc)
        ct = dt.astimezone(mytz)
        return ct.strftime('%z')

    def __append(self, data):
        """append data to script"""
        if type(data) == str:
            data = data.encode()
        self.script.write(data)
        self._byte_count += len(data)
        self._line_count += data.count(b'\n')
        if LOG_SCRIPT.isEnabledFor(logging.DEBUG):
            l = str(data.decode().strip()).splitlines()
            for ll in l:
                LOG_SCRIPT.debug(ll)

    def __add_data(self, string):
        """append a string to fast-import script, git style"""
        encoded = string.encode()
        header = NTR('data {}\n').format(len(encoded))
        self.__append(header.encode() + encoded)

    def __relative_path(self, p4file):
        """return local path of p4file, relative to view root"""
        return self.ctx.depot_path(p4file.depot_path).to_gwt()

    def __add_files(self, snapshot):
        """
        Write files in snapshot to fast-import script.

        snapshot = list of P4File, usually from P4Changelist.files.
        """

                # Why partition()?
                # Don't delete a parent after adding its child:
                # M 100644 deba01f cookbooks/apt/README
                # D cookbooks/apt   <== BUG, would also delete/omit README

        partitioned = p4gf_util.partition(lambda x:x.is_delete(), snapshot)
        for p4file in partitioned:
            path = self.__relative_path(p4file)
            if not path:
                continue
            if path == p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER:
                # Perforce-only artifact. Never copy this into Git.
                continue
            if p4file.is_delete():
                self.__append("D {0}\n".format(path))
            else:
                if p4file.sha1 == "":
                    LOG.debug("skipping missing revision {}#{}".format(path, p4file.revision))
                    continue
                if p4file.is_x_type():
                    mode = "100755"
                elif p4file.is_symlink():
                    mode = "120000"
                else:
                    mode = "100644"
                self.__append("M {0} {1} {2}\n".
                              format(mode, p4file.sha1, path))

    def __add_gitlinks(self, gitlinks):
        """
        Write the submodule/gitlink entries from the given list.
        """
        for sha1, path in gitlinks:
            if sha1 == p4gf_const.NULL_COMMIT_SHA1:
                self.__append("D {}\n".format(path))
            else:
                self.__append("M 160000 {0} {1}\n".format(sha1, path))

    def __branch_from(self
                     , destination_branch
                     , destination_p4cl
                     , first_branch_from_branch_id
                     , first_branch_from_change_number):
        '''
        This is the first commit on a new branch, rearrange files from
        parent branches to match the new branch's view mapping.

        branch_source_dict: dict branch_id ==> int(changelist number)

        destination_p4cl must be the first changelist on this branch.

        Side effect: switches client view to parent branch view,
                     then back to destination branch view.
        '''
        # Need a complete list of every depot file in the child/dest branch
        # after destination_p4cl change brings this branch into existence.
        # Does not include inherited files due to lightweight branching.
        child_map = destination_branch.view_p4map
        child_w2d = { child_map.translate(f.depot_path):f.depot_path
                      for f in destination_p4cl.files }

        # Need a complete list of every file in the parent/source branch view at
        # source changelist, and the depot file that landed there. Includes
        # inherited files due to lightweight branching.
        #
        # From this list we 'D'elete files that are not mapped in the
        # destination branch's view.
                # +++ Branch.p4_files() inserts 'clientFile' path so we don't
                # +++ have to deal with "inherited from some ancestor branch"
                # +++ mapping.
        source_branch   = self.ctx.branch_dict().get(first_branch_from_branch_id)
        source_p4_files = source_branch.p4_files( self.ctx
                                                , first_branch_from_change_number)

        self.ctx.switch_client_view_to_branch(destination_branch)

        if destination_branch.is_lightweight:
            # First commit on a lightweight branch? Then delete only files
            # unmapped by the view switch (which should be none, since
            # lightweight branches copy their parent view.)
            child_c2d_map = child_map.reverse()
            for source_p4_file in source_p4_files:
                pwf = source_p4_file['clientFile']  # Parent Workspace File
                if not child_c2d_map.translate(pwf):
                    path = self.ctx.client_path(pwf).to_gwt()
                    self.__append("D {0}\n".format(path))
                    LOG.debug("__branch_from lt deleted path={}".format(path))
                    continue
        else:
            # First commit on a fully populated branch? Then 'D'elete all files
            # not explicitly listed in the branch changelist.
            for source_p4_file in source_p4_files:
                pwf = source_p4_file['clientFile']  # Parent Workspace File
                pdf = source_p4_file['depotFile']   # Parent Depot     File
                cdf = child_w2d.get(pwf)            # Child  Depot     File

                LOG.debug("__branch_from fp pwf={:<30} pdf={:<40} cdf={:<40}"
                          .format(pwf, pdf, cdf))

                # work_tree holds the same depot_file in both parent and child
                # branch views? Let it carry forward from parent. NOP.
                if pdf == cdf:
                    continue

                # work_tree holds no file in child where parent had a file?
                # Delete it.
                if not cdf:
                    path = self.ctx.client_path(pwf).to_gwt()
                    self.__append("D {0}\n".format(path))
                    LOG.debug("__branch_from fp deleted path={}".format(path))
                    continue

                # Other cases where parent holds different or no file, but child
                # holds a file? Nothing to do here. __add_files() will catch
                # those.

    def __email_for_user(self, username):
        """get email address for a user"""
        user_3tuple = self.usermap.lookup_by_p4user(username)
        if not user_3tuple:
            return _('Unknown Perforce User <{}>').format(username)
        return "<{0}>".format(user_3tuple[p4gf_usermap.TUPLE_INDEX_EMAIL])

    def __full_name_for_user(self, username):
        """get human's first/last name for a user"""

        # First check our cache of previous hits.
        if username in self.username_map:
            return self.username_map[username]

        # Fall back to p4gf_usermap, p4 users.
        user_3tuple = self.usermap.lookup_by_p4user(username)
        if user_3tuple:
            user = p4gf_usermap.tuple_to_P4User(user_3tuple)
        else:
            user = None
        fullname = ''
        if user:
            # remove extraneous whitespace for consistency with Git
            fullname = ' '.join(user.full_name.split())
        self.username_map[username] = fullname
        return fullname

    def _add_parent(self, parent_commit, keyword=NTR('from')):
        '''Add one parent to the commit we're currently building.'''
        # Parent is either SHA1 of an existing commit or mark of a commit
        # created earlier in this import operation. Assume a length of
        # 40 indicates the former and mark ids will always be shorter.
        if isinstance(parent_commit, str) and len(parent_commit) == 40:
            self.__append(NTR('{keyword} {sha1}\n').format(keyword=keyword, sha1=parent_commit))
        else:
            self.__append(NTR('{keyword} :{mark}\n').format(keyword=keyword, mark=parent_commit))

    def _add_commit_parent_list(self, parent_commit_list):
        '''
        Add the one 'parent' or multiple 'merge' commands for a commit currently
        being written to the fast-import script.
        '''
        # If this is not the initial commit, say what it's based on
        # otherwise start with a clean slate
        LOG.debug("add_commit(): parent_commit={}".format(parent_commit_list))
        has_parent = False
        if parent_commit_list:
            if isinstance(parent_commit_list, list):
                for p in parent_commit_list:
                    if has_parent:
                        self._add_parent(p, NTR('merge'))
                    else:
                        self._add_parent(p)
                        has_parent = True
            else:
                self._add_parent(parent_commit_list)
                has_parent = True
        if not has_parent:
            self.__append(NTR('deleteall\n'))

    # pylint: disable=R0913
    # Too many arguments
    #
    # Yeah I know add_commit() is a tad complex. Breaking it into single-use
    # pieces just scatters the complexity across multiple functions, making
    # things less readable. Shut up, pylint.

    def add_commit( self
                  , cl
                  , mark_number
                  , parent_commit_list
                  , first_branch_from_branch_id
                  , first_branch_from_change_number
                  , dest_branch
                  , branch_name):
        """Add a commit to the fast-import script.

        Arguments:
        cl            -- P4Changelist to turn into a commit
        cl.files      -- [] of P4File containing files in changelist
        mark_number   -- Mark number assigned to this commit
        parent_commit_list
                      -- Mark or SHA1 of commit this commit will be based on.
                         Can be a singular str mark/SHA1 or a list of
                         [mark/SHA1 str] if cl should be a merge commit .
        first_branch_from_branch_id
        first_branch_from_change_number
                      -- branch_id and integer changelist number from which we're branching.
                         None unless this is the first commit on a new branch.
        dest_branch   -- Branch that receives this commit.
        """
        with Timer(OVERALL):
            with Timer(BUILD):
                self.__append(NTR('commit refs/heads/{0}\n').format(branch_name))
                self.__append(NTR('mark : {0}\n').format(mark_number))
                desc_info = DescInfo.from_text(cl.description)
                committer_added = False
                if desc_info:
                    for key in ('author', 'committer'):
                        v = desc_info[key]
                        if v:
                            self.__append(NTR('{key} {fullname} {email} {time} {timezone}\n').
                                          format( key      = key
                                                , fullname = v['fullname']
                                                , email    = v['email'   ]
                                                , time     = v['time'    ]
                                                , timezone = v['timezone']))
                            committer_added = True
                    desc = desc_info.clean_desc

                # Convoluted logic gates but avoids duplicating code. The point
                # is that we add the best possible committer data _before_
                # adding the description.
                if not committer_added:
                    if desc_info:
                        # old change description that lacked detailed author info,
                        # deserves a warning, but otherwise push onward even if the
                        # commit checksums will likely differ from the originals
                        LOG.warn('commit description did not match committer regex: @{} => {}'.
                                 format(cl.change, desc_info.suffix))
                    timezone = self.__get_timezone_offset(cl.time)
                    self.__append(NTR('committer {fullname} {email} {time} {timezone}\n').
                                  format(fullname=self.__full_name_for_user(cl.user),
                                         email=self.__email_for_user(cl.user),
                                         time=cl.time,
                                         timezone=timezone))
                    desc = cl.description
                self.__add_data(desc)

                self._add_commit_parent_list(parent_commit_list)
                if      first_branch_from_branch_id \
                    and first_branch_from_change_number:
                    self.__branch_from( dest_branch
                                      , cl
                                      , first_branch_from_branch_id
                                      , first_branch_from_change_number)
                self.__add_files(cl.files)
                if desc_info and desc_info.gitlinks:
                    self.__add_gitlinks(desc_info.gitlinks)

    def run_fast_import(self):
        """Run git-fast-import to create the git commits.

        Returns: a list of commits.  Each line is formatted as
            a change number followed by the SHA1 of the commit.

        The returned list is also written to a file called marks.
        """
        with Timer(OVERALL):
            with Timer(RUN):
                LOG.debug("running git fast-import")
                # tell git-fast-import to export marks to a temp file
                self.script.flush()
                marks_file = tempfile.NamedTemporaryFile(dir=self.ctx.tempdir.name, prefix='marks-')
                try:
                    cmd = ['git', 'fast-import', '--quiet', '--export-marks=' + marks_file.name]
                    ec = p4gf_proc.wait(cmd, stdin=self.script.name)
                    if ec:
                        _log_crash_report()
                        raise CalledProcessError(ec, NTR('git fast-import'))

                    #read the exported marks from file and return result
                    with open(marks_file.name, "r") as marksfile:
                        marks = [l.strip() for l in marksfile.readlines()]
                    if LOG.getChild('marks').isEnabledFor(logging.DEBUG3):
                        LOG.getChild('marks').debug3('git-fast-import returned marks ct={}\n'
                                                     .format(len(marks))
                                                     + '\n'.join(marks))
                    return marks
                finally:
                    self.script.close()
                    marks_file.close()

    def cleanup(self):
        '''
        Ensure temporary files are closed so they may be deleted.
        '''
        self.script.close()

    def __repr__(self):
        return "\n".join([repr(self.ctx),
                          "timezone                : " + self.timezone,
                          "project_root_path_length: " + str(self.project_root_path_length),
                          "script length           : {} lines".format(self._line_count),
                          "script size             : {} bytes".format(self._byte_count)
                          ])

    def __str__(self):
        return "\n".join([str(self.ctx),
                          "timezone                : " + self.timezone,
                          "project_root_path_length: " + str(self.project_root_path_length),
                          "script length           : {} lines".format(self._line_count),
                          "script size             : {} bytes".format(self._byte_count)
                          ])
