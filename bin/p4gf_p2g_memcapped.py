#! /usr/bin/env python3.3
'''Memory-capped version of p4gf_copy_to_git.'''
from   collections import namedtuple, deque
import logging

#mport gc
import hashlib
import os
import pygit2
import re
import shutil
import tempfile
import zlib

import p4gf_branch
import p4gf_gc
from   p4gf_p4changelist  import P4Changelist
from   p4gf_p4file        import P4File
import p4gf_proc
import p4gf_progress_reporter as ProgressReporter
import p4gf_tag
import p4gf_util

from P4 import OutputHandler, P4, P4Exception

# pylint:disable=W0212
# Access to a protected member of P2G.
# Yeah, we're claiming to be part of that class.

# pylint:disable=W9903
# non-gettext-ed string
# This debugging dump module requires no translation.

# Broken/missing
#
# graft
# detecting lightweight branches that serve as integ sources for known branches
#       can we feed the 'p4 filelog' data back into our change_num_on_branch
#       list upon detecting a new integ source?

LOG = logging.getLogger(__name__)



                        # Please keep P2GMemCapped at the top of this file.

class P2GMemcapped:
    '''
    State and code while copying Perforce changelists and
    file revisions from Perforce to Git.

    Copies from Perforce to Git while storing very little data across the entire
    process.

    All of the file revisions and data for a single changelist is brought into
    memory only while copying that one changelist, then forgotten to make room
    for the next changelist.

    Record individual file revision ==> blob sha1 links as filesystem symbolic
    links rather than in memory. Huge histories will have more file revisions
    than we can hold in memory.
    '''

    def __init__(self, p2g):

        self.ctx = p2g.ctx
        self.p2g = p2g

                        # Temp directory under which we store all
                        # depot/dir/file#1 symlinks to 'p4 print'ed-and-blobbed
                        # content.
        self.symlink_dir = os.path.join(self.ctx.tempdir.name, 'printed_symlinks')

                        # List of ChangeNumOnBranch
                        # Sorted by change_num, descending
                        #
                        # Using a deque so that I can use this as a work queue,
                        # pop_left() the next changelist to copy.
                        #
        self.change_num_on_branch_list = deque()

        self.print_handler = None

                        # int. Ratchets forward during _copy_one().
        self.highest_copied_change_num = 0

                        # Instrumentation
        self.cnob_count         = 0
        self.printed_rev_count  = 0
        self.printed_byte_count = 0

                        # mark number (? real, fake ?) to ?
        self.mark_to_branch_id  = {}

                        # branch ID to ?
        self.branch_id_to_temp_name = {}

                        # Depot branches that house lightweight branches
                        # we already know about, already have mapped through
                        # at least one Branch view.
        self.known_dbi_set          = {}

                        # Please keep __init__, copy, and _copy_one()
                        # as the top 3 functions.

                        # pylint:disable=R0915
                        # Too many statements
                        # Agreed. Clean this mess up into a decider,
                        # not a low-level doer.
    def copy(self, start_at, stop_at, new_git_branches):
        """copy a set of changelists from perforce into git"""

        LOG.debug('copy() start={} stop={} new_git_branches={}'
                  .format(start_at, stop_at, new_git_branches))

        with self.p2g.perf.timer[OVERALL]:
            self.p2g._log_memory('start')

            # Stop early if nothing to copy.
            # 'p4 changes -m1 //client/...'
            repo_empty = p4gf_util.git_empty()
            if not self._requires_copy(new_git_branches, repo_empty):
                self.p2g.fastimport.cleanup()
                LOG.debug("No changes since last copy.")
                return

            ### Fake start. Needs proper setup with graft support.
            self.p2g.rev_range = self.p2g.mc_rev_range()

            self.ctx.view_repo = pygit2.Repository(self.ctx.view_dirs.GIT_DIR)
            self.p2g._log_memory('pygit2')

            # O(Bfp + 1) p4 changes: one for each Branch fully populated,
            #                        + 1 for //.git-fusion/branches/repo/...
            #
            with self.p2g.perf.timer[CHANGES]:
                self.change_num_on_branch_list \
                                        = deque(self._p4_changes_each_branch())
            LOG.debug('Found ChangeNumOnBranch count: {}'
                      .format(len(self.change_num_on_branch_list)))
            self.p2g._log_memory(        'O(Bfp + 1) p4 changes')
            #p4gf_gc.report_growth ('aftr O(Bfp + 1) p4 changes')
            #p4gf_gc.report_objects('aftr O(Bfp + 1) p4 changes')
            if not self.change_num_on_branch_list:
                LOG.debug("No new changes found to copy")
                return

                        # Prepare for the loop over each changelist x branch.
            p4gf_util.ensure_dir(self.symlink_dir)
            self.p2g._fill_head_marks_from_current_heads()
            self.mark_to_branch_id = {}
            self.branch_id_to_temp_name \
                                = self.p2g._create_branch_id_to_temp_name_dict()

            self.known_dbi_set = { branch.depot_branch
                               for branch in self.ctx.branch_dict().values()
                               if branch.depot_branch }
                        # All string placeholders should have been replaced with
                        # pointers to full DepotBranchInfo instances before
                        # calling copy().
            for dbi in self.known_dbi_set:
                assert not isinstance(dbi, str)
                LOG.error('known: {}'.format(dbi))  ##ZZ

            # O(C) 'p4 changes -m1 + filelog + print'
            #
            # Print each file revision to its blob in the .git/objects/
            # Write each changelist to git-fast-import script.
            #
            with ProgressReporter.Indeterminate():
                while self.change_num_on_branch_list:
                    ProgressReporter.increment("MC Copying changelists...")
                    cnob = self.change_num_on_branch_list.pop()
                    self._copy_one(cnob)

                        # Explicitly delete the PrintHandler now so that it
                        # won't show up in any leak reports between now and
                        # P2GMemCapped's end-of-life.
            if self.print_handler:
                self.printed_byte_count = self.print_handler.total_byte_count
                self.printed_rev_count  = self.print_handler.printed_rev_count
                self.print_handler      = None

            self.p2g._log_memory(        'P2G_MC.copy() loop')
            p4gf_gc.report_growth ('after P2G_MC.copy() loop')
            p4gf_gc.report_objects('after P2G_MC.copy() loop')
            #p4gf_gc.backref_objects_by_type(dict().__class__)

            # Run git-fast-import to add everything to Git.
            with self.p2g.perf.timer[FAST_IMPORT]:
                LOG.info('Running git-fast-import')
                marks = self.p2g.fastimport.run_fast_import()

                        # Remove all temporary Git branch refs.
                        # After git-fast-import, we no longer need them.
            self._delete_temp_git_branch_refs()

                        # Record how much we've copied in a p4 counter so that
                        # future calls to _any_changes_since_last_copy() can
                        # tell if there's anything new to copy.
            self.ctx.write_last_copied_change(self.highest_copied_change_num)

            if repo_empty:
                # If we are just now rebuilding the Git repository, also
                # grab all of the tags that have been pushed in the past.
                p4gf_tag.generate_tags(self.ctx)
                self.p2g._log_memory('_generate_tags')

            with self.p2g.perf.timer[MIRROR]:
                self.p2g._mirror(marks, self.mark_to_branch_id)
                self.p2g._log_memory('_mirror')

            with self.p2g.perf.timer[BRANCH_REF]:
                self.p2g._set_branch_refs(marks)
                self.p2g._log_memory('_set_branch_refs')

            with self.p2g.perf.timer[PACK]:
                self.p2g._pack()
                self.p2g._log_memory('_pack')

        LOG.getChild("time").debug("\n" + str(self))
        LOG.info('MC Done. Commits: {cnob_ct:,d}  File Revisions: {rev_ct:,d}'
                 '  Bytes: {byte_ct:,d}  Seconds: {sec:,d}'
                 .format( cnob_ct = self.cnob_count
                        , rev_ct  = self.printed_rev_count
                        , byte_ct = self.printed_byte_count
                        , sec     = int(self.p2g.perf.timer[OVERALL].time)
                        ))
        p4gf_gc.report_objects('after P2G MC copy()')
        self.p2g._log_memory('copy() done')

    def _copy_one(self, cnob):
        '''
        Copy one ChangeNumOnBranch element from Perforce to Git.

        p4 print all of its file revisions directly into .git/objects as blobs
        add them to the git-fast-import script
        '''
        _debug2('_copy_one {}', cnob)
        branch = self.ctx.branch_dict().get(cnob.branch_id)
        with self.ctx.switched_to_branch(branch):

                        # Keep track of the highest changelist number we've
                        # copied. Can't rely on
                        # self.change_num_on_branch_list[-1] starting with our
                        # highest changelist number because we might discover
                        # new branches during later calls to _copy_one().
            change_num = int(cnob.change_num)
            if self.highest_copied_change_num < change_num:
                self.highest_copied_change_num = change_num
            self.cnob_count += 1

            # p4 changes -l -m1 @nnn
            #
            # Gets changelist description (including possible DescInfo),
            # owner, time.
            with self.p2g.perf.timer[CHANGES1]:
                r = self.ctx.p4run([ 'changes'
                                   , '-l'   # include full changelist description
                                   , '-m1'  # just this one changelist
                                   , '@{}'.format(cnob.change_num)])
                p4changelist = P4Changelist.create_using_changes(r[0])

            # p4 filelog -c nnnn -m1 //change_path/...
            #
            # Gets integration sources for parent calculations.
            # Gets files deleted at this rev (which 'p4 print' won't on 11.1).
            # Gets file list for this changelist.
            #
            # Cannot use p4 filelog //{client}/...@=nnn
            # That does request does not return one fstat for each file
            # in changelist nnn.
            with self.p2g.perf.timer[FILELOG]:
                cmd = [ 'filelog'
                      , '-c', cnob.change_num
                      , '-m1'
                      , cnob.path]
                filelog_results = self.ctx.p4run(cmd)

            ### Detect lightweight integration sources not yet known.
            ### Create new Branch views to map them into this repo,
            ### run 'p4 changes' on them to add their history to our
            ### change_num_on_branch_list work queue, sorted.
            dbil = self.p2g.to_depot_branch_list_mc(change_num, filelog_results)
            new_dbi_set = set(dbil) - self.known_dbi_set
            if new_dbi_set:
                ### push_front cnob
                for dbi in new_dbi_set:
                    LOG.error('AHA detected new integ source: {}'.format(dbi))
                    ### process dbi into branch, branch into more cnobs
                    ### mergesort new cnobs into cnob deque
                self.known_dbi_set.update(new_dbi_set)
                ### return, we'll deal with this later
                ### +++ save changes and filelog work

            # p4 print every revision modified by this changelist.
            #
            # +++ Also print every revision AFTER this changelist. There's a
            # +++ high probability that we'll need those revisons later.
            # +++ Printing them all now _greatly_ reduces the total number of
            # +++ 'p4 print' requests, reduces the Perforce server's workload
            # +++ (and thus repsponse time) in generating incremental file
            # +++ revisions from any files stored using RCS deltas (aka most
            # +++ files).
            with self.p2g.perf.timer[CALC_PRINT]:
                depot_path_rev_list = []
                for rr in filelog_results:
                    if (   (not isinstance(rr, dict))
                        or ('depotFile' not in rr)
                        or ('rev'       not in rr)):
                        continue
                    p4file = P4File.create_from_filelog(rr)
                    p4changelist.files.append(p4file)

                    depot_path = rr['depotFile']
                    rev        = rr['rev'][0]
                    if self._already_printed(depot_path, rev):
                        continue
                    depot_path_rev_list.append('{}#{},head'
                                               .format(depot_path, rev))

            rev_total = len(p4changelist.files)
            _debug2('Printing files.'
                    '  change: {change_num}'
                    '  total: {rev_total}'
                    '  need_print: {rev_need_print}'
                    '  already_printed: {rev_already_printed}'
                    , change_num          = cnob.change_num
                    , rev_need_print      = len(depot_path_rev_list)
                    , rev_already_printed = rev_total - len(depot_path_rev_list)
                    , rev_total           = rev_total)

            if depot_path_rev_list:
                with self.p2g.perf.timer[PRINT2]:
                    printhandler = self._print_handler()
                    server_can_unexpand = self.ctx.p4.server_level > 32
                    args = ["-a"]
                    if server_can_unexpand:
                        args.append("-k")
                    cmd = ['print'] + args + depot_path_rev_list
                    with p4gf_util.RawEncoding(self.ctx.p4)             \
                    ,    p4gf_util.Handler(self.ctx.p4, printhandler)   \
                    ,    self.ctx.p4.at_exception_level(P4.RAISE_ALL):
                        self.ctx.p4run(cmd)
                    printhandler.flush()

            # Find each file revision's blob sha1.
            for p4file in p4changelist.files:
                symlink_path = _depot_rev_to_symlink(
                                              depot_path  = p4file.depot_path
                                            , rev         = p4file.revision
                                            , symlink_dir = self.symlink_dir )
                blob_path   = os.readlink(symlink_path)
                p4file.sha1 = _blob_path_to_sha1(blob_path)

            # If we can copy the Git commit and its tree objects from
            # our gitmirror, do so.
                        # Non-MemCapped code calls all FI functions with
                        # timer[FAST_IMPORT] as outer container, so must we.
            with self.p2g.perf.timer[FAST_IMPORT]:
                if self.p2g._fast_import_from_gitmirror(p4changelist, branch):
                    LOG.debug2('@{} fast-imported from gitmirror.'
                               .format(cnob.change_num))
                    return

                # Build a git-fast-import commit object.

                        ### _fast_import_from_p4() runs its own filelog to
                        ### discover integ sources. That needs to be hoisted up
                        ### to our own filelog and passed down to avoid
                        ### duplicate work.

                LOG.debug2('@{} fast-importing from p4 changelist.'
                           .format(cnob.change_num))
                self.p2g._fast_import_from_p4_mc(
                      change                 = p4changelist
                    , branch                 = branch
                    , filelog_results        = filelog_results
                    , mark_to_branch_id      = self.mark_to_branch_id
                    , branch_id_to_temp_name = self.branch_id_to_temp_name )

    def _requires_copy(self, new_git_branches, is_repo_empty):
        '''
        If we have any new changes, OR new git branches, or an empty Git repo
        that needs initial data, then we have to run copy().
        '''
        return (   is_repo_empty
                or self.p2g._any_changes_since_last_copy()
                or new_git_branches )

    def _delete_temp_git_branch_refs(self):
        '''
        All those temporary Git branch refs whose names we assigned in
        _create_branch_id_to_temp_name_dict()? 'git branch -D' them.
        '''
        for name in self.branch_id_to_temp_name.values():
            ### unable to use pygit2 to delete reference, no idea why
            p4gf_proc.popen_no_throw(['git', 'branch', '-D', name])

    @staticmethod
    def _can_create_cnob(changes_dict):
        '''
        Can a Perforce 'changes' dict r be used to create a ChangeNumToBranch object?
        '''
        if not isinstance(changes_dict, dict):
            return False
        if 'change' not in changes_dict:
            return False
        if 'path'   not in changes_dict:
            return False
        return True

    @staticmethod
    def _to_cnob(changes_dict, branch_id):
        '''
        Create a new ChangeNumToBranch object from a 'p4 changes' dict
        and a branch id.
        '''
        return ChangeNumOnBranch( change_num = int(changes_dict['change'])
                                , branch_id  = branch_id
                                , path       = changes_dict['path'] )

    def _p4_changes_each_branch(self):
        '''
        Run 'p4 changes' on each branch view. Once for each named branch, and
        then one big one for the union of all lightweight branches.

        Return list of ChangeNumOnBranch, sorted by change_num.
        '''
        l = []

        client_path_fmt = self.ctx.client_view_path() + '@{begin},#head'

        sub_l = []
        for branch in self.ctx.branch_dict().values():
            with self.ctx.switched_to_branch(branch):
                start_change_num = 1        ### setup needs to fetch real start.

                # 'p4 changes //{branch}/...@1,#head'
                r = self.ctx.p4run([ 'changes'
                                    , client_path_fmt.format(begin=start_change_num)])
                _debug2('branch={branch} change_ct={change_ct}'
                       , branch     = p4gf_util.abbrev(branch.branch_id)
                       , change_ct = len(r) )
            for rr in r:
                if not self._can_create_cnob(rr):
                    continue
                sub_l.append(self._to_cnob(rr, branch.branch_id))

            l = _merge(l, sub_l)
            sub_l = []

        # Build a union view of just the lightweight branches. Usually we could
        # just use //.git-fusion/branches/{repo}/... , but that would prevent us
        # from sharing lightweight branches across multiple repos or after a
        # rerepo.
        lw_dict = { b.branch_id : b for b in self.ctx.branch_dict().values()
                                    if b.is_lightweight }
        lw_p4map = p4gf_branch.calc_branch_union_client_view(
                      self.ctx.config.p4client, lw_dict)
        with self.ctx.switched_to_view_lines(lw_p4map.as_array):
            start_change_num = 1        ### setup needs to fetch real start.
            # 'p4 changes //{union}/...@1,#head'
            r = self.ctx.p4run([ 'changes'
                                , client_path_fmt.format(begin=start_change_num)])
        for rr in r:
            if not self._can_create_cnob(rr):
                continue
            for lw_branch in lw_dict.values:
                if lw_branch.intersects_depot_path(rr['path']):
                    sub_l.append(self._to_cnob(rr, lw_branch.branch_id))
                    break

        l = _merge(l, sub_l)
        return l

    def _print_handler(self):
        '''
        Lazy create our PrintHandler.
        '''
        if not self.print_handler:
            server_can_unexpand = self.ctx.p4.server_level > 32
            self.print_handler = PrintHandlerMC(
                                      need_unexpand = not server_can_unexpand
                                    , tempdir       = self.ctx.tempdir.name
                                    , symlink_dir   = self.symlink_dir
                                    , p4            = self.ctx.p4 )
        return self.print_handler

    def _already_printed(self, depot_path, rev):
        '''
        Have we already printed this file revision?
        '''
        symlink_path = _depot_rev_to_symlink(
                                      depot_path  = depot_path
                                    , rev         = rev
                                    , symlink_dir = self.symlink_dir )
        e = os.path.islink(symlink_path)
        _debug3('_already_printed={} {}', 1 if e else 0, symlink_path)
        return e

# -- end class P2GMemCapped ---------------------------------------------------

                        # pylint:disable=C0103,R0201
                        # C0103 Invalid name
                        # These names are imposed by P4Python
                        # R0201 Method could be a function

class PrintHandlerMC(OutputHandler):

    """OutputHandler for p4 print, hashes files into git repo"""
    def __init__(self, need_unexpand, tempdir, symlink_dir, p4):
        OutputHandler.__init__(self)
        self.rev = None
        self.need_unexpand = need_unexpand
        self.tempfile = None
        self.tempdir = tempdir
        self.symlink_dir = symlink_dir
        self.p4 = p4

                        # Instrumentation
        self.total_byte_count  = 0
        self.printed_rev_count = 0

    def outputBinary(self, h):
        """assemble file content, then pass it to hasher via temp file"""
        self.appendContent(h)
        return OutputHandler.HANDLED

    def outputText(self, h):
        """assemble file content, then pass it to hasher via temp file

        Either str or bytearray can be passed to outputText.  Since we
        need to write this to a file and calculate a SHA1, we need bytes.

        For unicode servers, we have a charset specified which is used to
        convert a str to bytes.

        For a nonunicode server, we will have specified "raw" encoding to
        P4Python, so we should never see a str.
        """
        if self.p4.charset:
            try:
                # self.p4.__convert() doesn't work correctly here
                if type(h) == str:
                    b = getattr(self.p4, '__convert')(self.p4.charset, h)
                else:
                    b = getattr(self.p4, '__convert')(self.p4.charset, h.decode())
            except:
                msg = "error: failed {} conversion for {}#{}".format(
                    self.p4.charset, self.rev.depot_path, self.rev.revision)
                raise P4Exception(msg)
        else:
            if type(h) == str:
                raise RuntimeError("unexpected outputText")
            b = h
        self.appendContent(b)
        return OutputHandler.HANDLED

    def appendContent(self, h):
        """append a chunk of content to the temp file

        if server is 12.1 or older it may be sending expanded ktext files
        so we need to unexpand them.  Note that ktext can come through
        either outputBinary or outputText.

        It would be nice to incrementally compress and hash the file
        but that requires knowing the size up front, which p4 print does
        not currently supply.  If/when it does, this can be reworked to
        be more efficient with large files.  As it is, as long as the
        TemporaryFile doesn't rollover, it won't make much of a difference.

        So with that limitation, the incoming content is stuffed into
        a TemporaryFile.
        """
        if not len(h):
            return
        if self.need_unexpand and self.rev.is_k_type():
            h = unexpand(h)
        self.tempfile.write(h)

    def flush(self):
        """compress the last file, hash it and stick it in the repo

        Now that we've got the complete file contents, the header can be
        created and used along with the spooled content to create the sha1
        and zlib compressed blob content.  Finally that is written into
        the .git/objects dir.
        """
        if not self.rev:
            return
        size = self.tempfile.tell()
        if size > 0 and self.rev.is_symlink():
            # p4 print adds a trailing newline, which is no good for symlinks.
            self.tempfile.seek(-1, 2)
            b = self.tempfile.read(1)
            if b[0] == 10:
                size = self.tempfile.truncate(size - 1)
        self.tempfile.seek(0)
        self.total_byte_count += size
        self.printed_rev_count += 1
        compressed = tempfile.NamedTemporaryFile(delete=False, dir=self.tempdir,
                                                 prefix='p2g-blob-')
        compress = zlib.compressobj()
        # pylint doesn't understand dynamic definition of sha1 in hashlib
        # pylint: disable=E1101
        sha1 = hashlib.sha1()

        # pylint:disable=W1401
        # disable complaints about the null. We need that.
        # add header first
        header = ("blob " + str(size) + "\0").encode()
        compressed.write(compress.compress(header))
        sha1.update(header)

        # then actual contents
        chunksize = 4096
        while True:
            chunk = self.tempfile.read(chunksize)
            if chunk:
                compressed.write(compress.compress(chunk))
                sha1.update(chunk)
            else:
                break
        # pylint: enable=E1101
        compressed.write(compress.flush())
        compressed.close()
        digest = sha1.hexdigest()
        self.rev.sha1 = digest
        blob_path_tuple = _sha1_to_blob_path_tuple(self.rev.sha1)
        if not os.path.exists(blob_path_tuple.path):
            if not os.path.exists(blob_path_tuple.dir):
                os.makedirs(blob_path_tuple.dir)
            shutil.move(compressed.name, blob_path_tuple.path)
        else:
            os.remove(compressed.name)
        #self.revs.append(self.rev)
        symlink_path = _depot_rev_to_symlink( depot_path  = self.rev.depot_path
                                            , rev         = self.rev.revision
                                            , symlink_dir = self.symlink_dir )
        p4gf_util.ensure_parent_dir(symlink_path)

        e = os.path.islink(symlink_path)
        if not e:
            os.symlink(blob_path_tuple.path, symlink_path)

        _debug3('Printed {e} {blob} @{ch:<5} {rev:<50} {symlink}'
               , blob    = blob_path_tuple.path
               , symlink = symlink_path
               , rev     = self.rev.rev_path()
               , ch      = self.rev.change
               , e       = 'e' if e else ' ')
        self.rev = None

    def outputStat(self, h):
        """save path of current file"""
        self.flush()
        self.rev = P4File.create_from_print(h)
        #self.change_set.add(self.rev.change)

                        # Not sure I want to bump this for every single file
                        # revision. That's frequent enough to slow down this
                        # print phase.
        #ProgressReporter.increment('MC Copying file revisions')

                        # This doubles our log output. Merge this with flush(),
                        # uncomment only when you need to debug PrintHandler itself.
                        #
                        # Spaces here to align depot path with flush() Printed.
        #_debug3( 'PrintHandler.outputStat() ch={:<5}'
        #         '                               {}#{}'
        #       , self.rev.change
        #       , self.rev.depot_path
        #       , self.rev.revision )

        if self.tempfile:
            self.tempfile.seek(0)
            self.tempfile.truncate()
        else:
            self.tempfile = tempfile.TemporaryFile(buffering=10000000, dir=self.tempdir,
                                                   prefix='p2g-print-')
        return OutputHandler.HANDLED

    def outputInfo(self, _h):
        """outputInfo call not expected"""
        return OutputHandler.REPORT

    def outputMessage(self, _h):
        """outputMessage call not expected, indicates an error"""
        return OutputHandler.REPORT
                        # pylint:enable=C0103,R0201


def _depot_rev_to_symlink(depot_path, rev, symlink_dir):
    '''
    Return {symlink_dir}/depot/dir/file#1

    This is the symlink that we create upon printing a file, points to the
    Git blob file that holds the file's zlibbed content.
    '''
    depot_ish = '{}#{}'.format(depot_path[2:], rev)
    return os.path.join(symlink_dir, depot_ish)


# Element of change_num_on_branch_list, always sorted by change_num.
ChangeNumOnBranch = namedtuple( 'ChangeNumOnBranch'
                              , [ 'change_num'      # int
                                , 'branch_id'
                                , 'path'])


def _merge(left, right):
    '''
    Merge two sorted lists, return resulting combined list,
    sorted by change_num, DESCENDING.

    Assume inputs are already sorted by change_num, descending
    '''
    result = []
    li = 0
    ri = 0
    li_end = len(left)
    ri_end = len(right)
    while li < li_end and ri < ri_end:
        # Descending sort: take highest of the two heads.
        if left[li].change_num <= right[ri].change_num:
            result.append(right[ri])
            ri += 1
        else:
            result.append(left[li])
            li += 1

    # Any leftovers?
    if li < li_end:
        result.extend(left[li:])
    if ri < ri_end:
        result.extend(right[ri:])

    return result

# pattern for unexpanding keywords
KEYWORD_PATTERN = re.compile(r'\$(?P<keyword>Author|Change|Date|DateTime'
                             + r'|File|Header|Id|Revision):[^$\n]*\$')
def unexpand(line):
    """unexpand a line from keyword expanded file
    line is an array of bytes in unknown encoding.  To use regex to unexpand
    it, it must be converted to str.  Using latin-1 to decode/encode will
    safely handle any bytes with no decode failures.
    """
    return KEYWORD_PATTERN .sub(r'$\g<keyword>$', line.decode('latin-1')).encode('latin-1')


def _debug3(msg, *arg, **kwarg):
    '''
    If logging at DEBUG3, do so. If not, do nothing.
    '''
    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3(msg.format(*arg, **kwarg))


def _debug2(msg, *arg, **kwarg):
    '''
    If logging at DEBUG2, do so. If not, do nothing.
    '''
    if LOG.isEnabledFor(logging.DEBUG2):
        LOG.debug2(msg.format(*arg, **kwarg))


BlobPathTuple = namedtuple('BlobPathTuple', ['sha1', 'dir', 'file', 'path'])
BLOB_PATH_PREFIX = '.git/objects/'

def _sha1_to_blob_path_tuple(sha1):
    '''
    Return a BlobPathTuple where the blob file should go.

    path is a RELATIVE path ".git/objects/xx/x{38}"
    '''
    blob_dir  = BLOB_PATH_PREFIX + sha1[:2]
    blob_file = sha1[2:]
    blob_path = blob_dir+"/"+blob_file
    return BlobPathTuple( sha1 = sha1
                        , dir  = blob_dir
                        , file = blob_file
                        , path = blob_path )


def _blob_path_to_sha1(path):
    '''
    Convert a BlobPathTuple.path back to its original sha1.
    '''
    assert path.startswith(BLOB_PATH_PREFIX)
    return path[len(BLOB_PATH_PREFIX):].replace('/', '')


OVERALL     = "P4 to Git Overall"
CHANGES     = "p4 changes"
CHANGES1    = "p4 changes -m1 -l"
FILELOG     = "p4 filelog"
PRINT2      = "print mc"
CALC_PRINT  = "calc print"
FAST_IMPORT = "Fast Import"
MIRROR      = "Mirror"
BRANCH_REF  = "Branch Ref"
PACK        = "Pack"
