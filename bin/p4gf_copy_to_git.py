#! /usr/bin/env python3.3
"""copy_p4_changes_to_git"""

from   collections              import namedtuple
import logging
import sys

import pygit2

from p4gf_desc_info             import DescInfo
from p4gf_fastimport            import FastImport
from p4gf_fastimport_mark       import Mark
from p4gf_fastimport_marklist   import MarkList
from p4gf_l10n                  import _, NTR
from p4gf_p2g_changelist_cache  import ChangelistCache
from p4gf_p2g_filelog_cache     import FilelogCache
from p4gf_p2g_memcapped         import P2GMemcapped
from p4gf_p2g_print_handler     import PrintHandler
from p4gf_p2g_rev_range         import RevRange
from p4gf_p4changelist          import P4Changelist
from p4gf_gitmirror             import GitMirror
from p4gf_parent_commit_list    import ParentCommitList
from p4gf_object_type           import ObjectType
from p4gf_profiler              import Timer

import p4gf_branch
import p4gf_config
import p4gf_const
import p4gf_filelog_action
import p4gf_git
import p4gf_log
import p4gf_gc
import p4gf_proc
import p4gf_progress_reporter as ProgressReporter
import p4gf_tag
import p4gf_util

from P4 import P4

LOG = logging.getLogger(__name__)
LOG_MEMORY = logging.getLogger('memory')

MEM_CAPPED = False       # Set to True to use P2GMemCapped,
                        # False to use original 13.1 'git pull' code.

class P2G:
    """class to manage copying from Perforce to git"""
    def __init__(self, ctx):
        self.ctx = ctx
        self.fastimport = FastImport(self.ctx)

        self.new_branch_start   = "1"
        self.is_graft           = False
        self.stop_at            = "#head"
        self.current_branch_id  = None
        self.rev_range          = None  # RevRange instance set in copy().
                                        # dict[branch_id] to P4Changelist, but
                                        # no P4File elements in that P4Changelist
        self.branch_id_to_graft_change = None
        self.branch_id_to_graft_num = None

                                    # dict branch_id => ('@nn', end)
                                    # where end is either '@mm' or a commit sha1
                                    #
                                    # _get_branch_start_list() fills for all
                                    # known branches. Then _setup() removes any
                                    # branches with no new changelists.
        self.branch_start_list  = None

        self.changes            = ChangelistCache(self)  # changelists to copy
        self.graft_changes      = None  # graft point changelists to copy (# only)
        self.printed_revs       = None  # RevList produced by PrintHandler
        self.printed_rev_count  = 0

        self.status_verbose     = True

                                    # Where the current head of each branch
                                    # should go once we're done.
                                    # Values are BranchHead tuples.
                                    # Set/updated by _fast_import().
        self._branch_id_to_head = {}

                                    # 1:N 1 p4 changelist number
                                    #     N git-fast-import mark numbers
                                    # Usually 1:1 unless a single Perforce
                                    # changelist touches multiple branches.
                                    # Assigned in _fast_import(), used in
                                    # _mirror()
        self.mark_list          = MarkList()
        self.sha1_to_mark       = dict()

                                    # Cached results for
                                    # filelog_to_integ_source_list()
        self._filelog_cache     = FilelogCache(self)
        self._branch_info_cache = {}# Cached results for _to_depot_branch_set

                                    # Filled and used by _sha1_exists().
                                    # Contains commit, tree, ANY type of sha1.
        self._sha1s_known_to_exist  = set()

                                    # Most recent ghost changelist seen (and
                                    # skipped) for that branch. Values are
                                    # SkippedGhost tuples.
        self._branch_id_to_skipped_ghost = {}

    def __str__(self):
        return "\n".join(["\n\nFast Import:\n",
                          str(self.fastimport)
                          ])

    def get_branch_id_to_graft_num(self,
                                   ctx,
                                   start_at): # "@NNN" Perforce changelist num
        """For a graft. collect the highest P4 CL per git branch"""

        # Are there any PREVIOUS Perforce changelists before the requested
        # start of history? If so, then we'll need to graft our history onto
        # that previous point.
        if start_at != "@1":
            # Possibly (probably) grafting history: we need to know the correct
            # changelist that will be our first real changelist copied to Git.
            begin_change_num = int(start_at[1:])
            ctx.switch_client_view_to_union()
            path = ctx.client_view_path()
            path_at = path + start_at
            changes_result = ctx.p4run(["changes", "-m1", path_at])
            if not changes_result:
                # Rare surprise: there are no changes at or before the start
                # revision specifier, do not need to graft history.
                return

            LOG.debug("begin_change_num={}".format(begin_change_num))

            # Check each branch for history before that start. That history
            # gets truncated down to a graft-like commit.
            self.branch_id_to_graft_num = {}
            for branch in ctx.undeleted_branches():
                # Each branch gets its own graft commit (or possibly None).
                ctx.switch_client_view_to_branch(branch)
                changes_result = ctx.p4run(["changes", "-m2", path_at])
                # Highest changelist that comes before our start is this
                # branch's graft changelist.
                max_before = 0
                for change in changes_result:
                    change_num = int(change['change'])
                    if max_before < change_num < begin_change_num:
                        max_before = change_num
                if max_before:
                    self.branch_id_to_graft_num[branch.branch_id] = max_before
                    LOG.debug("graft={} for branch={}"
                              .format(max_before, branch.branch_id))

            if self.branch_id_to_graft_num:
                self.branch_id_to_graft_change = {}
                for branch_id, graft_num \
                    in self.branch_id_to_graft_num.items():
                    # Ignore all depotFile elements, we just want the
                    # change/desc/time/user. depotFiles here are insufficient,
                    # don't include depotFiles from before this change, which
                    # we'll fold in later, during grafting.
                    p4cl = P4Changelist.create_using_describe( self.ctx.p4
                                                             , graft_num
                                                             , 'ignore_depot_files')
                        # pylint:disable=W9905
                        # Lacks single quotes and that's okay here.
                    p4cl.description += (_("\n[grafted history before {start_at}]")
                                         .format(start_at=start_at))
                    self.branch_id_to_graft_change[branch_id] = p4cl
                        # pylint:enable=W9905


            # Restore the world so that calling code does not have to.
            ctx.switch_client_view_to_union()

    def mc_rev_range(self):
        '''
        P2GMemCapped version of setup().

        ### Temporary scaffolding until we can get even ONE successful pull.
        ### API needs start/stop, probably also need to discover and store all
        ### known branch head changelist/sha1
        '''
        return RevRange.from_start_stop(self.ctx)

    def _anon_branch_head(self, branch):
        '''
        Return the highest numbered changelist in a branch view that also has
        a corresponding Git commit already existing in the Git repo.
        Returns a 2-tuple (changelist number, commit sha1)

        Return (None, None) if no such changelist or commit.
        '''
        commit = ObjectType.last_change_for_branches(self.ctx,
                                                     [branch.branch_id],
                                                     must_exist_local=True)
        if commit:
            return (commit.details.changelist, commit.sha1)
        return (None, None)

    def _get_branch_start_list(self, start_at=None):
        '''
        Store a dictionary[branch_id] => 2-tuple ("@change_num", sha1)

        Where change_num is the highest numbered Perforce changelist already
        copied to Git, sha1 is its corresponding Git commit.

        Calling code starts the Perforce-to-Git copy AFTER the returned tuples.
        '''
        self.branch_start_list = {}
        for v in self.ctx.undeleted_branches():
            if  start_at and start_at.startswith("@"):
                self.branch_start_list[v.branch_id] = (start_at, start_at)
                continue
            if not v.git_branch_name:
                (ch, sha1) = self._anon_branch_head(v)
                if not (ch and sha1):
                    ch   = 1
                    sha1 = "@1"
                self.branch_start_list[v.branch_id] = ('@{}'.format(ch), sha1)
                continue

            commit_sha1 = p4gf_util.sha1_for_branch(v.git_branch_name)
            changelist = ObjectType.change_for_sha1(self.ctx,
                                                    commit_sha1,
                                                    v.branch_id)
            if changelist:
                branch_highest_changelist_number = changelist  + 1
                branch_highest_commit_sha1       = commit_sha1
            else:
                branch_highest_changelist_number = 1
                branch_highest_commit_sha1       = "@1"  # overload this sha1 to refer to @1
            self.branch_start_list[v.branch_id] = (
                "@{0}".format(branch_highest_changelist_number), branch_highest_commit_sha1)

        if LOG.isEnabledFor(logging.DEBUG2):
            l = ('{} {}'.format( p4gf_util.abbrev(branch_id)
                               , self.branch_start_list[branch_id])
                for branch_id in sorted(self.branch_start_list.keys()) )
            LOG.debug2('_get_branch_start_list() results:\n' + '\n'.join(l))

    def add_to_branch_start_list(self, branch_id, start_cl, start_sha1=None):
        """Add a branch to our list of copy branch start points."""

        _start_sha1 = start_sha1
        if not _start_sha1:
            _start_sha1 = "@1"
        self.branch_start_list[branch_id] = (
            "@{0}".format(start_cl), _start_sha1)

    def _any_changes_since_last_copy(self):
        '''
        Return True if there is at least one new changelist since the
        last time we copied between Git and Perforce.

        Git Fusion should do almost nothing, and certainly no supra-constant
        commands or loops, before first checking to see if there's anything
        worth doing.

        Careful: someone might have deleted the Git repo out from under us
        without clearing the counter. Check that too before NOPping.
        '''
        last_copied_change = self.ctx.read_last_copied_change()
        if not last_copied_change:
            return False

        with self.ctx.switched_to_union():
            at = NTR('{},#head').format(1 + int(last_copied_change))
            r = self.ctx.p4run(['changes', '-m1', self.ctx.client_view_path(at)])
            if r:
                return True
        return False

    def _setup(self, _start_at, stop_at):
        """Set RevRange rev_range, figure out which changelists to copy.
        Leaves client switched to union of known branch views.
        """
        # determine the highest commits for each branch
        # placed into self.branch_start_list
        # If a graft - start loading the newly discovered DBI from the graft point
        # Otherwise it loads from @1
        if  _start_at and _start_at.startswith("@") and  _start_at != "@1":
            self.new_branch_start = _start_at[1:]
            self.is_graft = True
        self.stop_at = stop_at
        self._get_branch_start_list(_start_at)

        def append(change):
            '''append a change to the list'''
            self.changes.update(change)

        for b in self.ctx.undeleted_branches():
            if _start_at:   # possible graft start or init from @1
                start_at = _start_at
            else:
                # passing in the sha1 - not the CLnum
                start_at = self.branch_start_list[b.branch_id][1]
            self.ctx.switch_client_view_to_branch(b)
            self.rev_range = RevRange.from_start_stop(self.ctx, start_at, stop_at)

            # get list of changes to import into git
            num = P4Changelist.get_changelists(self.ctx.p4, self._path_range(), append)

            LOG.debug2("_setup() branch={} range={} change_ct={}"
                      .format( p4gf_util.abbrev(b.branch_id)
                             , self.rev_range
                             , num ))

            if not num:
                del self.branch_start_list[b.branch_id]

        LOG.debug3('_setup() branch_start_list: {}'
                  .format(p4gf_util.debug_list(LOG, self.branch_start_list)))


        # If grafting, get those too.
        # We need to collect the potential graft CL once.

        if  self.is_graft:
            self.get_branch_id_to_graft_num(self.ctx, _start_at)

        self.ctx.switch_client_view_to_union()

        LOG.debug3('_setup() changes {0}'
                  .format(p4gf_util.debug_list(LOG, self.changes.keys())))


    def _path_range(self):
        """Return the common path...@range string we use frequently.
        """
        if self.current_branch_id:
            return self._branch_range(self.branch_start_list[self.current_branch_id][0])
        else:
            return self.ctx.client_view_path() + self.rev_range.as_range_string()

    def _branch_range(self, change):
        """For branch return '//<client>/...@N' where N is the highest branch
        changelist number.
        """
        _range = NTR('{begin},{end}').format(begin=change,
                                             end=self.stop_at)
        return self.ctx.client_view_path() + _range

    def _copy_print_view_element(self, printhandler, args, view_element):
        """p4 print all the revs for the given view, git-hash-object them into
        the git repo, add their depotFile, rev, and P4File info to our shared
        RevList.

        view_element is always a Branch.

        Returns a set of change numbers included in output of this print.
        """
        self.ctx.switch_client_view_to_branch(view_element)
        self.current_branch_id = view_element.branch_id

        LOG.debug('_copy_print_view_element() printing for element={}'
                  .format(view_element.to_log()))

        printhandler.change_set = set()
        with p4gf_util.RawEncoding(self.ctx.p4):
            with p4gf_util.Handler(self.ctx.p4, printhandler):
                with self.ctx.p4.at_exception_level(P4.RAISE_ALL):
                    self.ctx.p4run(["print", args, self._path_range()])
        printhandler.flush()
        return printhandler.change_set

    def _to_depot_branch_set(self, change_set):
        """Return a list of DepotBranchInfo objects describing storage
        locations that house change_set's p4files' changes.

        Runs 'p4 filelog' to find integration sources to any changelists.

        Can be overly aggressive and return integ sources to files not changed
        by changes in change_set due to view exclusions.
        """
        dbis = set()
        for change_num in change_set:
            if change_num in self._branch_info_cache:
                LOG.debug3("_branch_info_cache hit on {}".format(change_num))
                dbil = self._branch_info_cache[change_num]
            else:
                LOG.debug3("_branch_info_cache miss on {}".format(change_num))
                (dfl, _rl) = self.filelog_to_integ_source_list(change_num)
                dbil = self.ctx.depot_branch_info_index()\
                                    .depot_file_list_to_depot_branch_list(dfl)
                self._branch_info_cache[change_num] = dbil
            dbis.update(dbil)
        return dbis

    def to_depot_branch_list_mc(self, change_num, filelog_results):
        '''
        Iterate through the results of a 'p4 filelog' and return the
        DepotBranchInfo instances that describe all integration sources
        in the filelog results.
        '''
        (dfl, _rl, _ct) = self.calc_filelog_to_integ_source_list_mc(
                                                    change_num, filelog_results)
        dbil = self.ctx.depot_branch_info_index()\
                                    .depot_file_list_to_depot_branch_list(dfl)
        return dbil

    def _to_branch_view_list(self, depot_branch_set):
        """Return a list of new Branch view instances that map the given
        depot branches into this repo.
        """
        result = []
        for dbi in depot_branch_set:
            l = p4gf_branch.define_branch_views_for(self.ctx, dbi)
            result.extend(l)
        return result

    def _copy_print(self):
        """p4 print all revs and git-hash-object them into the git repo.

        On exit, leaves client view to undefined view: switched to either
        union of all branches, or if grafting, one random branch.
        """
        printhandler = PrintHandler(ctx=self.ctx)
        #if not self.ctx.p4.server_unicode:
        #    old_encoding = self.ctx.p4.encoding
        #    self.ctx.p4.encoding = "raw"
        args = [ '-a'   # all revisions within the specified range
               , '-k'   # suppresses keyword expansion
               ]

        # The union client view is a view into all of these depot branches.
        # We do not want or need to generate new branch views into any of these.
        seen_depot_branch_set = { b.get_or_find_depot_branch(self.ctx)
                                  for b in self.ctx.branch_dict().values() }

        work_queue = []     # list of elements, element is itself
                                    # a list or a branch.
                                    #
        # start with the list of defined repo branches
        for bid, br in self.ctx.branch_dict().items():
            if bid in self.branch_start_list:
                work_queue.append(br)

        with ProgressReporter.Indeterminate():
            while work_queue:
                view_element = work_queue.pop(0)
                new_change_set = self._copy_print_view_element(printhandler
                                                               , args
                                                               , view_element)
                # get set of branches not previously seen
                dbi_set = self._to_depot_branch_set(new_change_set)
                dbi_new = dbi_set - seen_depot_branch_set
                seen_depot_branch_set |= set(dbi_new)

                new_branch_view_list = self._to_branch_view_list(dbi_new)
                LOG.debug('_copy_print() new_branch_view_list={}'
                          .format(new_branch_view_list))
                # add these new branches to our dictionary of start CL
                # These are loaded from @1 to capture all of history
                for b in new_branch_view_list:
                    self.add_to_branch_start_list(b.branch_id, self.new_branch_start)
                work_queue.extend(new_branch_view_list)
                p4gf_gc.report_growth(NTR('in P2G._copy_print() work queue'))

            p4gf_gc.report_objects(NTR('after P2G._copy_print() work queue'))

            # If also grafting, print all revs in existence at time of graft.
            if self.branch_id_to_graft_change:
                for branch_id, change in self.branch_id_to_graft_change.items():
                    branch = self.ctx.branch_dict().get(branch_id)
                    self.ctx.switch_client_view_to_branch(branch)
                    args = ['-k'] # suppresses keyword expansion
                    path = self._graft_path(change)
                    LOG.debug("Printing for grafted history: {}".format(path))
                    with p4gf_util.RawEncoding(self.ctx.p4):
                        with p4gf_util.Handler(self.ctx.p4, printhandler):
                            self.ctx.p4run(["print", args, path])
                    printhandler.flush()
                    p4gf_gc.report_growth(NTR('in P2G._copy_print() graft change'))

            p4gf_gc.report_objects(NTR('after P2G._copy_print() graft change'))

        self.printed_revs = printhandler.revs
        if self.printed_revs:
            self.printed_rev_count += len(self.printed_revs)

    def _get_sorted_changes(self):
        '''return sorted list of changes to be copied to git'''
        if self.branch_id_to_graft_change:
            self.graft_changes = set([int(change.change)
                                      for change
                                      in self.branch_id_to_graft_change.values()])
            sorted_changes = sorted(list(self.changes.keys()) + list(self.graft_changes))
        else:
            sorted_changes = sorted(list(self.changes.keys()))

        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('_get_sorted_changes() returning:\n'
                       + "\n".join([str(ch) for ch in sorted_changes]))
        return sorted_changes

    def _fast_import_from_p4( self
                            , change, branch
                            , mark_to_branch_id
                            , branch_id_to_temp_name ):
        '''
        Translate a Perforce changelist into a Git commit,
        files into ls-tree and blob objects, all via git-fast-import.

        Called for Perforce changelists that originated in Perforce, or
        originated in Git but not for this repo+branch_id.
        '''
        # First commit ever on this branch?
        is_first_commit_on_branch = self.is_branch_empty(branch.branch_id)

        (  parent_commit_list
        , first_parent_branch_id
        , parent_branch_to_cl ) = self._parent_commit_list(
                                              change
                                            , branch
                                            , is_first_commit_on_branch)

        mark_number = self.mark_list.assign(change.change)
        desc_info = DescInfo.from_text(change.description)
        if desc_info and desc_info.sha1:
            self.sha1_to_mark[desc_info.sha1] = mark_number
        LOG.debug('_fast_import_from_p4 change={} mark={} branch={}'
                  .format(change, mark_number, branch.to_log()))
        LOG.debug2('_fast_import_from_p4 parent_commit_list={}'
                   .format(p4gf_util.abbrev(parent_commit_list)))
        mark_to_branch_id[mark_number] = branch.branch_id
        # Branching from an existing commit?
        if      is_first_commit_on_branch \
            and first_parent_branch_id    \
            and parent_commit_list:
            # If so, then we must not blindly accept parent commit's
            # list of work tree files as a starting point. Change from
            # one branch view to another becomes changes to work tree
            # file existence or path.
            LOG.debug("_fast_import_from_p4 new branch: first_parent_branch_id={}"
                      .format(first_parent_branch_id))
            LOG.debug3("_fast_import_from_p4 new branch: parent_branch_to_cl={}"
                      .format(parent_branch_to_cl))

            first_branch_from_branch_id     = first_parent_branch_id
            first_branch_from_change_number = \
                        parent_branch_to_cl.get(first_parent_branch_id)
        else:
            first_branch_from_branch_id     = None
            first_branch_from_change_number = None

        # create commit and trees
        self.fastimport.add_commit(change
                             , mark_number
                             , parent_commit_list
                             , first_branch_from_branch_id
                             , first_branch_from_change_number
                             , branch
                             , branch_id_to_temp_name[branch.branch_id])

        # Move this branch's head to this new commit.
        self._record_branch_head( branch_id  = branch.branch_id
                                , mark       = mark_number
                                , change_num = change.change )

                        # pylint:disable=R0913
                        # Too many arguments
                        # Tough.
    def _fast_import_from_p4_mc(
                              self
                            , change
                            , branch
                            , filelog_results
                            , mark_to_branch_id
                            , branch_id_to_temp_name ):
        '''
        Translate a Perforce changelist into a Git commit,
        files into ls-tree and blob objects, all via git-fast-import.

        Called for Perforce changelists that originated in Perforce, or
        originated in Git but not for this repo+branch_id.
        '''
        # First commit ever on this branch?
        is_first_commit_on_branch = self.is_branch_empty(branch.branch_id)

        (  parent_commit_list
        , first_parent_branch_id
        , parent_branch_to_cl ) = self._parent_commit_list_mc(
                          change                    = change
                        , current_branch            = branch
                        , filelog_results           = filelog_results
                        , is_first_commit_on_branch = is_first_commit_on_branch)

        mark_number = self.mark_list.assign(change.change)
        desc_info = DescInfo.from_text(change.description)
        if desc_info and desc_info.sha1:
            self.sha1_to_mark[desc_info.sha1] = mark_number
        LOG.debug('_fast_import_from_p4 change={} mark={} branch={}'
                  .format(change, mark_number, branch.to_log()))
        LOG.debug2('_fast_import_from_p4 parent_commit_list={}'
                   .format(p4gf_util.abbrev(parent_commit_list)))
        mark_to_branch_id[mark_number] = branch.branch_id
        # Branching from an existing commit?
        if      is_first_commit_on_branch \
            and first_parent_branch_id    \
            and parent_commit_list:
            # If so, then we must not blindly accept parent commit's
            # list of work tree files as a starting point. Change from
            # one branch view to another becomes changes to work tree
            # file existence or path.
            LOG.debug("_fast_import_from_p4 new branch: first_parent_branch_id={}"
                      .format(first_parent_branch_id))
            LOG.debug3("_fast_import_from_p4 new branch: parent_branch_to_cl={}"
                      .format(parent_branch_to_cl))

            first_branch_from_branch_id     = first_parent_branch_id
            first_branch_from_change_number = \
                        parent_branch_to_cl.get(first_parent_branch_id)
        else:
            first_branch_from_branch_id     = None
            first_branch_from_change_number = None

        # create commit and trees
        self.fastimport.add_commit(change
                             , mark_number
                             , parent_commit_list
                             , first_branch_from_branch_id
                             , first_branch_from_change_number
                             , branch
                             , branch_id_to_temp_name[branch.branch_id])

        # Move this branch's head to this new commit.
        self._record_branch_head( branch_id  = branch.branch_id
                                , mark       = mark_number
                                , change_num = change.change )
                        # pylint:enable=R0913

    def _sha1_exists(self, sha1):
        '''
        Is there ANY object in the Git repo with the given sha1?
        Could be a commit, tree, blob, whatever.
        '''
        with Timer(FI_SHA1_EXISTS):
            exists = sha1 in self._sha1s_known_to_exist
            if not exists:
                exists = p4gf_util.sha1_exists(sha1)
                if exists:
                    self._sha1s_known_to_exist.add(sha1)
            return exists

    def _print_to_git_store(self, sha1, p4_path):
        '''
        p4 print the object directly into the //P4GF_DEPOT/objects/... store.
        '''
        with Timer(FI_PRINT):
            # Fetch Git object from Perforce, write directly to Git.
            git_path = self.ctx.view_dirs.GIT_DIR + '/' \
                     + p4gf_util.sha1_to_git_objects_path(sha1)
            p4gf_util.ensure_parent_dir(git_path)
            with p4gf_util.RawEncoding(self.ctx.p4gf):
                self.ctx.p4gfrun([ 'print'
                                 , '-o', git_path
                                 , p4_path ])
            LOG.debug3('_print_to_git_store() {} {}'.format(sha1, git_path))
            self._sha1s_known_to_exist.add(sha1)
            return git_path

    def _print_tree_to_git_store(self, sha1):
        '''
        p4 print the tree object directly into the //P4GF_DEPOT/objects/... store.
        Return path to file we printed.
        '''
        p4_path = ObjectType.tree_p4_path(sha1)
        return self._print_to_git_store(sha1, p4_path)

    def _print_commit_to_git_store(self, commit):
        '''
        p4 print the commit object directly into the //P4GF_DEPOT/objects/... store.
        Return path to file we printed.
        '''
        p4_path = ObjectType.commit_p4_path(self.ctx, commit)
        return self._print_to_git_store(commit.sha1, p4_path)

    def _commit_to_tree_sha1(self, commit_sha1):
        '''
        Return the tree sha1 for a commit.
        '''
        with Timer(FI_TO_TREE):
            obj = self.ctx.view_repo.get(commit_sha1)
            LOG.debug2('_commit_to_tree_sha1({}) => {}'.format(commit_sha1, obj))
            if obj and obj.type == pygit2.GIT_OBJ_COMMIT:
                return p4gf_git.tree_from_commit(obj.read_raw())
            return None

    def _fast_import_from_gitmirror(self, change, branch):
        '''
        Copy a Git commit object and its Git ls-tree objects directly from
        where they're archived in //P4GF_DEPOT/objects/... into Git.

        Copy file revisions into Git using git-hash-object.

        Return True if able to copy directly from //P4GF_DEPOT/objects/... .
        Return False if not.
        '''
        with Timer(FI_GITMIRROR):
            # Do we already have a commit object for this changelist, this branch?
            commit_ot = ObjectType.commit_for_change(self.ctx,
                                                     change.change,
                                                     branch.branch_id)
            if not commit_ot:
                LOG.debug2('_fast_import_from_gitmirror() '
                           '        no commit in Perforce for ch={} branch={}. Returning False.'
                           .format(change.change, p4gf_util.abbrev(branch.branch_id)))
                return False
            if self._sha1_exists(commit_ot.sha1):
                # Already copied. No need to do more.
                LOG.debug2('_fast_import_from_gitmirror() {} commit already done, skipping'
                           .format(p4gf_util.abbrev(commit_ot.sha1)))
                self._record_branch_head( branch_id  = branch.branch_id
                                        , sha1       = commit_ot.sha1
                                        , change_num = commit_ot.details.changelist )
                return True

            # Copy commit from Peforce directly to Git.
            commit_git_path = self._print_commit_to_git_store(commit_ot)

            # Every file we add is suspect unless we complete without error.
            file_deleter    = p4gf_util.FileDeleter()
            file_deleter.file_list.append(commit_git_path)

            # Copy commit's tree and subtrees to Git.
            # blobs should have already been copied during _copy_print()
            tree_sha1  = self._commit_to_tree_sha1(commit_ot.sha1)
            if not tree_sha1:
                LOG.debug2('_fast_import_from_gitmirror()'
                           ' {} commit missing tree. Returning False.'
                          .format(p4gf_util.abbrev(commit_ot.sha1)))
                return False

            tree_queue = [tree_sha1]
            while tree_queue:
                tree_sha1 = tree_queue.pop(0)

                # If already in Git, no need to copy it again.
                if self._sha1_exists(tree_sha1):
                    LOG.debug2('_fast_import_from_gitmirror()'
                               ' {} tree   already done, skipping.'
                              .format(p4gf_util.abbrev(tree_sha1)))
                    continue

                # Copy tree from Perforce directly to Git.
                intree =  ObjectType.tree_exists_in_p4(self.ctx.p4gf, tree_sha1)
                if not intree:
                    LOG.debug2('_fast_import_from_gitmirror()'
                               ' {} tree   missing in mirror. Returning False'
                              .format(p4gf_util.abbrev(tree_sha1)))
                    return False
                tree_git_path = self._print_tree_to_git_store(tree_sha1)
                file_deleter.file_list.append(tree_git_path)
                LOG.debug2('_fast_import_from_gitmirror() {} tree   copied from mirror.'
                           .format(p4gf_util.abbrev(tree_sha1)))

                # Copy file children, enqueue tree children for future copy.
                for (i_mode, i_type, i_sha1, _i_path) in p4gf_util.git_ls_tree(self.ctx.view_repo,
                                                                                tree_sha1):
                    if 'tree' == i_type:
                        tree_queue.append(i_sha1)
                        continue
                    if '160000' == i_mode:
                        # Submodule/gitlink, nothing to do here
                        continue

                    e = self._sha1_exists(i_sha1)
                    if not e:
                        LOG.debug2('_fast_import_from_gitmirror() {}'
                                   ' untree missing. Returning False'
                                  .format(p4gf_util.abbrev(i_sha1)))
                        return False

            # Move this branch's head to this new commit.
            self._record_branch_head( branch_id  = branch.branch_id
                                    , sha1       = commit_ot.sha1
                                    , change_num = commit_ot.details.changelist )
            # Made it to here without error? Keep all we wrought.
            file_deleter.file_list = []
            LOG.debug2('_fast_import_from_gitmirror() {} commit copied from mirror. Returning True.'
                       .format(p4gf_util.abbrev(commit_ot.sha1)))
            return True

    def _get_changelist(self, changenum):
        """Get changelist object for change number, with no files.
        """
        # All we have is the change number and list of revs.
        # Can't use p4 change -o because that gives formatted time and we want raw.
        # Could use p4 describe, but that sends the potentially large list of files.
        # So use p4 changes, filtered by the first rev in the change, with limit of 1.
        cl = P4Changelist.create_changelist_list_as_dict(self.ctx.p4,
                                                         "@{},@{}".format(changenum, changenum),
                                                         1)[changenum]
        return cl

    def _get_changelist_for_branch(self, changenum, branch):
        """Get changelist object for change number.
        If change is a graft point, use branch to create fake changelist object
        containing required files.
        """
        change = self.changes.get(changenum)
        if branch:
            change.files = self.printed_revs.files_for_graft_change(changenum, branch)
        else:
            change.files = self.printed_revs.files_for_change(changenum)
        return change

    def _is_change_num_in_branch_range(self, change_num, branch_id):
        '''
        Copy to Git only those changelists that we've not already copied.

        Copy to Git no changelists if we've decided not to
        copy anything to that branch.
        '''
        t = self.branch_start_list.get(branch_id)
        if not t:           # _setup() stripped out this branch_id because we
                            # don't have any new work for this branch. Don't
            return False    # touch this branch at all.

        start_str = t[0]
        if start_str.startswith('@'):
            start_str = start_str[1:]
        return int(start_str) <= int(change_num)

    def _create_branch_id_to_temp_name_dict(self):
        '''
        Give every single Branch view its own unique Git branch name that we can
        use during git-fast-import without touching any real/existing Git branch
        references.

        Return a new dict of branch ID to a temporary Git branch name
        "git-fusion-temp-branch-{}", with either the Git branch name
        (if branch has one) or the branch ID stuffed into the {}.
        '''
        return {branch.branch_id : _to_temp_branch_name(branch)
                for branch in self.ctx.branch_dict().values()}

                        # pylint:disable=R0912
                        # Too many branches
                        # Yep, needs some cleanup
    def _fast_import(self, sorted_changes):
        """Build fast-import script from changes, then run fast-import.
        Assumes a single linear sequence from a single branch:
        * Current Perforce client must be switched to the view for that branch.
        * No merge commits.

        Returns (marks, mark_to_branch_id dict)
        """
        LOG.debug('_fast_import()')
        branch_dict = self.ctx.branch_dict()
        branch_id_to_temp_name = self._create_branch_id_to_temp_name_dict()
        current_branch = None
        LOG.debug3("_fast_import branch_dict={}".format(branch_dict.values()))
        self._fill_head_marks_from_current_heads()
        mark_to_branch_id = {}
        with ProgressReporter.Determinate(len(sorted_changes)):
            for changenum in sorted_changes:
                ProgressReporter.increment(_('Copying changelists...'))

                        # Never copy ghost changelists to Git.
                if _is_ghost_desc(self.changes.get(changenum).description):
                    self._skip_ghost(self.changes.get(changenum))
                    continue

                is_graft = bool(self.graft_changes) and  changenum in self.graft_changes

                # regular non-graft changes:
                if not is_graft:
                    # branch doesn't matter for non-graft changes
                    change = self._get_changelist_for_branch(changenum, None)
                    LOG.info('Copying {}'.format(change))

                    branch_list = [branch for branch in self.ctx.undeleted_branches()
                                   if branch.intersects_p4changelist(change)]
                    for branch in branch_list:
                        self.ctx.heartbeat()

                        if not self._is_change_num_in_branch_range(
                                                   changenum, branch.branch_id):
                            continue

                        if branch != current_branch:
                            self.ctx.switch_client_view_to_branch(branch)
                            current_branch = branch

                        if not self._fast_import_from_gitmirror(change, branch):
                            self._fast_import_from_p4(change
                                                     , branch
                                                     , mark_to_branch_id
                                                     , branch_id_to_temp_name )
                    continue

                # special case for graft changes:

                for branch in self.ctx.undeleted_branches():
                    # check if this is branch uses this graft change
                    if not branch.branch_id in self.branch_id_to_graft_change:
                        continue
                    gchange = self.branch_id_to_graft_change[branch.branch_id]
                    if not gchange.change == changenum:
                        continue

                    self.ctx.heartbeat()

                    if branch != current_branch:
                        self.ctx.switch_client_view_to_branch(branch)
                        current_branch = branch

                    change = self._get_changelist_for_branch(changenum, branch)
                    LOG.info('Copying {}'.format(change))

                    change.description = gchange.description
                    if not self._fast_import_from_gitmirror(change, branch):
                        self._fast_import_from_p4(change
                                                 , branch
                                                 , mark_to_branch_id
                                                 , branch_id_to_temp_name )
                p4gf_gc.process_garbage(NTR('in P2G._fast_import()'))

        # run git-fast-import and get list of marks
        LOG.info('Running git-fast-import')
        marks = self.fastimport.run_fast_import()

        # done with these
        for name in branch_id_to_temp_name.values():
            ### unable to use pygit2 to delete reference, no idea why
            p4gf_proc.popen_no_throw(['git', 'branch', '-D', name])

        # Record how much we've copied.
        self.ctx.write_last_copied_change(sorted_changes[-1])

        self.changes = None
        self._filelog_cache = None
        return (marks, mark_to_branch_id)
                        # pylint:enable=R0912

    def _mirror(self, marks, mark_to_branch_id):
        """build up list of p4 objects to mirror git repo in perforce
        then submit them
        """
        LOG.info('Copying Git and Git Fusion data to //{}/...'
                 .format(p4gf_const.P4GF_DEPOT))
        self.ctx.mirror.add_depot_branch_infos(self.ctx)
        self.ctx.mirror.add_branch_config2(self.ctx)
        self.ctx.mirror.add_objects_to_p4(marks, self.mark_list, mark_to_branch_id, self.ctx)
        LOG.getChild("time").debug("\n\nGit Mirror:\n" + str(self.ctx.mirror))
        # Reset to a new, clean, mirror.
        self.ctx.mirror = GitMirror(self.ctx.config.view_name)

        if marks:
            last_commit = marks[len(marks) - 1].strip()
            LOG.debug("Last commit fast-import-ed: " + last_commit)
        else:
            LOG.debug('No commits fast-imported')

    def filelog_to_integ_source_list(self, change_num):
        '''
        From what files does this change integrate?

        Return a 2-tuples of (depotFile_list, erev_list)
        '''
        return self._filelog_cache.get(change_num)

    def _calc_filelog_to_integ_source_list(self, change_num):
        """Run 'p4 filelog' to find and return a list of all integration
        source depotFile paths that contribute to this change.

        Return a 3-tuple of (depotFile_list,
                             erev_list,
                             <size of lists>)
        """
        path = self.changes.get_path(change_num)
        r = self.ctx.p4run(['filelog', '-m1', '-c', str(change_num), path])
        source_depot_file_list = []
        source_erev_list       = []
        sizeof = 0
        for rr in r:
            # Skip files that aren't integrated to/from somewhere.
            if (   (not rr.get('how' ))
                or (not rr.get('file'))
                or (not rr.get('erev')) ):
                continue
            # double-deref+zip how0,0 and file0,0 double-arrays.
            for how_n, file_n, erev_n in zip(rr['how'], rr['file'], rr['erev']):
                for how_n_m, file_n_m, erev_n_m in zip(how_n, file_n, erev_n):
                    if p4gf_filelog_action.is_from(how_n_m):
                        # erev starts with a # sign ("#3"),
                        # and might actually be a rev range ("#2,#3").
                        # Focus on the end of the range, just the number.
                        erev = erev_n_m.split('#')[-1]
                        source_depot_file_list.append(file_n_m)
                        source_erev_list      .append(erev)
                        sizeof += sys.getsizeof(file_n_m) + sys.getsizeof(erev)

        LOG.debug('filelog_to_integ_source_list() ch={} returning ct={}'
                  .format(change_num, len(source_depot_file_list)))
        LOG.debug3('\n'.join(p4gf_util.to_path_rev_list( source_depot_file_list
                                                       , source_erev_list)))
        if not source_depot_file_list:
            LOG.debug3('filelog_to_integ_source_list() ch={}'
                      ' returing 0, filelog gave us:{}'.format(change_num, r))

        sizeof += sys.getsizeof(source_depot_file_list)
        sizeof += sys.getsizeof(source_erev_list)
        return (source_depot_file_list, source_erev_list, sizeof)

    @staticmethod
    def calc_filelog_to_integ_source_list_mc(change_num, filelog_results):
        """Run 'p4 filelog' to find and return a list of all integration
        source depotFile paths that contribute to this change.

        Return a 3-tuple of (depotFile_list,
                             erev_list,
                             <size of lists>)
        """
        source_depot_file_list = []
        source_erev_list       = []
        sizeof = 0
        for rr in filelog_results:
            # Skip files that aren't integrated to/from somewhere.
            if (   (not rr.get('how' ))
                or (not rr.get('file'))
                or (not rr.get('erev')) ):
                continue
            # double-deref+zip how0,0 and file0,0 double-arrays.
            for how_n, file_n, erev_n in zip(rr['how'], rr['file'], rr['erev']):
                for how_n_m, file_n_m, erev_n_m in zip(how_n, file_n, erev_n):
                    if p4gf_filelog_action.is_from(how_n_m):
                        # erev starts with a # sign ("#3"),
                        # and might actually be a rev range ("#2,#3").
                        # Focus on the end of the range, just the number.
                        erev = erev_n_m.split('#')[-1]
                        source_depot_file_list.append(file_n_m)
                        source_erev_list      .append(erev)
                        sizeof += sys.getsizeof(file_n_m) + sys.getsizeof(erev)

        LOG.debug('filelog_to_integ_source_list() ch={} returning ct={}'
                  .format(change_num, len(source_depot_file_list)))
        LOG.debug3('\n'.join(p4gf_util.to_path_rev_list( source_depot_file_list
                                                       , source_erev_list)))
        if not source_depot_file_list:
            LOG.debug3('filelog_to_integ_source_list() ch={}'
                      ' returing 0, filelog gave us:{}'
                      .format( change_num
                             , filelog_results))

        sizeof += sys.getsizeof(source_depot_file_list)
        sizeof += sys.getsizeof(source_erev_list)
        return (source_depot_file_list, source_erev_list, sizeof)

    def _parent_commit_list( self
                           , change
                           , current_branch
                           , is_first_commit_on_branch ):
        """ Given a Perforce changelist, return a list of Git commits that
        should be parents of the Git commit we're about to create for this
        changelist.

        Returns a 3-tuple ( [sha1/mark list]
                          , first parent branch id
                          , {branch->changelist dict} )

        Returned list elements are either sha1s of existing commits (str),
        or git-fast-import marks (int).

        Return None if this changelist has no parent in Git.
        Except for the first commit in a repo, this should be very rare.

        change         : P4Changelist
        current_branch : Branch
        """
        pcl = ParentCommitList( self
                              , change
                              , current_branch
                              , is_first_commit_on_branch )
        pcl.calc()

        return ( pcl.parent_commit_list
               , pcl.first_parent_branch_id
               , pcl.branch_id_to_changelist_num )


    def _parent_commit_list_mc( self
                           , change
                           , current_branch
                           , filelog_results
                           , is_first_commit_on_branch ):
        '''
        Given a Perforce changelist and results from p4 filelog,
        return a list of Git commits that
        should be parents of the Git commit we're about to create for this
        changelist.

        Returns a 3-tuple ( [sha1/mark list]
                          , first parent branch id
                          , {branch->changelist dict} )

        Returned list elements are either sha1s of existing commits (str),
        or git-fast-import marks (int).

        Return None if this changelist has no parent in Git.
        Except for the first commit in a repo, this should be very rare.

        change         : P4Changelist
        current_branch : Branch
        '''
        pcl = ParentCommitList( self
                              , change
                              , current_branch
                              , is_first_commit_on_branch
                              , filelog_results = filelog_results )
        pcl.calc()

        return ( pcl.parent_commit_list
               , pcl.first_parent_branch_id
               , pcl.branch_id_to_changelist_num )


    def _fill_head_marks_from_current_heads(self):
        """Read 'git-show-ref' to find each known branch's current head,
        store the head sha1 to use later as the parent of the next
        commit on that branch.

        Upon return, self._branch_id_to_head has an entry for every
        branch_id in branch_dict() with a value of either an existing commit
        sha1, or None if branch ref not yet defined.
        """
        branch_name_list = [b.git_branch_name
                            for b in self.ctx.undeleted_branches()
                            if b.git_branch_name]
        git_name_to_head_sha1 = p4gf_util.git_ref_list_to_sha1(branch_name_list)
        branch_dict = self.ctx.branch_dict()
        for branch_id, branch in branch_dict.items():
            if branch.git_branch_name in git_name_to_head_sha1:
                sha1 = git_name_to_head_sha1[branch.git_branch_name]
                self._record_branch_head( branch_id  = branch_id
                                        , sha1       = sha1 )

    def is_branch_empty(self, branch_id):
        '''
        Do we have no commits/changelists recorded for this branch?
        '''
        return not self.branch_head_mark_or_sha1(branch_id)

    def branch_head_mark_or_sha1(self, branch_id):
        '''
        Return the mark or sha1 of the most recently recorded commit/changelist
        for this branch.
        '''
        bh = self._branch_id_to_head.get(branch_id)
        if not bh:
            return None
        return _mark_or_sha1(bh)

    def branch_head_to_change_num(self, branch_id):
        '''
        Return integer changelist number of most recent changelist copied to the given branch.

        Return None if branch is empty, no changelists copied ever, including previous pulls/pushes.

        Return 0 if branch contains copied changelists/commits, but we stupidly
        forgot to record their changelist number when building
        self._branch_id_to_head. (That's a bug and we need to fix that.)
        '''
        bh = self._branch_id_to_head.get(branch_id)
        if not bh:
            return None
        return int(bh.change_num)

    def _record_branch_head(self, branch_id
                           , mark       = None
                           , sha1       = None
                           , change_num = 0):
        '''
        Remember that this is the most recent changelist/commit seen on a branch.

        Record its mark_or_sha1 (mark from MarkList if we copied it, sha1 if
        seen from Git).

        Also record corresponding changelist number.
        '''
        self._branch_id_to_head[branch_id] \
            = BranchHead( mark       = mark
                        , sha1       = sha1
                        , change_num = int(change_num))

    def _branch_head_marks(self):
        '''
        Iterator/generator yields marks/sha1s, one per branch that has either.
        '''
        for bh in self._branch_id_to_head.values():
            if bh and bh.mark:
                yield bh.mark

    def _files_in_change_num(self, change_num):
        '''
        Run 'p4 files @=change_num' and return a list of depotFile results.
        '''
        r = self.ctx.p4run(['files', '@={}'.format(change_num)])
        for rr in r:
            if not isinstance(rr, dict):
                continue
            df = rr.get('depotFile')
            if df:
                yield df

    def _skip_ghost(self, ghost_p4change):
        '''
        We're not going to copy ghost_p4change to Git.

        Remember this ghost_p4change's changelist number so that  later when
        processing normal changelists, we can tell if the most recent changelist
        on this branch was a ghost that might warrant closer inspection during
        ParentCommitList calculation.
        '''
        change_num = ghost_p4change.change
        LOG.debug2('skipping ghost @{}'.format(change_num))
        for branch_id, branch in self.ctx.branch_dict().items():

                        # Do not call Branch.intersects_p4changelist() here. It
                        # requires P4Changelist.files to be populated. P2G no
                        # longer populates P4Changelist.files because that costs
                        # too much memory.
            depot_file_list = self._files_in_change_num(change_num)
            if branch.intersects_depot_file_list(depot_file_list):
                LOG.debug2('skipping ghost @{} on branch {}'
                        .format(change_num, p4gf_util.abbrev(branch_id)))
                di = DescInfo.from_text(ghost_p4change.description)
                ofcn = di.ghost_of_change_num               \
                                    if (di and di.ghost_of_change_num) else 0
                self._branch_id_to_skipped_ghost[branch_id] \
                    = SkippedGhost( change_num    = int(change_num)
                                  , of_change_num = int(ofcn) )

    def ghost_for_branch_id(self, branch_id):
        '''
        If the most recent changelist we've seen on this branch was a ghost,
        return a SkippedGhost tuple with the ghost and ghost-of changelist
        numbers as integers.
        If not, return None.
        '''
                        # Get most recent ghost on branch.
        skipped_ghost    = self._branch_id_to_skipped_ghost.get(branch_id)
        if not (skipped_ghost and skipped_ghost.change_num):
            return None
        ghost_change_num = skipped_ghost.change_num

                        # Anything on the branch newer than the ghost?
        bh = self._branch_id_to_head.get(branch_id)
        if bh and bh.change_num and ghost_change_num < bh.change_num:
            LOG.debug3("ghost_for_branch_id() branch {br} ghost {gh} < head {h}"
                       .format( br = p4gf_util.abbrev(branch_id)
                              , gh = ghost_change_num
                              , h  = bh.change_num ))
            return None

        LOG.debug2("ghost_for_branch_id() branch {br} ghost {gh} of {of}"
                   .format( br = p4gf_util.abbrev(branch_id)
                          , gh = ghost_change_num
                          , of = skipped_ghost.of_change_num ))
        return skipped_ghost

    @staticmethod
    def _pack():
        """run 'git gc' to pack up the blobs

        aside from any possible performance benefit, this prevents warnings
        from git about "unreachable loose objects"
        """
        pass  # p4gf_proc.popen_no_throw(["git", "gc"])

    def _set_branch_refs(self, mark_lines):
        """Force each touched, named, branch reference to the head of
        whatever history that we just appended onto that branch.
        """
        # Scan for interesting mark/sha1 lines.
        mark_to_sha1 = {mark : None
                        for mark in self._branch_head_marks() }
        for mark_line in mark_lines:
            ml = Mark.from_line(mark_line)
            if ml.mark in mark_to_sha1:
                mark_to_sha1[ml.mark] = ml.sha1

        # Detach HEAD because we cannot force the current branch to a
        # different commit. This only works if we have a HEAD: empty repos on
        # first clone will reject this command and that's okay.
        p4gf_proc.popen_no_throw(['git', 'checkout', 'HEAD~0'])

        for branch_id, bh in self._branch_id_to_head.items():
            head_mark = bh.mark
            head_sha1 = bh.sha1 if bh.sha1 else mark_to_sha1.get(bh.mark)
            branch    = self.ctx.branch_dict().get(branch_id)
            if not branch.git_branch_name:  # Anon branches have no ref to set.
                continue
            if head_sha1 is None:
                # Branch not moved by git-fast-import. Might still have been
                # moved by direct copy of commits into .git/objects/...
                LOG.warn("_set_branch_refs() found null head for branch {} mark={}"
                    .format(branch.git_branch_name, head_mark))
                continue
            LOG.debug("_set_branch_refs() {} mark={} sha1={}"
                      .format(branch.git_branch_name, head_mark, head_sha1))
            if head_sha1 and branch and branch.git_branch_name:
                p4gf_proc.popen(['git', 'branch'
                                , '-f', branch.git_branch_name
                                , head_sha1])

        # Reattach HEAD to a branch.
        self.ctx.checkout_master_ish()

    def _graft_path(self, graft_change):
        """If grafting, return '//<client>/...@N' where N is the graft
        changelist number.
        """
        return self.ctx.client_view_path(graft_change.change)

    @staticmethod
    def _log_memory(msg):
        '''How big is our heap now?'''
        LOG_MEMORY.debug('Memory after {:<24} {}: '
                        .format(msg, p4gf_log.memory_usage()))

    def desc_info_permits_merge(self, change_num):
        '''
        Does the current Perforce changelist's DescInfo block contain a
        "parents:" tag with 2+ parent commit sha1s?

        If not, then this was originally NOT a merge commit, and we should avoid
        turning it into one upon rerepo.

        If this is an old changelist, created before Git Fusion 2013.3
        introduced DescInfo.parents, then we do not know if the original commit
        was a merge or not, and we're not going to spend time digging through
        old Git repos to find out. Return True.
        '''
        global_config  = p4gf_config.get_global(self.ctx.p4gf)
        val            = global_config.get(
                              p4gf_config.SECTION_GIT_TO_PERFORCE
                            , p4gf_config.KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM
                            , fallback=None)

                        # Don't know? Then give up.
        if not val:
            return True
        try:
            req_change_num = int(val)
        except ValueError:
            req_change_num = 0
        if not req_change_num:
            return True

                        # Current changelist before the cutoff?
        if int(change_num) <= req_change_num:
            return True

                        # No DescInfo block? This changelist originated in
                        # Perforce.
        desc_info = DescInfo.from_text(self.changes.get(change_num).description)
        if not desc_info:
            return True
                        # Current changelist description will contain multiple
                        # parent commit sha1s if this was originally a merge
                        # commit.
        return (    desc_info.parents
                and 2 <= len(desc_info.parents ))

    def copy(self, start_at, stop_at, new_git_branches):
        """copy a set of changelists from perforce into git"""

        self._log_memory(NTR('start'))

        # If the Git repository has already been populated by an
        # earlier pull, and there are tags to update, do so now.
        # (avoiding the early-exit logic below...)
        repo_empty = p4gf_util.git_empty()
        if not repo_empty and p4gf_tag.any_tags_since_last_copy(self.ctx):
            p4gf_tag.update_tags(self.ctx)

        # Stop early if nothing to do.
        if (not repo_empty) and (not self._any_changes_since_last_copy()) \
                and not new_git_branches:
            LOG.debug("No changes since last copy.")
            self.fastimport.cleanup()
            return

        with Timer(OVERALL):
            self.ctx.get_view_repo()
            self._log_memory(NTR('pygit2'))

            with Timer(SETUP):
                self._setup(start_at, stop_at)
                self._log_memory('_setup')

                if      (not len(self.changes.keys())) \
                    and (not self.branch_id_to_graft_num):
                    LOG.debug("No new changes found to copy")
                    return

            with Timer(PRINT):
                LOG.info('Copying file revisions from Perforce')
                self._copy_print()
                self._log_memory('_copy_print')

            sorted_changes = self._get_sorted_changes()
            self._log_memory('_get_sorted_changes')

            with Timer(FAST_IMPORT):
                (mark_lines, mark_to_branch_id) = self._fast_import(sorted_changes)
                self._log_memory('_fast_import')

            if repo_empty:
                # If we are just now rebuilding the Git repository, also
                # grab all of the tags that have been pushed in the past.
                p4gf_tag.generate_tags(self.ctx)
                self._log_memory('_generate_tags')

            with Timer(MIRROR):
                self._mirror(mark_lines, mark_to_branch_id)
                self._log_memory('_mirror')

            with Timer(BRANCH_REF):
                self._set_branch_refs(mark_lines=mark_lines)
                self._log_memory('_set_branch_refs')

            with Timer(PACK):
                self._pack()
                self._log_memory('_pack')

        LOG.getChild("time").debug("\n" + str(self))
        LOG.info('Done. Changelists: {}  File Revisions: {}  Seconds: {}'
                 .format( len(sorted_changes)
                        , self.printed_rev_count
                        , int(Timer(OVERALL).time)))
        p4gf_gc.report_objects(NTR('after P2G.copy()'))
        self._log_memory(NTR('copy() done'))


# -- module-wide --------------------------------------------------------------

# timer/counter names
OVERALL     = NTR('P4 to Git Overall')
SETUP       = NTR('Setup')
CHANGES     = NTR('p4 changes')
CHANGES1    = NTR('p4 changes -m1 -l')
FILELOG     = NTR('p4 filelog')
PRINT       = NTR('Print')
PRINT2      = NTR('print mc')
CALC_PRINT  = NTR('calc print')
FAST_IMPORT = NTR('Fast Import')
MIRROR      = NTR('Mirror')
BRANCH_REF  = NTR('Branch Ref')
PACK        = NTR('Pack')

FI_GITMIRROR   = NTR('FI from Git Mirror')
FI_SHA1_EXISTS = NTR('_sha1_exists')
FI_PRINT       = NTR('_print_to_git_store')
FI_TO_TREE     = NTR('_commit_to_tree_sha1')

                        # Most recently seen commit/changelist for a single
                        # branch. Usually only 1 of mark/sha1 filled in:
BranchHead = namedtuple('BranchHead', [
          'mark'        # of commit being copied as part of this fast-import
                        # Generated by MarkList.
        , 'sha1'        # of existing commit already part of Git
        , 'change_num'  # int changelist number of above commit
        ])

                        # Most recently skipped ghost changelist for a
                        # single branch. Values for P2G.branch_id_to_skipped_ghost
SkippedGhost = namedtuple('SkippedGhost', [
          'change_num'      # int changelist number of ghost changelist.
        , 'of_change_num'   # int changelist number of the changelist of
                            # which this ghost is a copy.
        ])

def _is_ghost_desc(desc):
    '''
    Does this changelist description's tagged info block contain
    tags that appear only for ghost changelists?
    '''
    desc_info = DescInfo.from_text(desc)
    return desc_info and desc_info.ghost_of_sha1 != None


def _to_temp_branch_name(branch):
    '''
    Return git-fusion-temp-branch-foo.
    '''
    if branch.git_branch_name:
        return p4gf_const.P4GF_BRANCH_TEMP_N.format(branch.git_branch_name)
    return p4gf_const.P4GF_BRANCH_TEMP_N.format(branch.branch_id)


def copy_p4_changes_to_git(ctx, start_at, stop_at, new_git_branches):
    """copy a set of changelists from perforce into git"""

    p2g = P2G(ctx)

    if MEM_CAPPED:
        p2g_mc = P2GMemcapped(p2g=p2g)
        p2g_mc.copy(start_at, stop_at, new_git_branches)
    else:
        p2g.copy(start_at, stop_at, new_git_branches)


def _mark_or_sha1(branch_head):
    '''
    For code that read the old commingled list of marks + sha1s,
    re-commingle the decommingled BranchHead tuple.
    '''
    return branch_head.mark if branch_head.mark else branch_head.sha1
