#! /usr/bin/env python3.3
'''
Decide what Perforce actions to take to copy a single Git commit to Perforce.
'''
from   collections                  import namedtuple, defaultdict
import logging
import os
import pprint
import P4

import p4gf_const
from   p4gf_g2p_matrix_column   import G2PMatrixColumn as Column
from   p4gf_g2p_matrix_dump     import dump
import p4gf_g2p_matrix2_common                         as common
from   p4gf_g2p_matrix2_cell    import G2PMatrixCell   as Cell
from   p4gf_g2p_matrix2_decided import Decided
from   p4gf_g2p_matrix2_row     import G2PMatrixRow    as Row
from   p4gf_g2p_matrix2_row_decider import RowDecider
import p4gf_git
import p4gf_integrated_up_to
from   p4gf_l10n               import _, NTR
import p4gf_log
import p4gf_p4filetype
from   p4gf_path                    import force_trailing_delimiter
import p4gf_proc
from   p4gf_profiler                import Timer
import p4gf_util

LOG = logging.getLogger(__name__)
LOG_DUMP = LOG.getChild('dump')

                        # pylint:disable=W9903
                        # non-gettext-ed string
                        # These are all debug timer names. No L10N required.
# Timers
COPY                = 'Copy'    # Must be same as p4gf_copy_to_p4,
                                # but without circular import.
DISCOVER                = 'discover'
SYNC_K_0                    = 'p4 sync -k @0'
DISCOVER_RM_RF              = 'rm -rf (discover)'
DISCOVER_BRANCHES           = 'discover branches'
DISCOVER_FILES              = 'discover files'
DISCOVER_P4_FILES_GDEST         = 'GDEST p4 files'
DISCOVER_GIT_LS_TREE_GDEST      = 'GDEST git-ls-tree'
DISCOVER_POPULATE               = 'populate first changelist'
DISCOVER_P4IMPLY                = 'P4IMPLY'
DISCOVER_GPARN_INTEG_N          = 'GPARN integ -n'
DISCOVER_GPARN_FILES            = 'GPARN files'
DISCOVER_JIT                    = 'JIT'
DISCOVER_SYMLINKS               = 'symlinks'
DECIDE                  = 'decide'
DO_IT                   = 'do_it'
SYNC_K                        = 'p4 sync -k'
SYNC_F                        = 'p4 sync -f'
DO_IT_RM_RF                   = 'rm -rf (do_it)'
DO_INTEG                      = 'do integs'
DO_INTEG_INTEG                    = 'p4 integ'
DO_INTEG_RESOLVE                  = 'p4 resolve'
DO_COPY_FILES_FROM_GIT      = 'copy files from git'
DO_BATCH_ADD_EDIT_DELETE    = 'batch add/edit/delete'

# # timer/counter names from p4gf_copy_to_p4, do not use
# OVERALL         = "Git to P4 Overall"
# FAST_EXPORT     = "FastExport"
# GIT_CHECKOUT    = "Git Checkout"
# COPY            = "Copy"
# JIT_BRANCH      = "JIT Branch"
# GIT_STATUS      = "git status"
# P4_OPEN         = "p4 open files"
# P4_SUBMIT       = "p4 submit"
# CHECK_PROTECTS  = "Check Protects"
# MIRROR          = "Mirror Git Objects"
# GIT_LS_TREE     = "git-ls-tree"

                        # pylint:enable=W9903

# What happened to "integ for branch + delete"?
#
# We no longer "'p4 integ -Rb' for branch, then 'p4 resolve -ay' to ignore"
# in an attempt to "integ for branch + delete".
#
# I liked the "integ for branch, then ignore": it created integ links between
# changelists that closely mimicked Git's commit parents.
#
# But integ got branch + delete doesn't work so hot when integrating from
# multiple sources. We risked P4D's spectacular "more than one 'branch' or
# 'delete' integration" error.
#
# Without this "branch for delete" integ, we lose some inter-branch integ
# connections.
# -- 'p4 filelog' results change.
# -- re-repo sometimes drops contributing branches of history, because those
#    branches don't actually CHANGE the contents of the destination branch.

# Why so many unnecessary 'p4 edit' operations? Why does my 'p4 filelog' show
# 'add' instead of 'branch', 'edit' instead of 'integrate'?
#
# Because this script avoids calculating the sha1 of any local file, such as the
# result of a 'p4 integ' + 'p4 resolve'. It just assumes that the result differs
# from what Git wants (what's in the row.sha1)
#
# We _could_ calculate the sha1 of the resulting file, and then if it matches,
# skip the 'p4 edit'. That buys a cleaner Perforce integration history, at the
# price of slower 'git push' times.
#
# 'git push' time is a real cost that Git users pay each time they push.
# Dirtyness in Perforce integration history is a conceptual cost that Perforce
# users might see if they open one of these Git files in P4V's revision graph.
# So we choose 'git push' speed over integration cleanliness.

# pylint:disable=W0212
# Access to a protected member _xxx of a client class
#
# This warning is correct, but I'm disabling it for now so that I can focus on
# more immediate work. We eventually need to refactor p4gf_copy_to_p4's G2P to
# move things either to this class, or to a separate class that both G2P and
# G2PMatrix can share. Eventually we need to drop our backpointer into our
# owning G2P.

class G2PMatrix:
    '''
    Decide what Perforce actions to take to copy one Git commit to Perforce.

    Internally creates a huge matrix of file rows x branch columns.
    Table will usually have many empty cells.

    (There is a second matrix in p4gf_g2p_matrix_integ: that's an actual
     decision matrix with input and chosen result columns.)

    This discover/decide matrix will have at least SOME of these columns:

    GDEST    = The Git commit we're copying to Perforce,
               and its corresponding Perforce branch
               Always exists.

    GPARN    = Intersection of one Git commit parent and one Git Fusion branch.
               One for eah such Git Fusion branch. Can be lightweight or
               fully populated.

    GPARFPN  = Fully-populated basis for an above GPARN, usually one for each
               lightweight GPARN.

    P4JITFP  = Perforce branch @ changelist that serves as fully populated
               basis for GDest's lightweight branch. Unused, omitted,
               if GDest is on a fully populated destination branch.
               Often a duplicate of one of GPARFPN.

    P4IMPLY  = Perforce implied parent change: previous Perforce changelist on
               destination branch. Unused, omitted, if previous changelist on
               this Perforce branch corresponds to one of GPARN.

    Rows are stored in a dict (not a list), keyed by Git Work Tree path,
    one row per GWT file path. Each row has its own list of cells, cell indices
    which are set in discover_branches() (which also discovers columns).

            GDEST    P4JITFP   P4IMPLY   GPARN=0  GPARFPN=0  GPARN=N
    file1     .         .         .         .         .         .
    file2     .         .         .         .         .         .
    file3     .         .         .         .         .         .

    1.  Discover columns: Determine which columns we'll have for this commit.
        Set column indices.

    2.  Discover rows: For each column, gather input:
        Run commands to get lists of files/actions to fill in
        that column's cells with initial input data.
        - git-fast-export usually tells us GPARN=0's data in the simple case of
          one parent, one parent branch, same branch.
        - git-diff-tree and some P4.Map work tells us GPARN
        - p4 files tells us which files need to be Just-in-Time branched
          before we can add/edit/delete them
        - p4 integ -n can tell us additional file actions to copy
          between branches

    3.  Decide: For each file/row, decide what to do about this file.
        Some actions prohibit others.
        Some file paths (symlinks!) prohibit others.
        Store output results in cells.

    4.  Do: For each column, collapse column's output results into as few
        Perforce requests as possible, then run those requests.
        Apply columns in order:
        - branch for add/edit or delete from P4JIT
        - integ from GPARN
        - add/edit/delete from GDest

    Assumes that 'p4 sync [-k] //client/...@prev' has already been run.

    See G2PMatrixColumn for what's stored in which column's cells.
    '''
    def __init__( self
                , ctx
                , current_branch
                , fe_commit
                , g2p
                ):

        assert ctx
        assert current_branch
        assert fe_commit

        self.ctx            = ctx
        self.current_branch = current_branch
        self.fe_commit      = fe_commit

                        # G2P instance that created us.
                        #
                        # A sign of unfinished refactoring. We still peek and
                        # poke G2P's internal data. Eventually this will all
                        # migrate, either to this G2PMatrix, or if shared, to a
                        # separate class that both G2P and G2PMatrix can share.
        self.g2p            = g2p

                        # List of Column.
                        # Same integer indices as Row.cells
        self.columns        = []

                        # GWT path : Row
        self.rows           = {}
        self.rows_sorted    = []    # .rows.values(), sorted by gwt_path
                                    # Filled in at end of discover().

                        # The previous Perforce changelist on the current
                        # branch.
                        #
                        # If 0, then we know there are no changelists yet on
                        # this branch, that we are the first changelist on
                        # this branch.
                        #
                        # Lazy-fetched in current_branch_head_change_num(),
                        # so prefer that function over this data member.
        self._current_branch_change_num = None  # int

        self._temp_p4branch         = self.ctx.temp_branch()

                        # GWT paths of files that are symlinks in
                        # the destination commit.
                        #
                        # Each string ends with '/' regardless of whether the
                        # symlink points to a directory or not. Makes later
                        # scans for "is this _used_ as an ancestor directory?"
                        # easier.
        self._gdest_symlinks = []


                        # +++ Avoid Column.find() within "for each row" loop.
                        #     Point directly to these elements of self.columns
        self._gdest_column          = None
        self._p4jitfp_column        = None
        self._p4imply_column        = None
        self._integ_dest_column     = None

                        # From which Column(s) from self.columns are we
                        # populating this new branch?
                        #
                        # Usually empty. Filled in only when this is the first
                        # commit on a new branch AND we have a parent branch
                        # from which to populate.
        self._populate_from_column  = None

                        # From which Column of self.column should we trust Git's
                        # own delta (either git-fast-export or git-diff-tree)?
        self.git_delta_column       = None

                        # Are we going to submit a ghost Perforce changelist
                        # before copying the current Git commit to Perforce?
                        # Here's where we accumulate  decisions about what goes
                        # into that ghost changelist.
                        #
        self.ghost_column            = None

                        # Remember each integ across branches so that we can
                        # limit the Perforce server's search for unintegrated
                        # file actions.`
        if not ctx.integrated_up_to:
            ctx.integrated_up_to = p4gf_integrated_up_to.IntegratedUpTo()
        self.integrated_up_to = ctx.integrated_up_to

                        # Avoid case conflict problems on Mac OS X.
                        # Assumes your filesystem is case-sensitive!
        p4gf_util.force_case_sensitive_p4api(ctx.p4)

                        # A heterogeneous mix of branch views followed by p4
                        # integ results from using that branch view.
        self.integ_batch_history = []

                        # Switch between ghost and non-ghost accessors.
                        # Never None, always one of RowWrapper or
                        # GhostRowWrapper.
        self.row_wrapper = RowWrapper()

    def discover(self):
        '''
        Learn all we need about a single Git commit,
        its parents and branches and files and actions.
        '''
        with Timer(DISCOVER):

            self.row_wrapper = RowWrapper()

            self._p4_sync_k_0()
            self._force_local_filesystem_empty(DISCOVER_RM_RF)

            self._discover_branches()   # aka "discover columns"
            self._discover_files()      # aka "discover rows"

            # Filter out submodule entries as we will not be doing anything
            # with them here.
            self.rows = {path: row for path, row in self.rows.items() if row.mode != 0o160000}

            # Now that we have all the rows we'll ever have, create a flat
            # sorted list by gwt_path so that when we iterate we'll tend to get
            # to parent nodes before their children.
            self.rows_sorted = [ self.rows[key]
                                 for key in sorted(self.rows.keys()) ]

    def decide(self):
        '''
        Use information that discover() collected to choose an appropriate
        action for each file.
        '''
        with Timer(DECIDE):
            # +++ Special short-circuit for linear histories of fully populated
            #     branches: git-fast-export tells us exactly what to do.
            #     There is no need to discover anything further.

            self.row_wrapper = RowWrapper()

            if self.is_linear_fp():
                self.decide_rows_linear_fp()
            else:
                # Choose actions for each individual file.
                row_decider = RowDecider(self)
                for row in self.rows_sorted:
                    row_decider.decide(row)
                    # If this row is completely undecided, check if it
                    # is not in fact a deletion of a submodule, which
                    # always shows up as a phantom tree entry with no
                    # corresponding entry in Perforce.
                    gdest_cell       = row.cell_if_col(self._gdest_column)
                    if (     gdest_cell
                        and  gdest_cell.discovered
                        and (gdest_cell.discovered.get('git-action') == 'D')):
                        _detect_submodule(self.ctx, row, self.fe_commit)

            # If we're still left with an empty Perforce changelist, force at
            # least one file open. Perforce does not permit 'p4 submit' of empty
            # changelists.
            self._decide_force_open_if_none()

    def do_it(self):
        '''
        Actually perform the Peforce integ/add/edit/delete actions that
        decide() chose.
        '''
        with Timer(DO_IT):
            LOG.debug('do_it()')
            if LOG_DUMP.isEnabledFor(logging.DEBUG2):
                sha1 = p4gf_util.abbrev(self.fe_commit.get('sha1'))
                desc = self.fe_commit.get('data', '')[:20]
                if '\n' in desc:
                    desc = desc[:desc.find('\n')]
                nc = self.ctx.numbered_change.change_num \
                        if self.ctx.numbered_change else '-'
                LOG_DUMP.debug2('Giant Matrix Dump: @{nc} {sha1} {desc}\n'
                                .format(nc=nc, sha1=sha1, desc=desc)
                                + '\n'.join(dump(self)))

            ### Debugging a specific changelist? Want to inspect the wreckage?
            ### This code exits without calling cleanup code.
            ###
            ### if self.ctx.numbered_change.change_num in [9,'9']:
            ###    os._exit(1)

            if self.is_linear_fp():
                        # +++ Linear fully populated history does not need to
                        # +++ waste time or network I/O to sync, integ, or
                        # +++ resolve any files. Skip
                self._do_create_local_placeholder_if()
            else:
                        # First, integ for delete.
                        # The process of integ +  resolve can create local
                        # filesystem files and ancestor directories, all of
                        # which can prevent later integ for branch or edit. Get
                        # these done and resolved and off our local filesystem.

                for col in self.columns:
                        # Skip GHOST integs. Those were handled in ghost_do_it().
                    if col.col_type == Column.GHOST:
                        continue

                    self._do_integ(column = col, for_delete = True)

                self._force_local_filesystem_empty(DO_IT_RM_RF)

                        # Sync a small subset of client to local filesystem:
                        # just the files that have to exist locally to keep 'p4
                        # integ' from failing with error. We don't _really_ need
                        # these files (Git has all the file content we need),
                        # but Perforce requires them. Can't be helped.
                self._do_p4_sync_f()
                self._do_create_local_placeholder_if()

                        # Integ, second wave: edit and branch. No delete.
                for col in self.columns:
                        # Skip GHOST integs. Those were handled in ghost_do_it().
                    if col.col_type == Column.GHOST:
                        continue

                    self._do_integ(column = col, for_delete = False)

                        # A little bit of post-integ discovery and deciding to
                        # react to whatever the Perforce server did for our
                        # integ+resolve requests.
                self._decide_p4_requests_post_do_integ()
                self._decide_p4filetypes_post_do_integ()

                self._set_p4_requests_for_local_git_diffs()

                        # Back to doing things. add/edit/delete.
            self._copy_files_from_git()
            self._do_batches_add_edit_delete()

                        # Let outer code call 'p4 submit'

    def ghost_decide(self):
        '''
        Do we have anything to hide in a ghost commit?
        '''
        if not self.ghost_column:
            return

        self.row_wrapper = GhostRowWrapper(self.ghost_column)

        # Choose actions for each individual file.
        row_decider = RowDecider(self)
        for row in self.rows_sorted:
            row_decider.ghost_decide(row)

    def ghost_do_it(self):
        '''
        If we need to insert a ghost p4changelist before copying the
        current Git commit to Perforce, then do so now.

        Return True if we opened anything for the ghost changelist, False if not.
        If we return True, caller must submit.
        '''
        if not self.ghost_column:
            return False

        self.row_wrapper = GhostRowWrapper(self.ghost_column)

        with Timer(DO_IT):

            if LOG_DUMP.isEnabledFor(logging.DEBUG2):
                sha1 = p4gf_util.abbrev(self.fe_commit.get('sha1'))
                desc = self.fe_commit.get('data', '')[:20]
                if '\n' in desc:
                    desc = desc[:desc.find('\n')]
                nc = self.ctx.numbered_change.change_num \
                        if self.ctx.numbered_change else '-'
                LOG_DUMP.debug2('Ghost Matrix Dump: @{nc} {sha1} {desc}\n'
                                .format(nc=nc, sha1=sha1, desc=desc)
                                + '\n'.join(dump(self)))

                        # This function should read like a simplified version of
                        # do_it(). That's where I copied most of this code from.

                        # First, integ for delete.
                        # The process of integ +  resolve can create local
                        # filesystem files and ancestor directories, all of
                        # which can prevent later integ for branch or edit. Get
                        # these done and resolved and off our local filesystem.
            self._do_integ(column = self.ghost_column, for_delete = True)
            self._force_local_filesystem_empty(DO_IT_RM_RF)

                        # Sync a small subset of client to local filesystem:
                        # just the files that have to exist locally to keep 'p4
                        # integ' from failing with error. We don't _really_ need
                        # these files (Git has all the file content we need),
                        # but Perforce requires them. Can't be helped.
            self._ghost_do_p4_sync_f()

                        # Second, integ for branch.
            self._do_integ(column = self.ghost_column, for_delete = False)

                        # A little bit of post-integ discovery and deciding to
                        # react to whatever the Perforce server did for our
                        # integ+resolve requests.
            self._ghost_decide_p4_requests_post_do_integ()
            self._decide_p4filetypes_post_do_integ()
            self._ghost_copy_files_from_git()
            self._ghost_add_for_delete()
            self._do_batches_add_edit_delete()

                        # Tell caller if we opened anything that
                        # requires caller to 'p4 submit'
            opened = self.ctx.p4.run(['opened', '-m1'])
            LOG.debug2("ghost_do_it() opened -m1={}".format(opened))
            return True if opened else False

    def convert_for_second_ghost_changelist(self):
        '''
        In rare cases, we need TWO ghost changelists. This second changelist
        deletes any files that we had to add + delete.

        Destructively convert this G2PMatrix instance to hold only delete
        actions for that second changelist.

        Return True if converted, False if no need for second changelist.
        '''
        if not self.ghost_column:
            return False
        i = self.ghost_column.index     # for less typing

        row_list = []
        for row in self.rows_sorted:
            if (    row.cells[i]
                and row.cells[i].decided
                and row.cells[i].decided.add_delete):
                row.cells[i].decided.p4_request = 'delete'
                row_list.append(row)

        if not row_list:
            return False

        self.rows_sorted = row_list
        return True

    # -- discover -------------------------------------------------------------

    def _discover_branches(self):
        '''
        What branch receives this commit? Which branches might contribute?

        Create a column object for each contributing branch.
        '''
        with Timer(DISCOVER_BRANCHES):

            # The Git commit that we're copying goes on what branch?
            gdest = Column(
                      col_type     = Column.GDEST
                    , branch       = self.current_branch
                    , depot_branch = self.current_branch \
                                        .find_depot_branch(self.ctx)
                    , sha1         = self.fe_commit['sha1']
                    , change_num   = self._current_branch_head_change_num()
                    )

            # Intersect each Git parent commit with
            # zero or more Perforce branch views.
            par_sha1_list = self.g2p._parents_for_commit(self.fe_commit)
            par1_otl      = []
            par_otl       = []
            if par_sha1_list:
                # Keep a copy of first-parent's otl in a second list,
                # as well as in the unified par_otl list.
                par1_otl  = self.g2p.commit_sha1_to_otl(par_sha1_list[0])
                par_otl.extend(par1_otl)
                for par_sha1 in par_sha1_list[1:]:
                    par_otl.extend(self.g2p.commit_sha1_to_otl(par_sha1))

            # If a single Perforce branch view appears multiple times, collapse
            # down to just the most recent (highest changelist number) for each
            # contributing branch view.
            par_otl = self._keep_highest_change_num_per_branch_id(par_otl)

            # Add one column for each Perforce branch that intersects one
            # (or more) Git parent commits.
            gparn_list = [ self._ot_to_gparn(
                                          par_ot
                                        , is_first_parent=(par_ot in par1_otl)
                                        )
                           for par_ot in par_otl ]

            # Add one GPARFPN fully-populated basis column for each lightweight
            # GPARN column that has a basis. Yes, this implies that fully
            # populated Perforce will appear many times in our column list.
            # That's okay.
            gparfpn_list = [ self._gparn_to_gparfpn(gparn)
                             for gparn in gparn_list]

            p4jitfp = self._to_p4jitfp()
            p4imply = self._to_p4imply(par_otl)
            ghost   = self._to_ghost( gdest      = gdest
                                    , p4jitfp    = p4jitfp
                                    , p4imply    = p4imply
                                    , gparn_list = gparn_list )

            col_list = self._strip_none( [ gdest, p4jitfp, p4imply, ghost ]
                                       + gparn_list
                                       + gparfpn_list )

            # +++ Copy index positions into each element so that we don't have
            #     to do a list scan just to find position.
            for i, column in enumerate(col_list):
                column.index = i

            self.columns                = col_list
            self._populate_from_column  = self._to_populate_column(gparn_list)
            self._gdest_column          = gdest
            self._p4jitfp_column        = p4jitfp
            self._p4imply_column        = p4imply
            self.ghost_column           = ghost
            self.git_delta_column       = p4gf_util.first_of([p4imply, gdest])

            if self._p4imply_column:
                self._integ_dest_column = self._p4imply_column
            else:
                for gparn in gparn_list:
                    if gparn.branch == gdest.branch:
                        self._integ_dest_column = gparn
                        break

            LOG.debug3('git_delta_column  = {}'
                       .format(self.git_delta_column.index
                               if self.git_delta_column else None))

    def _ot_to_gparn(self, par_ot, is_first_parent):
        '''
        Create a new GPARN parent column for a parent commit's ObjectType.
        '''
        branch       = self.ctx.branch_dict().get(par_ot.details.branch_id)
        depot_branch = branch.find_depot_branch(self.ctx)
        return Column ( col_type        = Column.GPARN
                      , branch          = branch
                      , depot_branch    = depot_branch
                      , sha1            = par_ot.sha1
                      , change_num      = int(par_ot.details.changelist)
                      , is_first_parent = is_first_parent
                      )

    def _gparn_to_gparfpn(self, gparn):
        '''
        If gparn's branch is lightweight and backed by fully populated Perforce,
        return a new GPARFPN column for that fully populated basis.
        If not, return None.
        '''
        if not gparn.branch.is_lightweight:
            return None

        change_num = gparn.branch.find_fully_populated_change_num(self.ctx)
        if not change_num:
            return None

                        # Find our ancestor Git commit that goes with
                        # that change_num.

                        ### BUG: It is possible for change_num to be a
                        ### changelist that originated in Perforce, and thus
                        ### straddles MULTIPLE Git Fusion FP branches. Randomly
                        ### choosing any old match here is incorrect, need to
                        ### choose one that is indeed an ancestor of GPARN.

        sha1 = self.g2p._change_num_to_sha1(change_num, branch_id=None)

        return Column ( col_type       = Column.GPARFPN
                      , branch         = gparn.branch
                      , depot_branch   = None
                      , sha1           = sha1
                      , change_num     = int(change_num)
                      , fp_counterpart = gparn   # also sets gparn->gparfp link
                      )

    def _to_p4jitfp(self):
        '''
        Return a new Column for the current branch's fully populated basis,
        or None if not.
        '''
        LOG.debug('_to_p4jitfp() current_branch={} new={}'
                  .format(self.current_branch, self.current_branch.is_new))
        fp_basis = self.current_branch.find_fully_populated_change_num(self.ctx)
        if not fp_basis:
            return None
        return Column( col_type     = Column.P4JITFP
                     , branch       = self.current_branch
                     , depot_branch = None
                     , change_num   = int(fp_basis)
                     )

    def _to_p4imply(self, par_otl):
        '''
        Return a new Column if the destination branch appears NOWHERE yet as a
        parent, but contains at least one Perforce changelist. If so, that
        changelist implicitly becomes a parent within Perforce but  not Git.

        Return None if not.
        '''
        branch_id_list = (par_ot.details.branch_id for par_ot in par_otl)
        if self.current_branch.branch_id in branch_id_list:
            return None

        change_num = self._current_branch_head_change_num()
        if not change_num:
            return None

        sha1 = self.g2p._change_num_to_sha1( change_num
                                           , self.current_branch.branch_id )

        return Column(
                  col_type     = Column.P4IMPLY
                , branch       = self.current_branch
                , depot_branch = self.current_branch.find_depot_branch(self.ctx)
                , change_num   = change_num
                , sha1         = sha1
                )

    def _requires_ghost(self, gdest, p4jitfp, p4imply, gparn_list):
        '''
        Do we need to submit a Ghost changelist before copying the current
        Git commit?

        Return True if there's any chance that we need it.

        Can return True even if it turns out that we don't really need a Ghost
        changelist: we returned True because we need to delete a file, but don't
        (yet) know whether we need to branch-for-delete or not. That gets
        decided later.
        '''
                        # Rearranging the depot branch to match a
                        # different Git first-parent?
        if p4imply:
            return True

                        # Populating a new depot branch?
        if self._to_populate_column(gparn_list):
            return True

        can_branch_for_edit = (    p4jitfp
                               and gparn_list
                               and gparn_list[0].branch == gdest.branch )

                        # JIT branch-for-delete?
                        # Deleting any files in a lightweight branch? That's a
                        # (potential) JIT-branch-for-delete, put the branch
                        # action in a ghost changelist.
                        #
                        # OK to return True here even if it turns out we don't
                        # need to branch-for-delete, don't need a ghost
                        # changelist after all.
        if gdest.branch.is_lightweight:
            for fe_file in self.fe_commit['files']:
                if fe_file.get('action') == 'D':
                    return True
                if fe_file.get('action') == 'M' and can_branch_for_edit:
                    return True

        return False

    def first_parent_column(self):
        '''
        Return the GPARN column that corresponds to the Git first parent commit.

        Return None if no such GPARN.
        '''
        return Column.find_first_parent(self.columns)

    def _to_ghost(self, gdest, p4jitfp, p4imply, gparn_list):
        '''
        Create a GHOST column to hold any Perforce file actions that
        must be submitted before we can copy the current Git commit to Perforce.

        BUG: Why are we producing GHOST column for M5? Any others?
        '''
        if not self._requires_ghost( gdest      = gdest
                                   , p4jitfp    = p4jitfp
                                   , p4imply    = p4imply
                                   , gparn_list = gparn_list ):
            return None

        first_parent_col = Column.find_first_parent(gparn_list)
        if first_parent_col:
            return Column(
                      col_type       = Column.GHOST
                    , branch         = first_parent_col.branch
                    , depot_branch   = first_parent_col.depot_branch
                    , sha1           = first_parent_col.sha1
                    , change_num     = first_parent_col.change_num
                    , fp_counterpart = first_parent_col.fp_counterpart
                    )
        else:
                        # Rare: a Git commit with no parent.
                        #
                        # The only reason we'd insert a ghost changelist
                        # is to rearrange a reused Perforce depot branch
                        # to emptiness. Delete _all_ the files!
            return Column(
                      col_type     = Column.GHOST
                    )

    def _to_populate_column(self, col_list):
        '''
        From which column should we populate a new branch?

        If this is the first commit on a new branch, return the git-first-parent
        GPARN column from col_list. (Implied: also need to copy from the GPARN's
        GPARFPN basis if populating a new fully populated branch.)

        Return None if branch already populated, OR if no suitable source
        column in gparn_list.
        '''
        # Already populated?
        if self._current_branch_head_change_num():
            return None

        return Column.find_first_parent(col_list)

    @staticmethod
    def _strip_none(lst):
        '''Some things in this room do not react well to None.'''
        return [x for x in lst if x != None]

    @staticmethod
    def _keep_highest_change_num_per_branch_id(otl):
        '''
        Retain only one ObjectType element per branch_id, keeping the
        element with the highest Perforce changelist number.

        Returns same list, unchanged, if nothing to skip.
        '''
        skipped = False
        d = {}
        for ot in otl:
            d_ot = d.get(ot.details.branch_id)
            if (   d_ot
                and int(ot.details.changelist) <= int(d_ot.details.changelist) ):
                skipped = True
                continue
            d[ot.details.branch_id] = ot
        if skipped:
            return list(d.values())
        else:
            return otl

    def _discover_files(self):
        '''
        Fill in our rows dict with one row for each destination file that
        we need to add/edit/delete/integrate.
        '''
        with Timer(DISCOVER_FILES):
            self._discover_files_gdest()
            self._discover_p4_files_gdest()

            # +++ Special short-circuit for linear histories of fully populated
            #     branches: git-fast-export tells us exactly what to do. There is
            #     no need to discover anything further.
            if not self.is_linear_fp():

                self._git_ls_tree_gdest()
                self._git_ls_tree_ghost()
                self._discover_files_gparn()
                self._discover_files_gparfpn0()
                self._discover_p4imply_files()
                self._discover_ghost_files()
                self._integ_across_depot_branches()
                self._discover_jit()

            self._discover_symlinks()       ### Can we move this inside the
                                            ### short-circuited code above?

    def _discover_files_gdest(self):
        '''
        Transfer git-fast-export's instructions to our matrix.
        '''
        # We're the first _discover_files_xxx() to run, so we get to
        # clobber the files dict without worrying about previous values.
        # assert(): Make sure we really ARE the first to touch the files dict.
        assert not self.rows
        LOG.debug('_discover_files_gdest()')
        self.rows = { fe_file.get('path') : self._fe_file_to_row(fe_file)
                      for fe_file in self.fe_commit['files'] }

    def _discover_p4_files_gdest(self):
        '''
        What files already exist within the destination Perforce branch?
        Such files do not require JIT-branch fileactions.
        '''
        with Timer(DISCOVER_P4_FILES_GDEST):
            if not self._gdest_column.change_num:
                LOG.debug('_discover_p4_files_gdest()'
                          ' no GDEST change_num. Skipping.')
                return
            LOG.debug('_discover_p4_files_gdest()')
            self._store_discovered_cells( self._gdest_column
                                        , self._files_at(self._gdest_column))

    def _fe_file_to_row(self, fe_file):
        '''
        Convert a single 'files' element from a git-fast-export commit
        to a single Row() object, with one Cell filled in for GDEST.
        '''
        gwt_path     = fe_file.get('path')
        row = Row( gwt_path   = gwt_path
                 , depot_path = self.ctx.gwt_path(gwt_path).to_depot()
                 , sha1       = fe_file.get('sha1')
                 , mode       = p4gf_util.octal(fe_file.get('mode'))
                 , col_ct     = len(self.columns) )

        cell = Cell()
        cell.discovered = { 'git-action' : fe_file.get('action') }
        row.cells[self._gdest_column.index] = cell
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3(row.to_log_level(LOG.getEffectiveLevel()))
        return row

    def is_linear_fp(self):
        '''
        Is this commit on the same branch as its parent commit, AND is this
        branch fully populated? If so, then there's never anything to integrate,
        all we have to do is add/edit/delete as dictated by git-fast-export.
        '''
        # Fully populated branches, or lightweight branches with no FP basis are
        # "fully populated" in that they have no P4JITFP column to worry about.
        #
        # Lightweight branches with an FP basis are not fully populated,
        # have a P4JITFP column to consider.
        if self.current_branch.is_lightweight:
            dbi = self.current_branch.depot_branch          # for less typing
            if (   (not dbi)                        # Not calced yet? Assume
                                                    # FP basis, but probably
                                                    # never happens.
                or (dbi.parent_changelist_list) ):  # Has FP basis.
                return False

            # If this commit we're reuses an existing lightweight branch, we
            # have to figure out diffs from our implied parent.
            if self._p4imply_column:
                return False

        # Exactly one parent branch?
        if 1 != Column.count(self.columns, Column.GPARN):
            return False

        # Parent on same branch as current?
        gpar0 = Column.find(self.columns, Column.GPARN)
        return gpar0.branch == self.current_branch

    @staticmethod
    def _cell_has_discovered(cell, key=None):
        '''
        Return True for cells that exist, have a discovered dict,
        and (optional) have a discovered value for the given key.

        See also Cell.safe_discovered()
        '''
        if not cell:
            return False
        if not cell.discovered:
            return False
        if key and key not in cell.discovered:
            return False
        return True

    def _iter_rows_with_discovered(self, column_index, key=None):
        '''
        Generator to iterate over rows that have discovered dicts,
        and (optionally) values for the given key.

        Works with unsorted self.rows, not self.rows_sorted, so that
        you can use it during discovery.
        '''
        for row in self.rows.values():
            cell = row.cells[column_index]
            if not self._cell_has_discovered(cell, key):
                continue

            yield row

    def _git_ls_tree_gdest(self):
        '''
        Run 'git-ls-tree -r' on the current/destination commit and store the
        results in GDEST.

        Currently records only file mode, because that gives us:
        -- exists? (is there a mode at all)
        -- is symlink?
        If we need more, keep more.
        '''
        with Timer(DISCOVER_GIT_LS_TREE_GDEST):
            LOG.debug('_git_ls_tree_gdest()')
            self._git_ls_tree( column      = self._gdest_column
                             , commit_sha1 = self.fe_commit['sha1'])

            # Promote blob sha1s of git-ls-tree of GDEST commit to row-level
            # storage, because "has a filemode and sha1 at row-level storage" is
            # how we tell if a file exists in the destination commit (and what
            # its contents are).
            #
            # Can't do this in _git_ls_tree() because _git_ls_tree() runs
            # for P4IMPLY columns as well as GDEST.
            for row in self._iter_rows_with_discovered(
                              column_index = self._gdest_column.index
                            , key          = 'sha1'):
                cell = row.cells[self._gdest_column.index]

                sha1 = cell.discovered['sha1']
                assert(   (not row.sha1)
                       or (row.sha1 == sha1))
                row.sha1 = sha1

    def _git_ls_tree_ghost(self):
        '''
        Run 'git-ls-tree -r' on the commit we're trying to recreate in our GHOST
        column and store the results in GDEST.
        '''
        if (   (not self.ghost_column)
            or (self.ghost_column.sha1 == None)):
            return

        with Timer(DISCOVER_GIT_LS_TREE_GDEST):
            LOG.debug('_git_ls_tree_ghost()')
            self._git_ls_tree( column      = self.ghost_column
                             , commit_sha1 = self.ghost_column.sha1)

    def _git_ls_tree(self, column, commit_sha1):
        '''
        Run 'git-ls-tree -r' on the current/destination commit and store the
        results in the requested column's cells.

        If the requested column is GDEST, also store the file mode in row.mode.

        Records in column's cell.discovered:
        -- cell.discovered['sha1']
        -- cell.discovered['git-mode'] : string "100644" not integer

        If we need more, keep more.
        '''
        LOG.debug('_git_ls_tree() col={col} commit_sha1={sha1}'
                  .format( col  = column.index
                         , sha1 = p4gf_util.abbrev(commit_sha1)))
        for r in p4gf_util.git_ls_tree_r( repo         = self.ctx.view_repo
                                        , treeish_sha1 = commit_sha1 ):
            if r.type != 'blob':
                common.debug3('_git_ls_tree() skip {r}', r=r)
                continue
            common.debug3('_git_ls_tree()      {}', r)

            row = self._row(r.gwt_path)
            cell = row.cell(column.index)
            if not cell.discovered:
                cell.discovered = {}

            r_mode = p4gf_util.octal(r.mode)

            cell.discovered['sha1'    ] = r.sha1
            cell.discovered['git-mode'] = r.mode

            if column.col_type == Column.GDEST:
                # Our value for 'mode' clobbers any value from
                # git-fast-export, previously stored by _fe_file_to_row().
                # Should be fine, should match.
                if row.mode and (row.mode != r_mode):
                    LOG.error('BUG: row.mode={old} != ls-tree mode={new} {ls}'
                              .format(old=row.mode, new=r_mode, ls=r))
                assert not row.mode or (row.mode == r_mode)
                row.mode = r_mode

    def _current_branch_head_change_num(self):
        '''
        Return integer changelist number of most recent changelist submitted
        to current branch.

        Return 0 if no changelists on current branch.
        '''
        if self._current_branch_change_num == None:
            with self.ctx.switched_to_branch(self.current_branch):
                r  = self.ctx.p4run([ 'changes', '-m1'
                                    , self.ctx.client_view_path()])
                rr = p4gf_util.first_dict_with_key(r, 'change')
            if not rr:
                self._current_branch_change_num = 0
            else:
                self._current_branch_change_num = int(rr['change'])
        return self._current_branch_change_num

    def _discover_files_gparn(self):
        '''
        Discovery.

        What are the possible integration sources from all of our GPARN parents,
        including their GPARFPN fully populate bases?

        Runs 'p4 files' in each GPARN, GPARFPN column.

        If we're also the very first changelist in a new depot branch, also run
        'p4 copy -n' from each GPARN.
        '''
        with Timer(DISCOVER_POPULATE):
            if self.current_branch.is_populated(self.ctx):
                self._discover_files_at_gparn()
            else:
                self._discover_files_populate_from()

    def _discover_files_gparfpn0(self):
        '''
        Discovery.

        If first-parent GPARN is lightweight, how does it differ from its
        fully populated basis GPARFPN?

        Runs 'git-diff-tree GPARFPN GPARN' and stores action
        in GPARFPN's git-action.
        '''
        with Timer(DISCOVER_POPULATE):
            first_parent_col = Column.find_first_parent(self.columns)
            if not first_parent_col:
                LOG.debug("_discover_files_gparfpn0 GPARN=None")
                return

            LOG.debug("_discover_files_gparfpn0 GPARN.index={} .lw={}"
                      .format( first_parent_col.index
                             , first_parent_col.branch.is_lightweight))

            if not first_parent_col.branch.is_lightweight:
                return

            gparfpn_col = first_parent_col.fp_counterpart

            if not gparfpn_col:
                return
                ### BUG in _discover_branches(). Even when a lightweight branch
                ### lacks an FP counterpart (it can happen!), we need a GPARFPN
                ### column in which to store our "delta from 0000" actions.

            self._discover_git_diff_tree_files(
                                             col_index = gparfpn_col.index
                                           , old_sha1  = gparfpn_col.sha1
                                           , new_sha1  = first_parent_col.sha1 )

    def _copy_from(self, column):
        '''
        Run 'p4 copy -n -b <temp_branch> //...@cl' and return the results.

        Discover what files can be copied from what contributing branch.
        '''
        # Our own branch usually appears in an GPARN column somewhere.
        # Don't try to integ from ourself to ourself.
        if column.branch == self.current_branch:
            return

        self._branch_map_from(column)
        return self.ctx.p4run([ 'copy', '-n'
                              , '-b', self._temp_p4branch.name
                              , '//...@{cl}'.format(cl=column.change_num)
                              ])

    def _integ_range(self, from_column):
        '''
        Return a string "123,456" if we have a start and end
        changelist number, or just "456" if we only have an
        end changelist number.
        '''
        #return str(from_column.change_num)
        start_change_num = self.integrated_up_to.get(
                  from_branch = from_column.branch
                , to_branch   = self.current_branch)
        stop_change_num  = from_column.change_num
        if start_change_num:
            cl_range = '{},{}'.format(start_change_num, stop_change_num)
        else:
            cl_range = str(stop_change_num)
        return cl_range

    def _integ_preview_from(self, column):
        '''
        Run 'p4 integ -n -Rbd -t -i -b <temp_branch> //...@cl'
        ##ZZ maybe ## Run 'p4 integ -n -d -t -i -b <temp_branch> //...@cl'
        and return the results.

        Discover what files can be integrated from what contributing branch.

        Performance Warning:
        This can be VERY expensive. I've seen these integ preview requests take
        30+ seconds of server time. For lightweight branches based on fully
        populated Perforce, 95% of these results are not ever integrated, so
        that's a lot of per-commit expense for file actions we never perform.
        '''
        # Our own branch usually appears in an GPARN column somewhere.
        # Don't try to integ from ourself to ourself.
        if column.branch == self.current_branch:
            return

        self._branch_map_from(column)

        with self.ctx.p4.at_exception_level(P4.P4.RAISE_NONE):
                    # RAISE_NONE: don't raise exception on
                    # [Warning]: 'All revision(s) already integrated.'

                    ##ZZ maybe -Rb safe here, since 'p4 files' always clobbers.
                    # Do NOT use -Rbd here. -Rbd here will cause the integ to
                    # leave a file open for 'delete' if it exists in source but
                    # not in dest. We need to see that as 'branch' in order to
                    # know that it is indeed a candidate for being branched.
                    #
                    # Using -d instead.

            return self.ctx.p4run([ 'integ', '-n'
                                  , '-3'   # New integ -3 engine is 25x faster
                                           # than integ -2.
                                  #,'-Rbd' # Schedule branch or delete resolves.
                                  , '-d'   # Just delete, don't schedule for
                                           # delete resolve.
                                  , '-t'   # Propagate filetype changes.
                                  , '-i'   # Permit baseless merges.
                                  , '-b', self._temp_p4branch.name
                                  , '//...@{}'.format(self._integ_range(column))
                                  ])

    def _files_at(self, column):
        '''
        Run 'p4 files //client/...@cl' and return the results.

        Discover what files exist and their Perforce filetype.

        Does NOT pass -e to 'p4 files', so result list will include deleted
        file revisions. Some callers need that.
        '''
        # Only run 'p4 files' once per column.
        if column.discovered_p4files:
            return
        column.discovered_p4files = True

        # Usually we use the branch view unaltered, but for columns of fully
        # populated basis for a lightweight branch, use the lightweight branch,
        # rerooted to FP.
        src_branch = column.branch
        if column.col_type in [Column.GPARFPN, Column.P4JITFP]:
            src_branch = column.branch.copy_rerooted(None)

        with self.ctx.switched_to_branch(src_branch):
            result_list = self.ctx.branch_files_cache.files_at(
                                             ctx        = self.ctx
                                           , branch     = src_branch
                                           , change_num = column.change_num )

            # Use the GPARN source branch view to translate to gwt_path
            # while that branch is still in effect.
            for r in result_list:
                r['gwt_path'] = self._gwt_path(r)
            return result_list

    def _discover_files_populate_from(self):
        '''
        Discovery.

        In case we need to seed a new branch (lightweight or fully populated),
        fill in git-first-parent GPARN and GPARFPN cells with p4 file info.

        This function was called _populate_first_commit_fp() before Matrix 2,
        but that name was awful: we didn't actually populate (we just
        discovered), and once Matrix2 came along, we used this same function for
        both fp fully populated and lt lightweight. We also need this data for
        more than just populating the first changelist on a new depot branch.
        '''
        LOG.debug('_discover_files_populate_from()')

        for gparn in Column.of_col_type(self.columns, Column.GPARN):
            self._git_ls_tree(gparn, gparn.sha1)

            if not gparn.is_first_parent:
                continue

            # Populate from parent.
            self._store_discovered_cells(gparn, self._copy_from(gparn))
            self._store_discovered_cells(gparn, self._files_at (gparn))

                    # If parent is lightweight, populate from its
                    # fully populated basis.
            gparfpn = gparn.fp_counterpart
            LOG.debug2('_discover_files_populate_from() GPARFPN={}'
                       .format(gparfpn))
            if gparfpn:
                self._store_discovered_cells(gparfpn, self._copy_from(gparfpn))
                self._store_discovered_cells(gparfpn, self._files_at (gparfpn))

    def _discover_files_at_gparn(self):
        '''
        What files exist at what revision and action in each GPARN, GPARFPN
        column?
        '''
        LOG.debug('_discover_files_at_gparn()')

                        # Fetch git ls-tree of each parent so we can match up
                        # file sha1 and type for smarter resolve -at/-ay when
                        # merging between existing Perforce files.
        for gparn in Column.of_col_type(self.columns, Column.GPARN):
            self._git_ls_tree(gparn, gparn.sha1)

            if not gparn.is_first_parent:
                continue

                    # Fetch 'p4 files' result of GPARN 0 so we know
                    # which files already exist in Perforce.
            self._store_discovered_cells(gparn, self._files_at (gparn))

                    # If parent is lightweight, fetch files from its
                    # fully populated basis.
            gparfpn = gparn.fp_counterpart
            LOG.debug2('_discover_files_at_gparn() GPARFPN={}'
                       .format(gparfpn))
            if gparfpn:
                self._store_discovered_cells(gparfpn, self._files_at (gparfpn))

    def _gwt_path(self, d):
        '''
        Find or calculate a Git Work Tree path (aka key into self.rows{})
        from a single 'p4 integ' dict result.
        '''
        if 'gwt_path' in d:
            return d['gwt_path']
        elif 'clientFile' in d:
            return self.ctx.local_path(d['clientFile']).to_gwt()
        elif 'depotFile' in d:
            return self.ctx.depot_to_gwt_path(d['depotFile'])
        return None

    def _row(self, gwt_path, depot_path=None):
        '''
        Find and return a Row for the given gwt_path.

        Create and insert new Row if necessary.
        '''
        row = self.rows.get(gwt_path)
        if not row:
            dp = depot_path if depot_path \
                            else self.ctx.gwt_to_depot_path(gwt_path)
            assert self.columns  # Requires columns!
            row = Row( gwt_path   = gwt_path
                     , depot_path = dp
                     , col_ct     = len(self.columns))
            self.rows[gwt_path] = row
        return row

    def _store_discovered_cells(self, column, p4result_list):
        '''
        Store the results of a 'p4 integ' or other such operation in cells.

        Create Row elements when necessary.
        '''
        if not p4result_list:
            return
        for r in p4result_list:
            self._store_discovered_cell(column, r)

    def _store_discovered_cell(self, column, p4result):
        '''
        Store the results of a 'p4 integ' or other such operation in cells.

        Create Row elements when necessary.
        '''
        if not isinstance(p4result, dict):
            return
        gwt_path = self._gwt_path(p4result)
        if not gwt_path:
            return

        row  = self._row(gwt_path=gwt_path)
        cell =  row.cell(column.index)
        if cell.discovered:
            cell.discovered.update(p4result)
        else:
            cell.discovered = p4result

    # "struct" for git-diff-tree --name-status result rows.
    GitDiffTreeResult = namedtuple('GitDiffTreeResult', ['action', 'gwt_path'])

    @staticmethod
    def _git_diff_tree(old_sha1, new_sha1):
        '''
        Run 'git diff-tree -r --name-status <a> <b>' and return the results
        as a list of GitDiffTreeResult <action, gwt_path> tuples.

        Ideally we would run git diff-tree -r WITHOUT -z or --name-status,
        which would include old/new mode and sha1. But that munges the
        gwt_path, any non-printing chars in the file path are converted
        and the path enquoted. Run 'git ls-tree -r -z' to extract mode and sha1.
        '''
        d = p4gf_proc.popen([ 'git', 'diff-tree', '-r'
                            , '-z'   # -z = machine-readable \0-delimited output
                            #, '--name-status'
                            , old_sha1, new_sha1])

                                        # pylint:disable=W1401
                                        # Anomalous backslash in string: '\0'
                                        # Known bug in pylint 0.26.0
                                        #            fixed in 0.27.0
        for pair in p4gf_util.pairwise(d['out'].split('\0')):
            parts = pair[0].split()
            if parts and parts[0] == ':160000' or parts[1] == '160000':
                # Skip over submodules, cannot process them
                continue
            yield G2PMatrix.GitDiffTreeResult(action=parts[4], gwt_path=pair[1])
                                        # pylint:enable=W1401

    def _discover_git_diff_tree_files(self, col_index, old_sha1, new_sha1):
        '''
        Run git-diff-tree and store its results in 'git-action' discovery cells.
        '''
        LOG.debug3("_discover_git_diff_tree_files() old={} new={} store in col={}"
                   .format( p4gf_util.abbrev(old_sha1)
                          , p4gf_util.abbrev(new_sha1)
                          , col_index))
        for r in self._git_diff_tree(old_sha1, new_sha1):
            row  = self._row(r.gwt_path)
            cell = row.cell(col_index)
            if not cell.discovered:
                cell.discovered = {}
            cell.discovered['git-action'] = r.action
            LOG.debug3('_discover_git_diff_tree_files() {} {}'
                       .format(r.action, r.gwt_path))

    def _discover_p4imply_files(self):
        '''
        If the previous changelist on this branch is NOT one of the current
        commit's parents, then add it as an "implied" parent that's a Perforce
        artifact.

        Compare the previous changelist against the current commit and add
        its delta as a to-do list.
        '''
        with Timer(DISCOVER_P4IMPLY):
            p4imply = Column.find(self.columns, Column.P4IMPLY)
            if not p4imply:
                return

            LOG.debug('_discover_p4imply_files()')
            # Find corresponding Git commit so we can ask Git what's different.
            prev_change_num = self._current_branch_head_change_num()
            prev_sha1       = self.g2p._change_num_to_sha1(
                                                 prev_change_num
                                               , self.current_branch.branch_id )
            if not prev_sha1:
                return

            # Record git-diff-tree actions.
            self._discover_git_diff_tree_files(
                                              col_index = p4imply.index
                                            , old_sha1 = prev_sha1
                                            , new_sha1 = self.fe_commit['sha1'])

            # Record file existence and symlink-ness at the old p4imply commit.
            self._git_ls_tree(p4imply, prev_sha1)

    def _discover_ghost_files(self):
        '''
        What file actions must we submit, in a ghost changelist, before we
        copy the current Git commit to Perforce?
        '''
        if not self.ghost_column:
            return

                        # How does our GDEST's depot branch differ from
                        # what Git thinks the world looks like before this
                        # commit?
        want_sha1 = self.ghost_column.sha1                                  \
                    if self.ghost_column.sha1                               \
                    else p4gf_const.EMPTY_TREE_SHA1
        have_sha1 = None
        if self._p4imply_column and self._p4imply_column.sha1:
            have_sha1 = self._p4imply_column.sha1
        else:
            have_sha1 = self.g2p.branch_to_p4head_sha1(self.current_branch)
        if not have_sha1:
            have_sha1 = p4gf_const.EMPTY_TREE_SHA1

        self._discover_git_diff_tree_files( col_index = self.ghost_column.index
                                          , old_sha1 = have_sha1
                                          , new_sha1 = want_sha1 )

    def _branch_map_from(self, from_column):
        '''
        Redefine our temp branch mapping to map from_column
        to our current_branch.
        '''
        src_branch = from_column.branch
        dst_branch = self.current_branch
        assert src_branch != dst_branch

        src2dst_map = P4.Map.join( src_branch.view_p4map
                                 , dst_branch.view_p4map.reverse())
        self._temp_p4branch.write_map(self.ctx.p4, src2dst_map)

    def _integ_across_depot_branches(self):
        '''
        Cross-branch integ?

        Does this commit include any files from any depot branch other than
        the current branch? It does if this is a merge from another branch.
        It does if is a "linear" commit with a single parent, but that
        parent got assigned to a branch other than this commit.

        Perform a full integrate of each contributing depot branch to the
        current/destination branch, even for files that git-fast-export does
        not list as changed. This ensures that older file actions get
        propagated even if git-fast-export omits them.

        This is especially true for any Git commit that is not a merge, but
        which Git Fusion's branch assignment places on a branch other than its
        parent. (This happens occasionally at branch points where a single Git
        commit has multiple children.)

        All parent commits must already be copied to Perforce.
        '''
        for gparn in Column.of_col_type(self.columns, Column.GPARN):
            LOG.debug('_integ_across_depot_branches() GPARN col={}'
                      .format(gparn.index))
            with Timer(DISCOVER_GPARN_INTEG_N):
                self._store_discovered_cells( gparn
                                            , self._integ_preview_from(gparn))
            with Timer(DISCOVER_GPARN_FILES):
                self._store_discovered_cells(gparn, self._files_at  (gparn))

    def _discover_jit(self):
        '''
        Which files CAN be JIT-branched from our JIT basis?
        '''
        with Timer(DISCOVER_JIT):
            # Only lightweight branches need JIT.
            if not self._gdest_column.branch.is_lightweight:
                return
            # Not all lightweight branches have a fully populated basis.
            p4jitfp = Column.find(self.columns, Column.P4JITFP)
            if not p4jitfp:
                return

            self._store_discovered_cells(p4jitfp, self._files_at(p4jitfp))

    def _discover_symlinks(self):
        '''
        What paths are symlinks in the destination commit?
        '''
        ### Not sure if we still need, skipping until we do: what files are
        ### symlinks in parent commits or contributing Perforce branches?

        with Timer(DISCOVER_SYMLINKS):
            symlinks = []
            for row in (row for row in self.rows.values()
                        if row.cells[self._gdest_column.index]):
                disc = row.cells[self._gdest_column.index].discovered
                if not disc:
                    continue
                mode = row.mode
                if mode == 0o120000:
                    symlinks.append(force_trailing_delimiter(row.gwt_path))

            self._gdest_symlinks = sorted(symlinks)

    def _p4_sync_k_0(self):
        '''
        p4 sync -k @0

        We don't have to sync to the current branch's #head, but we do have to
        sync to @0. This clears our 'p4 have' list of any files that prevent
        later integ preview requests. Such interference causes integ preview to
        fail with "must sync before integrating", not return integ-preview
        results.
        '''
        with Timer(SYNC_K_0):
            self.ctx.p4run(['sync', '-k', '@0'])

    # -- decide ---------------------------------------------------------------

    def symlink_in_path(self, gwt_path):
        '''
        Do any GDEST symlink paths appear as an ancestor directory in
        the given path?

        Return symlink path if found, None if not.
        '''
        for x in self._gdest_symlinks:
            if gwt_path.startswith(x):
                return x
        return None

    def _cell_p4_exists_at_gdest_head(self, row):
        '''
        Does this row appear to have an existing file in Perforce that we
        could 'p4 edit'?
        '''
        cell   = row.cells[self._gdest_column.index]
        action = Cell.safe_discovered(cell, 'action')
        if (   (not action)
            or ('delete' in action) ):
            return False
        return True

    def _ok_to_force_edit(self):
        '''
        Return a row that has a file that already exists in Perforce,
        exists in the Git destination commit, and is suitable for
        'p4 edit'. Return None if none found.
        '''
        for row in self.rows_sorted:
            if not self._cell_p4_exists_at_gdest_head(row):
                continue
            if not row.sha1:
                continue
            if row.mode == 0o120000:    # Symlink edit is too complicated.
                continue                # Force-edit something simpler.
            return row
        return None

    def _decide_force_open_if_none(self):
        '''
        Perforce prohibits 'p4 submit' of an empty changelist, so attempt
        to open at least one file for edit or add.

        If no rows have a decided action, force-edit one row that already
        exists in our branch.

        If NOT rows already exist in our branch, force-add a placeholder
        that p4gf_copy_to_git never copies to Git.
        '''
        # Usually we have at least one file with a decided action.
        for row in self.rows_sorted:
            if row.has_p4_action():
                return

        # Any files exist at head in Perforce? NOP-'p4 edit' one.
        force_row = self._ok_to_force_edit()
        if force_row:
            self._decide_force_edit(force_row)
        else:
            self._decide_force_add_placeholder()

    @staticmethod
    def _decide_force_edit(row):
        '''
        Mark this row for 'p4 edit' even though there's no content change.

        Does NOT sync file to local filesystem. We'll get it from Git instead.
        '''
        LOG.debug('_decide_force_edit() {}'.format(row.gwt_path))
        assert row.sha1
        row.p4_request = 'edit'

    def _decide_force_add_placeholder(self):
        '''
        Add or edit .p4gf_empty_commit_placeholder.
        '''
        gwt_path   = p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER
        depot_path = self.ctx.gwt_path(gwt_path).to_depot()

        row = self.rows.get(gwt_path)
        if not row:
            LOG.debug('_decide_force_add_placeholder new row {}'
                      .format(gwt_path))
            # Insert new row for this placeholder.
            row = Row( gwt_path = gwt_path
                     , depot_path = depot_path
                     , col_ct     = len(self.columns) )

            # New row invalidates old rows_sorted. Re-sort.
            self.rows[gwt_path] = row
            self.rows_sorted = [self.rows[key]
                                for key in sorted(self.rows.keys())]
        else:
            LOG.debug('_decide_force_add_placeholder old row {}'
                      .format(gwt_path))

        # Usually this is 'add', but sometimes the placeholder's already there
        # and can only be 'edit'ed.
        if self._exists_undeleted_at_head_gdest(row):
            row.p4_request = 'edit'
        else:
            row.p4_request = 'add'

    def _do_create_local_placeholder_if(self):
        '''
        If we decided to 'p4 add' a placeholder file, create it now.

        We cannot create it before do_it(), since do_it() clears out our
        directory. Nor can we copy it from Git, because this placholder
        never exists in Git.
        '''

        # Usually we don't need this placeholder
        row = self.rows.get(p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER)
        if not row:
            return

        local_path = self.ctx.gwt_path(row.gwt_path).to_local()
        LOG.debug('_do_create_local_placeholder_if() {}'.format(local_path))
        with open(local_path, 'w') as f:
            f.write('')

    def decide_rows_linear_fp(self):
        '''
        Blindly obey git-fast-export.
        '''
        assert self._gdest_column
        assert self.git_delta_column
        assert self._gdest_column == self.git_delta_column

        for row in self._iter_rows_with_discovered(
                                       column_index = self._gdest_column.index
                                     , key = 'git-action'):
            gdest_cell = row.cells[self._gdest_column.index]
            common.apply_git_delta_cell(
                          gdest_cell     = gdest_cell
                        , git_delta_cell = gdest_cell )
            if not gdest_cell.decided:
                if gdest_cell.discovered.get('git-action') == 'D' \
                   and _detect_submodule(self.ctx, row, self.fe_commit):
                    continue
            row.p4_request = gdest_cell.decided.p4_request
            row.p4filetype = self._decide_one_p4filetype(
                          row             = row
                        , prev_p4filetype = gdest_cell.discovered.get('type'))

    # -- do -------------------------------------------------------------------

    def _do_p4_sync_k(self, path_list):
        '''
        'p4 sync -k <list>' all the files we need to work on.

        (Later We do call _do_p4_sync_f(), to fetch only the few files that
         Perforce requires us to copy from Perforce.)
        '''
        with Timer(SYNC_K):
            self._p4run_chunked(
                     cmd = [ 'sync'
                           , '-k'   # -k : No touchy local filesystem!
                           , '-q'   # -q : Since we don't use the results for
                           ]        #      anything, don't bother sending 'em'.
                   , arg_list = path_list )

    def _force_local_filesystem_empty(self, timer_name):
        '''
        ### Belongs in outer code, after sync -k but before rest of do().

        Check that there are no files under the Perforce client workspace root.
        If there are, 'rm -rf' them.

        Get any unwanted symlinks out of our way before p4 integ+resove
        might unknowingly use those symlinks as filesystem directories
        and send a file to some random or undefined location by following
        the symlink.
        '''
        with Timer(timer_name):
            p4gf_util.rm_dir_contents(self.ctx.view_dirs.p4root)

    def _requires_sync_f(self, row, col_index_list):
        '''
        What combinations of integ + resolve flags will fail if we don't
        actually copy the file from Perforce to the local filesystem?

        Keep this set as small as possible, because this is all wasted
        network + disk I/O: the file we eventually send to Perforce in
        'p4 submit' comes from Git. We don't _need_ these bytes. We're
        fetching them solely to work around Perforce server rules.
        '''
        # One cannot sync what is not there.
        exists_in_gdest = common.gdest_cell_exists_in_p4(
                                        row.cells[self._gdest_column.index])
        if not exists_in_gdest:
            return False

        for col_index, cell in enumerate(row.cells):
            if not (cell and cell.decided and cell.decided.resolve_flags):
                continue
            if not col_index in col_index_list:
                continue
            for f in ['-ay', '-af']:
                if f in cell.decided.resolve_flags:
                    return True
        return False

    def _do_p4_sync_f(self):
        '''
        Populate local filesystem with files that 'p4 sync -k' did not,
        but which 'p4 resolve' or 'p4 submit' will require later.

        'p4 sync -k' + 'p4 integ' + 'p4 resolve -ay' can leave us with the
        Perforce server thinking we've got the file, but with our local
        filesystem lacking the file, 'p4 submit' correctly fails with:

            open for read: /Users/dir/file.txt: No such file or directory
            Some file(s) could not be transferred from client.

        'p4 integ' = 'p4 resolve -af' can fail with
            open for read: /Users/dir/file.txt: No such file or directory
        '''
        self._do_p4_sync_f_common(self._col_index_list_no_ghost())

    def _ghost_do_p4_sync_f(self):
        '''
        Ghost version of _do_p4_sync_f()

        Populate local filesystem with files that 'p4 sync -k' did not,
        but which 'p4 resolve' or 'p4 submit' will require later.

        Restrict sync to only those files with a ghost integ action.
        '''
        self._do_p4_sync_f_common(self._col_index_list_ghost_only())

    def _do_p4_sync_f_common(self, col_index_list):
        '''
        Populate local filesystem with files that 'p4 sync -k' did not,
        but which 'p4 resolve' or 'p4 submit' will require later.

        Code common to both GHOST and non-GHOST implementation.
        '''
        with Timer(SYNC_F):
            client_path_list = [self.ctx.gwt_path(row.gwt_path).to_client()
                                for row in self.rows_sorted
                                if self._requires_sync_f(row, col_index_list)]
            if not client_path_list:
                common.debug2('_do_p4_sync_f() no files to sync')
                return

            common.debug2('_do_p4_sync_f() ct={ct}', ct=len(client_path_list))
            common.debug3('{l}', l=client_path_list)
            self.ctx.p4run(['sync', '-f'] + client_path_list)

    def _iter_has_integ(self, column):
        '''
        Iterate through all the rows that have integ actions for column,
        skipping those that do not.
        '''
        for row in self.rows_sorted:
            cell = row.cells[column.index]
            if not (    cell
                    and cell.decided
                    and cell.decided.has_integ()):
                continue
            yield row

    IntegResolveKey = namedtuple('IntegResolveKey', ['integ', 'resolve'])
    @staticmethod
    def _to_integ_resolve_key(row, column):
        '''
        Return a key that contains the row's decided integ and resolve action,
        suitable for use in a dict.

        +++ Assumes row already _has_ decided to integ, since we filter all
            that noise out in _iter_has_integ() and I see no point in wasting
            code or CPU time checking for that again.
        '''
        d  = row.cells[column.index].decided
        return G2PMatrix.IntegResolveKey(integ = d.integ_flags, resolve = d.resolve_flags)

    @staticmethod
    def _iter_depot_file(iterable):
        '''
        Return the 'depotFile' value for each element of iterable that has one.
        '''
        for x in iterable:
            if isinstance(x, dict) and 'depotFile' in x:
                yield x['depotFile']


    def _do_integ(self, column, for_delete):
        '''
        If this column contains 1 or more cells with integ actions, and the
        destination Git commit wants this row deleted (for_delete=True) or not
        (for_delete=False), then integ from those cells.

        Collapse multiple integ+resolve actions into one single Perforce request
        to integ+resolve per unique-set-of-flags, using a branch spec to carry
        the from/to depot path pairs.

        Sift through the wreckage after each integ to see what files
        successfully opened for integ and resolved. Those that did not trigger
        either a fallback action, an exception, or nothing if the integ wasn't
        all that important anyway.
        '''
        common.debug2( '_do_integ() col={col} for_delete={for_delete}'
               , col=column.index, for_delete=for_delete )

        with Timer(DO_INTEG):
            # Batch files by decided integ+resolve pair.
            action_to_row = defaultdict(list)
            for row in self._iter_has_integ(column):

                gdest_wants_deleted = False if row.sha1 else True
                if for_delete != gdest_wants_deleted:
                    continue

                action_to_row[self._to_integ_resolve_key(row, column)
                             ].append(row)

            # Perform integ+resolve actions in batches.
            for row_list in action_to_row.values():
                self._do_integ_batch(column=column, row_list=row_list)


                    # pylint:disable=R0913
                    # Too many arguments (6/5)
                    # I don't care.
    def _react_to_any_integ_failure( self
                                   , column
                                   , integ_result_list
                                   , row_list
                                   , integ_error_list ):
        '''
        If 'p4 integ' failed to open all requested rows for integ,
        trigger the Decided.on_integ_failure behavior for failed rows.
        This behavior is often OMG RAISE, so we might not get to every row.

        Bolted onto the side: did we successfully open ANY file for integ?
        If so, return True. If not, return False.
        '''

        # Two similar but for us very different E_WARN warnings returned when
        # 'p4 integ' fails to open ANY files for integ:
        #
        # MsgDm_ExINTEGPEND  "[%argc% - all|All] revision(s) already
        #                     integrated in pending changelist."
        #
        #   ExINTEGPEND means that one of our previous column integ requests
        #   already opened the file for integ and we've nothing more to do here.
        #   That's fine, not an error. Does mean that we might not have anything
        #   to resolve later, so don't raise an exception if that happens.
        #
        # MsgDm_ExINTEGPERM  "[%argc% - all|All] revision(s)
        #                     already integrated."
        #
        #   ExINTEGPERM means that one of the files that we want to integ is not
        #   open for integ, we failed to open it, and we've likely got a
        #   failure, or at least a bug in our decision code. Somehow we decided
        #   that this file needs to be integrated, when in fact it does not.
        #
        # In either case %argc% is rarely useful. Is empty when we use a -b
        # branch mapping.
        #
        # We can get one such message for _each_ command-line arg. So since we
        # call 'p4 integ -b src#rev src#rev src#rev src#rev ...' we will get a
        # flood of these messages. Yet we still might have one or more integ
        # failures. The only way to know for sure is to search for each
        # requested file and see if it shows up as a depotFile in the 'p4 integ'
        # result list.

        #    Any rows that did not succeed? Trigger their failure action
        #    (if any). Often this is OMG RAISE!
        success_depot_file_list = set(df
                             for df in self._iter_depot_file(integ_result_list))

        for row in row_list:
            if not row.depot_path in success_depot_file_list:
                self._react_to_integ_failure( row=row
                                            , column=column
                                            , integ_error_list=integ_error_list)

        if not success_depot_file_list:
            # No files integ'ed means nothing to resolve.
            return False

        # At least one file integrated, thus one file might need resolve.
        # Do run resolve and let P4D figure out if we needed it or not.
        return True

    def _dump_row_gwt(self, gwt, msg=''):
        '''
        If we can find the requested row, dump it.
        '''
        row = self.rows.get(gwt)
        if not row:
            return
        LOG.debug3('_dump_row_gwt() {}'.format(msg))
        self._dump_row(row)

    def _dump_row(self, row, log=LOG):
        '''
        Something has gone wrong with this one row.
        Dump what you know before crashing.

        Logs at ERROR level.
        '''
        log.error('Giant dump of one row:\n{}:'
                 .format('\n'.join(dump(self, one_row=row))))

    def _dump_on_integ_failure(self, human_msg, row, column):
        '''
        Dump state about a specific integ failure.
        '''
        try:
            p4gf_log.create_failure_file('push-')
            with self.ctx.p4.at_exception_level(self.ctx.p4.RAISE_NONE):
                log = logging.getLogger('failures')
                log.error(human_msg)
                log.error('col={}'.format(column.index))

                src_depot_path  = self._integ_src(row, column.index)
                src_rev_range   = self._integ_src_rev_range(row, column.index)

                dest_depot_path = row.depot_path

                cmd = ['fstat'
                      , src_depot_path
                      , '{}#{}'.format(src_depot_path, src_rev_range)
                      , dest_depot_path
                      ]
                log.error('fstat of integ source, source#rev, dest:\np4 {}'
                          .format(' '.join(cmd)))

                            # Call ctx.p4.run() directly rather than ctx.p4run()
                            # to avoid p4run()'s history tracker.
                r = self.ctx.p4.run(cmd)
                log.error('\n{}'.format(pprint.pformat(r)))

                # Re-attempt the failed integ and dump its results.
                cmd = self._to_integ_cmd(row, column)                   \
                    + [ '-n'
                      ,'{}#{}'.format(src_depot_path, src_rev_range)
                      , dest_depot_path
                      ]
                log.error('re-attempting integ just to show failure:\np4 {}'
                          .format(' '.join(cmd)))
                r = self.ctx.p4.run(cmd)
                log.error('\n{}'.format(pprint.pformat(r)))

                # G2P._dump_on_failure() will dump the ENTIRE matrix. But just in case
                # we don't get the point, dump the specific row that caused the failure.
                #
                self._dump_row(row, log=log)

                # pylint:disable=W0703
                # Catching too general exception Exception
        except Exception as e:
            LOG.error(e)

    def _react_to_integ_failure(self, row, column, integ_error_list):
        '''
        We failed to perform the requested integ for row x column.

        Obey the row's Decided.on_integ_failure action.
        '''
        common.debug2('_react_to_integ_failure() col={col} {gwt}'
                .format( col = column.index
                       , gwt = row.gwt_path ))
        if LOG.isEnabledFor(logging.DEBUG2):
            self._dump_row(row)
        decided = row.cells[column.index].decided
        if decided.on_integ_failure == decided.NOP:
            common.debug3('_react_to_integ_failure() NOP {gwt}', gwt=row.gwt_path)
            return

        if decided.on_integ_failure == decided.RAISE:
            human_msg = (
                _('Cannot integrate: p4 -c {change_num} integ -i -t {flags}'
                ' {src} {dest}'
                '\n{integ_error_list}')
                .format(
                  flags      = decided.integ_flags
                , change_num = self.ctx.numbered_change.change_num
                , src        = self._integ_src( row, column.index )
                , dest       = row.depot_path
                , integ_error_list = '\n'.join(integ_error_list)))
            self._dump_on_integ_failure(human_msg, row, column)
            raise RuntimeError(human_msg)

        # Fall back to add/edit/delete.
        assert decided.on_integ_failure == decided.FALLBACK
        assert decided.integ_fallback
        new_req = _max_p4_request(decided.integ_fallback, row.p4_request)
        common.debug3('Integ fallback was={was} fallback={fallback}'
                ' now={now} gwt={gwt}'
               , was      = row.p4_request
               , fallback = decided.integ_fallback
               , now      = new_req
               , gwt      = row.gwt_path )
        self.row_wrapper.set_p4_request(row, new_req)

    @staticmethod
    def _integ_src(row, column_index):
        '''
        Return the appropriate value to use as an integration source
        from this column.
        '''
        return G2PMatrix._first_val( row.cells[column_index].discovered
                                   , ['fromFile', 'depotFile'] )

    def _set_integ_branch_mapping(self, row_list, column):
        '''
        Define a from/to P4.Map, store in our temp branch mapping.
        '''
        p4map = P4.Map()
        for row in row_list:
            p4map.insert( self._integ_src( row, column.index )
                        , row.depot_path)
        self._temp_p4branch.write_map(self.ctx.p4, p4map)

        self.integ_batch_history.append(NTR('p4 temporary branch view assigned:'))
        self.integ_batch_history.extend(p4map.as_array())

        # VERY noisy do not check in
        #if LOG.isEnabledFor(logging.DEBUG3):
        #    LOG.debug3('_set_integ_branch_mapping()\n{m}'
        #              .format(m='\n'.join(p4map.as_array())))

    @staticmethod
    def _first_val(coll, key_list):
        '''
        Return the first value that matches one of the keys.
        '''
        for key in key_list:
            val = coll.get(key)
            if val:
                return val
        return None

    def _integ_src_rev_range(self, row, src_column_index):
        '''
        Return "start,end" or just "end" if the source cell has such.
        '''
        src             = row.cells[src_column_index].discovered
        src_start_rev   = self._first_val(src, ['startFromRev'])

                        # Why 'rev' and not 'endFromRev'?  GF-1418 discovered
                        # that sometimes endFromRev < rev, which yield old
                        # endFromRev = 'delete' file actions that don't match
                        # 'file exists at #rev' inputs to the decision matrix.
        src_end_rev     = self._first_val(src, ['rev'])
        if src_start_rev:
            return NTR('{start},{end}').format( start = src_start_rev
                                              , end   = src_end_rev )
        else:
            return NTR('{end}'        ).format( end   = src_end_rev )

    def _to_integ_dest(self, row, src_column_index):
        '''
        Return dest_path#src_rev or dest_path#src_rev,src_rev

        It sounds wierd, but integ -b destfile#3 will integ
        from destfile's SOURCE revision #3 to destfile.
        '''
        dest_depot_path = row.depot_path
        src_rev_range   = self._integ_src_rev_range(row, src_column_index)
        return NTR('{path}#{src_rev_range}') \
               .format( path          = dest_depot_path
                      , src_rev_range = src_rev_range   )

    ### Move this to context or util. Probably util.

    # What p4run_chunked returns.
    P4RunResult = namedtuple('P4RunResult', [ 'result_list'
                                            , 'error_list'
                                            , 'warning_list'
                                            , 'message_list' ])

    def _p4run_chunked(self, cmd, arg_list):
        '''
        Run Perforce request cmd, over and over, on small chunks of arg_list.

        Sometimes it's faster to run 100 small commands on 100 files each than 1
        big command on 10,000 files. Giant 'p4 integ' requests on 8,000 files
        can take over an hour. Please don't dive into a p4 request for an hour
        without any form of feedback. That's indistinguishable from a hang.

        Concatenate the results and return as a P4RunResult.
        '''
        chunk = arg_list
        p4rr  = self.P4RunResult([], [], [], [])
        p4    = self.ctx.p4

        chunk_size = 100

        chunk      = arg_list[:chunk_size]
        rem_list   = arg_list[chunk_size:]

        while chunk:
            r = self.ctx.p4run(cmd + chunk)

            p4rr.result_list .extend(r)
            p4rr.error_list  .extend(p4.errors)
            p4rr.warning_list.extend(p4.warnings)
            p4rr.message_list.extend(p4.messages)

            chunk    = rem_list[:chunk_size]
            rem_list = rem_list[chunk_size:]

        return p4rr

    @staticmethod
    def _to_integ_cmd(row, column):
        '''
        Return the base of an integ command, including integ flags.
        Omits -b branch name.

        Common code for actually doing integ and for dumping state upon failure.
        '''
        decided   = row.cells[column.index].decided
        return ['integ']                                          \
               + decided.integ_flags.split()                      \
               + ['-3', '-i']

    def _do_integ_batch_chunked(self, column, row_list):
        '''
        Break the integ into smaller chunks with a shorter branch mapping.
        '''
        p4rr = self.P4RunResult([], [], [], [])
        if not row_list:
            return p4rr

        chunk_size = 100

        chunk      = row_list[:chunk_size]
        rem_list   = row_list[chunk_size:]

                # All rows in this list have the same Decided.integ_flags
                # and  Decided.resolve_flags. Fetch any row's copy so that
                # we can use it in all our integ requests.
        integ_cmd = self._to_integ_cmd(row_list[0], column) \
                  + [ '-b', self._temp_p4branch.name]
        p4 = self.ctx.p4            # for less typing

        while chunk:

            # 1. Build a from/to branch mapping that we'll use to drive
            #    a single batch integ.
            self._set_integ_branch_mapping(chunk, column)

            dest_list = [self._to_integ_dest( row              = row
                                            , src_column_index = column.index )
                         for row in chunk ]

            # 2. Perform the 'p4 integ', honoring the flags for these rows.
            with self.ctx.p4.at_exception_level(P4.P4.RAISE_NONE) \
            ,    Timer(DO_INTEG_INTEG):
                r = self.ctx.p4run(integ_cmd + dest_list)

            p4rr.result_list .extend(r)
            p4rr.error_list  .extend(p4.errors)
            p4rr.warning_list.extend(p4.warnings)
            p4rr.message_list.extend(p4.messages)

            self.integ_batch_history.append(integ_cmd + dest_list)
            self.integ_batch_history.extend(p4.errors)
            self.integ_batch_history.extend(p4.warnings)

            chunk    = rem_list[:chunk_size]
            rem_list = rem_list[chunk_size:]

        return p4rr

    def _do_integ_batch(self, column, row_list):
        '''
        One batch integ + resolve.
        '''
        assert column
        assert row_list
        common.debug2( '_do_integ_batch col={col} row_ct={row_ct}'
               , col=column.index, row_ct=len(row_list) )

        p4rr = self._do_integ_batch_chunked(column, row_list)

        # 3. Pore over the results looking for successes.
        #    Trigger Decided.on_integ_failure for un-successes.
        can_resolve = self._react_to_any_integ_failure(
                      column             = column
                    , integ_result_list  = p4rr.result_list
                    , row_list           = row_list
                    , integ_error_list   = p4rr.warning_list + p4rr.error_list )
                # If nothing to resolve, we're done with this batch.
        if not can_resolve:
            return

        # 4. Perform the 'p4 resolve', honoring the flags for these rows.

                # All rows in this list have the same Decided.integ_flags
                # and  Decided.resolve_flags. Fetch any row's copy so that
                # we can use it in all our integ requests.
        a_decided = row_list[0].cells[column.index].decided

                # Why RAISE_ERROR? Failure is not an option here.
                #
                # We cannot cleanly revert just this one failed integ+resolve
                # action on a file without also reverting any previously
                # successful integ+resolve actions pending in this changelist.
        resolve_cmd = ['resolve'] \
                    + a_decided.resolve_flags.split()
        with self.ctx.p4.at_exception_level(P4.P4.RAISE_ERROR) \
        ,    Timer(DO_INTEG_RESOLVE):
            self.ctx.p4run(resolve_cmd)
            # Retain resolve warnings and errors in case we need to report them
            resolve_error_list   = self.ctx.p4.warnings + self.ctx.p4.errors
        self._raise_if_unresolved(resolve_error_list)

        # +++ Remember this integ so that we can use it later as a stopping
        #     point for the Perforce server's search back through history
        #     for unintegrated changes between these two branches.
        self.integrated_up_to.set( from_branch = column.branch
                                 , to_branch   = self.current_branch
                                 , change_num  = column.change_num )

    def _raise_if_unresolved(self, resolve_error_list):
        '''
        If there are any files open for integ that are not fully resolved,
        raise an exception.

        Unfortunately, 'p4 resolve' does not return E_FAILED error messages if
        it fails to resolve all files. So we have to run 'p4 fstat -Ru' (-Ru =
        limit output to files opened that need resolving) to find resolve
        failures.
        '''
        fstat_cmd = [ 'fstat'
                    , '-Ru' # limit output to files opened that need resolving
                    , self.ctx.client_view_path()]
        with self.ctx.p4.at_exception_level(P4.P4.RAISE_ERROR):
            r = self.ctx.p4run(fstat_cmd)
        if not r:
            return

        human_msg = (_('Unable to resolve files after integrate.'
                     '\n{err_list}')
                     .format(err_list = '\n'.join(resolve_error_list)))
        raise RuntimeError(human_msg)

    @staticmethod
    def _better_p4_request(a, b):
        '''
        If b is a better request than a, return b.
        Otherwise keep a.
        '''
        # non-null + null ==> non-nll
        if not b:
            return a
        if not a:
            return b

        # x + x ==> x (no change)
        if a == b:
            return a

        # add + edit ==> edit
        if a == 'add' and b == 'edit':
            return b
        if a == 'edit' and b == 'add':
            return a

        # No other combinations permitted.
        raise RuntimeError(_("BUG: illegal action combination '{}' and '{}'")
                           .format(a, b))

    def _decide_p4_requests_post_do_integ(self):
        '''
        Set each row's row.p4_request to 'add', 'edit', 'delete', or None.

        Each row gets the best request value out of all its individual column
        requests: usually
        * one column holds git_delta_column's converted
          A/M/T/D ==> 'add'/'edit'/'edit'/'delete' actions,
          see _apply_git_delta(),
        * other columns hold any 'p4 integ' actions:
          see Decided.from_integ_matrix_row()
          or fallbacks: see _react_to_any_integ_failure()
        '''
        col_index_list = self._col_index_list_no_ghost()
        for row in self.rows_sorted:
            row.p4_request = self._best_p4_request( col_index_list
                                                  , row
                                                  , row.p4_request )

    def _ghost_decide_p4_requests_post_do_integ(self):
        '''
        Set each row's ghost cell.decided.p4_request to
        'add', 'edit', 'delete', or None.
        '''
        if not self.ghost_column:
            return
        col_index_list = self._col_index_list_ghost_only()
        for row in self.rows_sorted:
            ghost_cell = row.cell_if_col(self.ghost_column)
            if not (ghost_cell and ghost_cell.decided):
                continue
            ghost_cell.decided.p4_request = self._best_p4_request(
                                            col_index_list
                                          , row
                                          , ghost_cell.decided.p4_request )

    def _best_p4_request(self, col_index_list, row, p4_request):
        '''
        Calculate and return one row's 'p4 add/edit/delete' request, taking
        into account any 'p4 integ' action.
        '''
        req = p4_request
        for i, cell in enumerate(row.cells):
            if i not in col_index_list:
                continue

            if not (    cell
                and cell.decided
                and cell.decided.p4_request ):
                continue

            req2 = self._better_p4_request(req, cell.decided.p4_request)
            if req2 == req:
                continue

            if LOG.isEnabledFor(logging.DEBUG3):
                LOG.debug3('_best_p4_request()'
                           ' was={was} now={now} col={col} {gwt}'
                           .format( was = req
                                  , now = req2
                                  , col = i
                                  , gwt = row.gwt_path ))
            req = req2
        return req

    def _existing_p4filetype(self, row):
        '''
        Return the p4filetype of a file that already exists in Perforce.

        Return None for files that do not yet exist, or exist but are
        deleted at head revision.

        Requires that GDEST column be filled in with 'p4 files' info.
        '''
                        # Must already exist in Perforce.
        cell   = row.cells[self._gdest_column.index]
        action = Cell.safe_discovered(cell, 'action')
        if not action:
            return None

                        # Cannot be deleted at head revision.
        if 'delete' in action:
            return None

        return cell.discovered.get('type')

# pylint: disable=R0912
    def _decide_one_p4filetype(self, row, prev_p4filetype):
        '''
        Calculate and return what p4filetype we'd like this row to end up with.
        Take into account what Git expects (file mode) and what Perforce might
        have after any integ+resolve actions have had a chance to propagate
        filetype changes.

        +++ Elsewhere you could clear this value to None for any rows
            that already have the desired p4filetype. This replaces multiple
            'p4 edit -t <p4filetype>' request with a single larger 'p4 edit'
            request.
        '''
        rw = self.row_wrapper       # For less typing.

        # No filetype for files that do not have a file mode in destination
        # commit's git-ls-tree: such files don't exist in this Git, don't get to
        # have a filetype.
        mode = p4gf_util.octal(rw.mode(row))
        if not mode:
            return None

        # Return None for any file being deleted. You don't get to specify a
        # filetype with 'p4 delete'.
        if rw.p4_request(row) == 'delete':
            return None

        if mode == 0o120000:
            return 'symlink'

        # Force Perforce's x bit to match Git's
        if not mode in [ 0o100755, 0o100644 ]:
            raise RuntimeError('_decide_one_p4filetype unexpected row.mode={}'
                               .format( p4gf_util.mode_str(mode)))
        git_x = mode == 0o100755

        # What filetype does Perforce think this file will be?
        if prev_p4filetype:
            p4_result_filetype = prev_p4filetype
        else:
            p4_result_filetype = self._existing_p4filetype(row)

        # If no idea what Perforce filetype will be, return either None (let 'p4
        # add' choose) or '+x' (let 'p4 add' decide, but make whatever it
        # chooses executable).
        if not p4_result_filetype:
            if git_x:
                return NTR('+x')
            else:
                return None

        if p4_result_filetype == 'symlink':
            if p4gf_util.octal(row.mode) == 0o100755:
                p4_result_filetype = 'xtext'
            else:
                p4_result_filetype = 'text'
            return p4_result_filetype

        # If Perforce filetype does not match Git's x-bit, make it match.
        bm = p4gf_p4filetype.to_base_mods(p4_result_filetype)
        p4_x = 'x' in bm[1:]
        if p4_x != git_x:
            if git_x:       # add +x
                return p4gf_p4filetype.from_base_mods(bm[0], bm[1:] + ['x'])
            else:           # remove +x
                mods = bm[1:]
                mods.remove('x')
                return p4gf_p4filetype.from_base_mods(bm[0], mods)


        # Perforce filetype does not need to change.
        return p4_result_filetype
# pylint: enable=R0912

    def _decide_p4filetypes_post_do_integ(self):
        '''
        What shall we pass as -t <filetype> for our add/edit/delete actions?

        Filetype stored in Row.p4filetype.

        _decide_p4filetypes_post_do_integ() must run AFTER all _do_integ()
        commands, because our 'p4 integ -t ...' requests might propagate a
        filetype change, or might not, depending on integration history that the
        Perforce server sees and calculates. We cannot easily reproduce that
        same knowledge and code within Git Fusion.
        '''
        common.debug2('_decide_p4filetypes_post_do_integ')

        # 1. Fetch the results of any integ actions that might or might not
        #    have propagated a filetype change. Also picks up Perforce filetype
        with self.ctx.p4.at_exception_level(P4.P4.RAISE_NONE):
                    # RAISE_NONE: don't let empty branches raise exception on
                    # [Warning]: '//git-fusion-p4gf_repo/... - no such file(s).'
            r = self.ctx.p4run([ 'fstat'
                               , '-Or'              # -Or pending integ info.
                               , '-TdepotFile,type' # -T  Don't need the whole
                                                    # world, just a couple
                                                    # pieces of info.
                               , self.ctx.client_view_path()])
        depot_path_to_type = { rr['depotFile'] : rr['type']
                                for rr in r
                                if (    isinstance(rr, dict)
                                    and 'depotFile' in rr
                                    and 'type'      in rr )  }

        # 2. Feed that integ'ed type, plus any input from Git, into a decision.
        for row in self.rows.values():
            self.row_wrapper.set_p4filetype(
                  row
                , self._decide_one_p4filetype(
                     row = row
                   , prev_p4filetype = depot_path_to_type.get(row.depot_path)))

    def _how_to_local_git_diff_one(self, row):
        '''
        Given a row, how should we modify its local file and open it in
        Perforce to counteract any difference from what Git expects for
        this commit?

        Requires that row has already been 'p4 sync -f'ed and
        'p4 integ' + 'p4 resolve'ed.

        Return one of:
        None     : no diff
        'add'    : local file missing.
        'edit'   : local file does not match Git's record
                   Does not include filetype mismsatch
        'delete' : local file exists but Git lacks a record for this file.
        '''
        # Skip empty commit placeholder (very rare).
        if row.gwt_path == p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER:
            return None

        # Skip any file that we've not already opened for integ. Such files are
        # not 'p4 sync'ed to local filesystem. Any diff calculations would be
        # against the previous revision in P4, which match revisions in previous
        # Git commits, and thus are already factored into git-fast-export or
        # git-diff-tree.
        if not row.has_p4_action():
            return

        git_exists   = row.sha1 and row.mode
        local_path   = self.ctx.gwt_path(row.gwt_path).to_local()
        # Avoid 'True' for a path which contains a symlink in its path
        # otherwise it appears as a file which should not be present
        # _gdest_symlinks entries each contain a trailing slash
        # thus a filename which itself is a symlink and is not parented
        # by a symlink will not be found in symlink_in_path
        if self.symlink_in_path(row.gwt_path):
            local_exists = False
        elif os.path.isdir(local_path) and not os.path.islink(local_path):
            local_exists = False
        else:
            local_exists = os.path.lexists(local_path)


        # Not in git or local? Nothing to diff.
        if (    (not git_exists)
            and (not local_exists)):
            return None

        # Not in destination Git commit? Delete or NOP. But one of the big
        # design requirements  for this row x column matrix is to never
        # integ+resolve into existence a file, neither locally or in pending
        # changelist, that is to be deleted. Such files must either remain
        # unintegrated, or integrated for delete.
        if local_exists and not git_exists:
            raise RuntimeError(_("BUG: p4 integ+resolve created"
                               " local file '{local_path}' that does not exist in"
                               " Git commit {commit_sha1}")
                               .format( local_path  = local_path
                                      , commit_sha1 = self.fe_commit['sha1']))

        # Exists in Git but not local filesystem.
        if not local_exists:
            return 'add'

        # Exists in both Git and local. Do they match?

        # If local file is polluted with RCS keyword expansion, strip those
        # out before calculating its sha1 otherwise we'll get unnecessary diffs,
        # unnecessary 'p4 edit' requests.

        local_sha1 = p4gf_util.git_hash_object(local_path)
        if local_sha1 != row.sha1:
            return 'edit'

        # Exist in both and sha1s match. No need to edit or overwrite
        # with content from Git.
        return None

    def _set_p4_requests_for_local_git_diffs(self):
        '''
        Any file missing or holds content different from what Git
        wants for this commit?

        Mark rows for add/edit/delete.

        Delete files that differ by content.
        '''
        for row in self.rows_sorted:
            how = self._how_to_local_git_diff_one(row)
            if not how:
                continue

            # Schedule add/edit/delete.
            row.p4_request = self._better_p4_request(row.p4_request, how)

            # Differing content no good, get rid of it.
            local_path = self.ctx.gwt_path(row.gwt_path).to_local()
            if os.path.lexists(local_path):
                p4gf_util.unlink_file_or_dir(local_path)

    def _exists_undeleted_at_head_gdest(self, row):
        '''
        Does this file already exist in Perforce, and not deleted?

        If so, then you cannot 'p4 add' it. You can 'p4 edit' or 'p4 delete' it.
        '''
        gdest = row.cells[self._gdest_column.index]
        if not gdest:
            return False
        if not gdest.discovered:
            return False

        for key in ['action', 'headAction']:
            action = gdest.discovered.get(key)
            if action and 'delete' not in action:
                return True
        return False

    def _copy_file_from_git(self, row, blob_sha1):
        '''
        Copy one file from Git's internal file blob to correct local_path
        for eventual 'p4 add/edit' + 'p4 submit'.

        NOP for rows that are not going to be add/edit/reopen-ed.
        '''
        common.debug2('_copy_file_from_git      {row}', row=row)
        # Not in Git?
        if not blob_sha1:
            return
        # Not scheduled for add, edit, or reopen?
        if self.row_wrapper.p4_request(row) not in ['add', 'edit', None]:
            return
        if self.row_wrapper.p4_request(row) == None:# and row.p4filetype == None:
            return

        # Local file already exists? Get it out of our way.
        # It _might_ have the correct content and file mode, but
        # in some cases (such as changing filetype from text to symlink)
        # we cannot just reuse the existing file.
        local_path = self.ctx.gwt_path(row.gwt_path).to_local()
        if os.path.lexists(local_path):
            p4gf_util.unlink_file_or_dir(local_path)

        p4gf_util.ensure_parent_dir(local_path)
        common.debug2('_copy_file_from_git copy {row}', row=row)
        p4gf_git.cat_file_to_local_file( self.row_wrapper.sha1(row)
                                       , self.row_wrapper.p4filetype(row)
                                       , local_path )

    def _copy_files_from_git(self):
        '''
        Copy files from Git's internal file blobs to local Perforce workspace
        from which we'll p4 add/edit/reopen them into Perforce.
        '''
        with Timer(DO_COPY_FILES_FROM_GIT):
            for row in self.rows_sorted:
                self._copy_file_from_git(row, row.sha1)

    def _ghost_copy_files_from_git(self):
        '''
        Copy files from Git's internal file blobs to local Perforce workspace
        from which we'll p4 add/edit/reopen them into Perforce.
        '''
        if not self.ghost_column:
            return

        for row in self.rows_sorted:
            cell = row.cell_if_col(self.ghost_column)
            if not (cell and cell.discovered):
                continue

                        # Cannot get content from Git, it's already deleted
                        # at GPARN/GHOST.
            if cell.decided and cell.decided.add_delete:
                continue

            self._copy_file_from_git( row
                                    , cell.discovered.get('sha1') )

    def _ghost_add_for_delete(self):
        '''
        Create empty placeholder files that we can add for a later delete.
        '''
        if not self.ghost_column:
            return

        for row in self.rows_sorted:
            cell = row.cell_if_col(self.ghost_column)
            if (not (    cell
                     and cell.discovered
                     and cell.decided
                     and cell.decided.add_delete )):
                continue

            local_path = self.ctx.gwt_path(row.gwt_path).to_local()
            p4gf_util.ensure_parent_dir(local_path)
            with open(local_path, 'w') as f:
                f.write("")

    def _ghost_redecide_add_for_delete(self):
        '''
        Change the p4action of any add_delete file from 'add' to 'delete'.
        '''
        if not self.ghost_column:
            return

        for row in self.rows_sorted:
            cell = row.cell_if_col(self.ghost_column)
            if (not (    cell
                     and cell.discovered
                     and cell.decided
                     and cell.decided.add_delete )):
                continue

            cell.decided.p4_request = 'delete'

    def _batch_add_edit_delete_to_cmd(self, p4_request, p4filetype, row_list):
        '''
        Run a single 'p4 <request> -t <p4filetype> file1 file2 ... filen'
        '''
        cmd = [p4_request]
        if p4_request == 'add':
            # 'p4 add' wants local filesystem syntax, with evil @#%* chars
            #  unescaped. Flag -f to permit evil @#%* chars.
            cmd.append('-f')
            path_list = [self.ctx.gwt_path(row.gwt_path).to_local()
                         for row in row_list]
        else:
            # We don't need Perforce touching our local files, thankyouverymuch.
            cmd.append('-k')

            # 'p4 edit' and 'p4 delete' prefer depot syntax, with
            # evil @#%* chars escaped.
            path_list = [row.depot_path for row in row_list]

        if p4filetype:
            assert p4_request != 'delete'
            cmd.extend(['-t', p4filetype])

        return cmd + path_list

    def _do_batches_add_edit_delete(self):
        '''
        For each row that has an add/edit/delete action, perform it.

        Collapse multiple identical actions into one single Perforce request
        to add/edit/delete a list of files.

        Does NOT factor in filetype. We'll reopen -t later.
        '''
        with Timer(DO_BATCH_ADD_EDIT_DELETE):
            LOG.debug('_do_batches_add_edit_delete()')

            # Used internally as a key when bucketizing rows
                            # pylint:disable=C0103
                            # Invalid name "RequestFiletype" for type variable
                            # It's a type name, not a variable.
                            # Pylint does not understand namedtuple.
            RequestFiletype = namedtuple( 'RequestFiletype'
                                        , ['p4_request', 'p4filetype'])
                            # pylint:enable=C0103

            reopen = defaultdict(list)

            # Bucketize
            req_to_rows = defaultdict(list)
            for row in self.rows_sorted:
                row_p4_request = self.row_wrapper.p4_request(row)
                row_p4filetype = self.row_wrapper.p4filetype(row)
                row_has_integ  = self.row_wrapper.has_integ (row)

                if not row_p4_request:
                    continue

                if (    row_p4_request == 'edit'
                    and row_p4filetype
                    and (   row_has_integ
                         or row_p4filetype == 'symlink')):
                    key = RequestFiletype('edit', None)
                    reopen[row_p4filetype].append(row)
                else:
                    key = RequestFiletype( p4_request = row_p4_request
                                         , p4filetype = row_p4filetype )

                        # It is a programming error to assign a p4_request to a
                        # row that does not carry both Git work tree and
                        # Perforce depot paths.
                if not (row.gwt_path or row.depot_path):
                    msg = _('BUG: row has p4_request but lacks both paths:\n{}') \
                          .format(row.to_log_level(logging.DEBUG3))
                    raise RuntimeError(msg)

                req_to_rows[key].append(row)

            for key, path_list in req_to_rows.items():
                # Perforce server requires that any file for 'edit' or 'delete'
                # first be in our 'have' list.
                if key.p4_request != 'add':
                    self._do_p4_sync_k([row.depot_path for row in path_list])

                cmd = self._batch_add_edit_delete_to_cmd( key.p4_request
                                                        , key.p4filetype
                                                        , path_list)
                common.debug2('_do_batches_add_edit_delete() {}', cmd)
                r = self.g2p._p4run(cmd, bulldoze=True, revert=True)
                self.g2p._handle_unicode(r)

            for p4filetype, row_list in reopen.items():
                cmd =   [ 'reopen', '-t', p4filetype ] \
                      + [ row.depot_path for row in row_list ]
                common.debug2('_do_batches_add_edit_delete() {}', cmd)
                r = self.g2p._p4run(cmd, bulldoze=True, revert=False)

    def columns_to_log_level(self, level):
        '''Debugging dump.'''
        if level <= logging.DEBUG2:
            return '\n'.join([col.to_log_level(level)
                              for col in self.columns])
        else:
            return ' '.join([col.col_type for col in self.columns])

    def _ghost_do_rearrange(self):
        '''
        If we have a P4IMPLY column, request whatever Perforce file actions
        are necessary to rearrange our current branch to look exactly like
        our Git first-parent.
        '''
        pass


    def _ghost_submit(self):
        '''
        If we have any Perforce file actions to submit to our ghost
        changelist, do so. If not, let the ghost NumberedChangelist
        delete itself upon its __exit__().
        '''
        r = self.ctx.p4run(['opened', '-m', '1'])
        if not r:
            LOG.debug('_ghost_submit() nothing to submit. Was @{}'
                      .format(self.ctx.numbered_change.change_num))
            return

        try:
            LOG.debug('_ghost_submit() submitting @{}'
                      .format(self.ctx.numbered_change.change_num))
            changenum = self.g2p._p4_submit_x(
                                     desc      = _('Ghost changelist')
                                   , owner     = p4gf_const.P4GF_USER
                                   , sha1      = None
                                   , branch_id = self.current_branch.branch_id
                                   , gsreview  = None )
            LOG.debug('_ghost_submit() submitted @{}'.format(changenum))
        except P4.P4Exception as e:
            self.g2p._revert_and_raise(str(e), is_exception = True)

    def _col_index_list_no_ghost(self):
        '''
        Return a list of column indices, skipping any GHOST column.

        Suitable for use as a parameter to functions that
        iterate through columns or cells, but you want to
        skip over the GHOST column.
        '''
        col_index_list = list(range(0, len(self.columns)))
        if self.ghost_column:
            col_index_list.remove(self.ghost_column.index)
        return col_index_list

    def _col_index_list_ghost_only(self):
        '''
        Return a list containing only the GHOST column index,
        or empty list if no ghost column.

        Suitable for use as a parameter to functions that
        iterate through columns or cells, but you want to
        only process the GHOST column.
        '''
        if self.ghost_column:
            return [self.ghost_column.index]
        else:
            return []


# RowDecider moved to p4gf_g2p_matrix2_row_decider.py

# -- RowWrapper ---------------------------------------------------------------

                        # pylint:disable=C0111
                        # Missing method docstring
                        # Yeah, these are simple pass-throughs, a docstring
                        # would just add clutter.
class RowWrapper:
    '''
    Accessors for non-GHOST operations.

    RowWrapper and GhostRowWrapper allow the same function to work on both
    GHOST and non-GHOST operations.
    GHOST operations read and write the Row's GHOST column.
    Non-GHOST operations read and write the Row directly.
    '''
    def __init__(self):
        pass

    @staticmethod
    def p4_request(row):
        return row.p4_request

    @staticmethod
    def set_p4_request(row, val):
        row.p4_request = val

    @staticmethod
    def p4filetype(row):
        return row.p4filetype

    @staticmethod
    def set_p4filetype(row, val):
        row.p4filetype = val

    @staticmethod
    def has_integ(row):
        return row.has_integ()

    @staticmethod
    def sha1(row):
        return row.sha1

    @staticmethod
    def mode(row):
        return row.mode

class GhostRowWrapper:
    '''
    Accessors for GHOST operations.

    Read and write only GHOST cell.
    '''
    def __init__(self, ghost_col):
        self.ghost_col = ghost_col

    def _decided(self, row):
        cell = row.cell_if_col(self.ghost_col)
        if cell:
            return cell.decided
        else:
            return None

    def _discovered(self, row):
        cell = row.cell_if_col(self.ghost_col)
        if cell:
            return cell.discovered
        else:
            return None

    def p4_request(self, row):
        d = self._decided(row)
        if d:
            return d.p4_request
        else:
            return None

    def set_p4_request(self, row, val):
        d = self._decided(row)
        if d:
            d.p4_request = val
        else:
            row.cell(self.ghost_col.index).decided = Decided(p4_request = val)

    def p4filetype(self, row):
        d = self._decided(row)
        if d:
            return d.ghost_p4filetype
        else:
            return None

    def set_p4filetype(self, row, val):
        d = self._decided(row)
        if d:
            d.ghost_p4filetype = val
        else:
            row.cell(self.ghost_col.index).decided = Decided(ghost_p4filetype = val)

    def has_integ(self, row):
        d = self._decided(row)
        if d:
            return d.has_integ()
        else:
            return False

    def sha1(self, row):
        d = self._discovered(row)
        if d:
            return d.get('sha1')
        else:
            return False

    def mode(self, row):
        d = self._discovered(row)
        if d:
            return d.get('git-mode')
        else:
            return False

                        # pylint:enable=C0111


# -- module-wide functions ----------------------------------------------------


def _max_p4_request(a, b):
    '''
    Return the correct add/edit/delete action of two possible requests.

    'add' can convert to 'edit'.
    It is a bug to request 'delete' and anything other than 'delete'.
    '''
    # Get NULLs and no-change out of the way.
    if not b:
        return a
    if not a:
        return b
    if a == b:
        return a

    # Differing non-None actions.

    # Can't combine 'delete' with anything.
    if (a == 'delete') or (b == 'delete'):
        raise RuntimeError(_("Bug: conflicting actions 'p4 {a}' vs. 'p4 {b}'")
                           .format(a, b))

    return 'edit'

def _detect_submodule(ctx, row, commit):
    '''
    Detect if the given row is the deletion of a submodule, which is typically
    hard to detect without having a parent commit's tree entry mode available.
    This function should be called when a row is undecided and its git-action
    is 'D' (according to git-fast-export).

    If this is the case, the commit object is modified such that the gitlinks
    list will have a new entry, and True is returned; otherwise the commit is
    not altered and False is returned.

    Arguments:
        ctx -- Git Fusion context
        row -- matrix row
        commit -- commit object from FastExport
    '''
    for parent in ctx.view_repo[commit['sha1']].parents:
        ent = p4gf_util.git_ls_tree_one(ctx.view_repo, parent.hex, row.gwt_path)
        if ent and ent.mode == '160000':
            # Mark this commit with the submodule/gitlink deletion
            links = commit.setdefault('gitlinks', [])
            links.append((p4gf_const.NULL_COMMIT_SHA1, row.gwt_path))
            LOG.debug2("_detect_submodule(): detected submodule {}".format(row.gwt_path))
            return True
    return False
