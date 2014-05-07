#! /usr/bin/env python3.3
'''
Information about a column in G2PMatrix.

Moved to a separate file solely to make it easier to edit without a
lot of scrolling.
'''
import p4gf_branch
import p4gf_depot_branch
from   p4gf_l10n    import NTR
import p4gf_util

class G2PMatrixColumn:
    '''
    A source of change that contributes to a Git commit.
    '''

            # Values for col_type

            # Git destination intersection with current Perforce branch view.
            #
            # Receives this Git commit.
            # .change_num set to previous changelist (if any) on this
            # Perforce branch.
            #
            # cell.discovered:
            #   git-action : from git-fast-export
            #   mode       : from git-ls-tree
            #                Currently ALL modes recorded, might thin down
            #                to just symlink=120000 if that's all we need.
            #   p4 files //client/...@n
            #
    GDEST = 'GDEST'

            # One Git parent commit, intersected with one Perforce branch view.
            #
            # Each Git parent may intersect multiple Peforce branches, but we
            # retain only the most recent (highest changelist number) column
            # for any single branch, so a single Branch appears no more than
            # once in our GPARN list.
            #
            # cell.discovered:
            #                   If populating first changelist on new branch:
            #   p4 copy -n -b <tmp> //...@n
            #   p4 files //client/...@n
            #
            #                   Always
            #   p4 integ -n -t -d -i -b <tmp> //...@n
            #   p4 files //client/...@n
            #
    GPARN = 'GPARN'

            # One Git parent commit, intersected with one Perforce branch view,
            # if a counterpart GPARN column is lightweight and has a
            # fully-populated basis. This is that basis.
            #
            # (Some lightweight GPARN branches lack a fully populated basis,
            #  so don't expect one GPARFPN for each lightweight GPARN.)
            #
            # Often this is identical to P4JITFP, since a lightweight branch
            # uses the same fully populated basis as its first parent.
            #
            # cell.discovered:
            #                   If populating first changelist on new FP branch:
            #   p4 copy -n -b <tmp> //...@n
            #   p4 files //client/...@n
            #
    GPARFPN = 'GPARFPN'

            # Fully populated basis for Just-in-Time branch actions.
            #
            # If destination branch is lightweight, then this column tells from
            # where to integ files that have not yet been populated into the
            # destination branch.
            #
            # Discovery is what files COULD be JIT-branched if necessary.
            #
            # cell.discovered:
            #   p4 files //client/...@n
            #
    P4JITFP = 'P4JITFP'

            # Implied parent on same Perforce branch.
            #
            # If the previous Perforce changelist on GDEST's Perforce branch
            # does not correspond to any Git parent GPARN, then list that
            # previous Perforce changelist here as an implicit parent.
            # Any integ action here is invoked ONLY if there are no other
            # integ actions AND we decide to add/edit/delete.
            #
            # cell.discovered:
            #   git-action : git-diff-tree p4imply's-commit.sha1
            #                gdest's-commit.sha1
            #   mode       : git-ls-tree
            #
    P4IMPLY = 'P4IMPLY'


            # A Perforce changelist we create and submit in order to stage
            # GDEST's depot branch to receive the pending Git commit.
            #
            # Also contains integ (branch) actions to perform the first half of
            # JIT-branch-for-delete, and integ (branch) actions to populate the
            # first commit of a new depot branch.
            #
            # Column header data is same as Git first-parent column, if one
            # exists, or empty/None if the current Git commit is a root/orphan
            # with no parent commit.
            #
            # cell.discovered
            #   git-action : git-diff-tree to convert p4imply's state
            #                to GPARN first-parent's state.
            #
            #   git-mode   : file mode and sha1 of file in GPARN, aka
            #   sha1       : ...the commit this GHOST is attempting to
            #                ...recreate
            #
    GHOST   = 'GHOST'

            # A short reminder of what appears in which p4 result dict:
            #
            # p4 files:
            #   ... depotFile //depot/main/about-this-folder.txt
            #   ... rev 2
            #   ... change 566664
            #   ... action delete
            #   ... type text
            #   ... time 1355530980
            #
            # p4 integ -n -t -i -d -b <tmp> //...@n
            # p4 copy  -n          -b <tmp> //...@n
            #   ... depotFile //depot/main/about-this-folder.txt
            #   ... clientFile /Users/zig/p4/depot/main/about-this-folder.txt
            #   ... workRev 1
            #   ... action branch
            #   ... ... otherAction sync
            #   ... fromFile //depot/dev/git-fusion/bin/about-this-folder.txt
            #   ... startFromRev none
            #   ... endFromRev 2

    # pylint:disable=R0913
    # Too many arguments
    # This is intentional. I prefer to fully construct an instance with a
    # single call to an initializer, not construct, then assign, assign, assign.

    def __init__( self
                , col_type          = None
                , branch            = None
                , depot_branch      = None
                , sha1              = None
                , change_num        = 0
                , is_first_parent   = False
                , fp_counterpart    = None
                ):
        assert isinstance(change_num, int)

        self.col_type   = col_type

                    # If fully populated basis GPARFPN or P4JITFP, then branch
                    # is the lightweight branch for which we serve as a basis,
                    # not the fully populated branch (which is often None).
        self.branch         = branch

                    # Where this lightweight branch's files live.
                    # None if this is a fully populated branch, or if this is
                    # the GPARFPN fully populated basis for some other
                    # lightweight GPARN branch.
        self.depot_branch   = depot_branch

        self.sha1           = sha1
        self.change_num     = change_num

                    # +++ index within G2PMatrix.columns, lets us more quickly
                    #     index into G2PMatrixRow.cells[] or
                    #     G2PMatrix.columns[].
                    #     Set at end of G2PMatrix._discover_branches() when we
                    #     fill in G2PMatrix.columns[].
        self.index          = -1

                    # Is this a GPARN column that corresponds to Git's
                    # first-parent commit?
        self.is_first_parent    = is_first_parent

                    # Latches True in G2PMatrix._files_at()
        self.discovered_p4files = False

                    # If we are a GPARN column, what is our FP basis (if any?)
                    # If we are a GPARFPN column, what lightweight GPARN uses
                    # us as a basis? (We never share a single GPARFPN across
                    # multiple GPARNs)
        self.fp_counterpart     = fp_counterpart
        if fp_counterpart:
            fp_counterpart.fp_counterpart = self

    def to_log_level(self, _level):
        '''Debugging dump.'''

        # Single-line dump
        fmt = (NTR('{index}: {col_type:<7} b={branch:<7} d={dbi:<7}'
                   ' {sha1:<7} ch={ch:>4} {first_parent:<9} {fp_counterpart}'))
        fp_counterpart  = (NTR(' fp_counter=[{i}]')
                           .format(i=self.fp_counterpart.index)
                           if self.fp_counterpart else '')
        return fmt.format(
                  index          = self.index
                , col_type       = self.col_type
                , branch         = p4gf_branch.abbrev(self.branch)
                , dbi            = p4gf_depot_branch.abbrev(self.depot_branch)
                , sha1           = p4gf_util.abbrev(self.sha1)
                , ch             = self.change_num
                , first_parent   = 'first-par' if self.is_first_parent else ''
                , fp_counterpart = fp_counterpart
                )

    @staticmethod
    def count(lst, col_type):
        '''
        How many GPARN columns do we have?
        '''
        ct = 0
        for c in lst:
            if c.col_type == col_type:
                ct += 1
        return ct

    @staticmethod
    def find(lst, col_type):
        '''Seek.'''
        for c in lst:
            if c.col_type == col_type:
                return c
        return None

    @staticmethod
    def find_fp_basis(lst, gparn):
        '''
        If gparn is lightweight, it (usually) has a corresponding GPARFPN
        column somewhere. Return that. Return None if no FP basis.

        DO NOT USE WITH MATRIX 2
        Matrix 2 stores GPARFPN's actual sha1, not a copy of GPARN's sha1.
        '''
        if not gparn.branch.is_lightweight:
            return None
        for c in lst:
            if (    (c.col_type == G2PMatrixColumn.GPARFPN)
                and (c.branch   == gparn.branch)
                and (c.sha1     == gparn.sha1)):
                return c
        return None

    @staticmethod
    def of_col_type(lst, col_type):
        '''Subset of lst with matching col_type. Stable ordering.'''
        return [c for c in lst if c.col_type == col_type]

    @staticmethod
    def find_first_parent(lst):
        '''Return the GPARN column that corresponds to our Git first-parent.'''
        for col in lst:
            if col.is_first_parent and col.col_type == G2PMatrixColumn.GPARN:
                return col
        return None


