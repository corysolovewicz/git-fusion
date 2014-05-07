#! /usr/bin/env python3.3
'''
A single row in a G2PMatrix, including each cell's data.
'''
import logging

from   p4gf_g2p_matrix2_cell    import G2PMatrixCell as Cell
from   p4gf_l10n                import NTR
import p4gf_util

LOG = logging.getLogger('p4gf_g2p_matrix2').getChild('row')  # subcategory of G2PMatrix.

class G2PMatrixRow:
    '''
    A single file's sources of change, and chosen actions to apply that
    change to Perforce.
    '''
                # pylint:disable=R0913
                # Too many arguments
                # This is intentional. I prefer to fully construct an instance
                # with a single call to an initializer, not construct, then
                # assign, assign, assign.

    def __init__( self
                , gwt_path   = None
                , depot_path = None
                , sha1       = None # file/blob sha1, not commit sha1
                , mode       = None
                , col_ct     = 0
                ):
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('Row(): gwt={} depot={} sha1={} mode={} col_ct={}'
                      .format( gwt_path, depot_path, p4gf_util.abbrev(sha1)
                             , p4gf_util.mode_str(mode), col_ct))

        if gwt_path:        # Caller must supply both if supplying GWT.
            assert depot_path

                # Destination/result data. What git-fast-export gives us, or
                # what we decide based on cross-branch integrations.
        self.gwt_path       = gwt_path

                # Destination depot path, calculated via current branch view
                # mapping. Caller supplies.
        self.depot_path     = depot_path

                # file sha1 and mode copied from initial git-fast-export or
                # git-ls-tree. Left None if Git has no record of this gwt_path
                # at this commit.
        self.sha1           = sha1
        self.mode           = mode      # int, not string
        if mode:
            assert isinstance(mode, int)

                # Same integer indices as G2PMatrix.columns
        self.cells          = [None] * col_ct

                # One of [None, 'add', 'edit', 'delete'] chosen from
                # all cells plus any difference from Git.
                #
                # Set during _react_to_integ_failure() upon integ failure.
                # Set during _decide_p4_requests_post_do_integ() to pull the
                # winning Decided.p4_request out of this row's cells.
                # Set during _set_p4_requests_for_local_git_diffs()
                # if local filesystem content does not match what Git requires.
                #
        self.p4_request     = None

                # The one true filetype chosen from Git's mode
                # and x bits and existing Perforce filetype.
        self.p4filetype    = None

    def __repr__(self):
        return ('Row: {sha1:<7} {mode:<6} {p4_request:<6} {p4filetype:<7}'
                ' {gwt_path:<20} {depot_path}'
                .format( sha1       = p4gf_util.quiet_none(
                                      p4gf_util.abbrev(    self.sha1))
                       , mode       = p4gf_util.mode_str(  self.mode)
                       , p4_request = p4gf_util.quiet_none(self.p4_request)
                       , p4filetype = p4gf_util.quiet_none(self.p4filetype)
                       , gwt_path   =                      self.gwt_path
                       , depot_path =                      self.depot_path))

    def cell(self, index):
        '''
        Return the requested cell.

        Create, insert, then return a new cell if we've not yet populated
        that cell.

        Does NOT extend cell list, you should have handled that at initializer
        time with a correct col_ct.
        '''
        if not self.cells[index]:
            self.cells[index] = Cell()
        return self.cells[index]

    def cell_if_col(self, column):
        '''
        Return the requested cell if exists, None if not.
        '''
        if not column:
            return None
        return self.cells[column.index]

    def has_p4_action(self):
        '''
        Does this row hold any decided p4 integ or p4 add/edit/delete request?
        '''
        if self.p4_request:
            return True
        for cell in self.cells:
            if cell and cell.decided and cell.decided.has_p4_action():
                return True
        return False

    def to_log_level(self, level):
        '''Debugging dump.'''

        # Single line dump
        fmt = NTR('Row: {sha1:<7} {mode:<6} {p4_request:<6} {p4filetype:<10}'
               ' {gwt_path:<10} {depot_path:<10}')

        topline = fmt.format(
                           sha1       = p4gf_util.abbrev(self.sha1) \
                                        if self.sha1 else '0000000'
                         , mode       = p4gf_util.quiet_none(
                                        p4gf_util.mode_str(  self.mode))
                         , gwt_path   = self.gwt_path
                         , depot_path = self.depot_path
                         , p4_request = p4gf_util.quiet_none(self.p4_request)
                         , p4filetype = p4gf_util.quiet_none(self.p4filetype)
                         )

                # Detail each cell at DEBUG2 not DEBUG3. DEBUG2 produces one-
                # line dumps for each cell, which should be useful. DEBUG3 will
                # produce multi-line dumps of each cell, which is VERY noisy.
        if level <= logging.DEBUG2:
            # Multi-line dump.
            lines = [ topline ]
            for i, cell in enumerate(self.cells):
                if not cell:
                    lines.append(NTR('  {i}: {cell}').format(i=i, cell=cell))
                else:
                    lines.append(NTR('  {i}: {cell}')
                            .format( i=i
                                   , cell=cell.to_log_level(level)))
            return '\n'.join(lines)
        else:
            return topline

    def exists_in_git(self):
        '''
        Does this file exist in the destination Git commit? Have we discovered
        and recorded a blob sha1 and file mode for this row?
        '''
        return self.sha1 and self.mode

    def has_integ(self):
        '''Do we have a request to integrate?'''
        for cell in self.cells:
            if cell and cell.decided and cell.decided.has_integ():
                return True
        return False


