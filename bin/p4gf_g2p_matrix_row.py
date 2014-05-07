#! /usr/bin/env python3.3
'''
A single row in a G2PMatrix, including each cell's data.
'''
import logging
import pprint

                        # Avoid import cycles.
                        # Do not import common here.

import p4gf_g2p_matrix_integ
from   p4gf_l10n    import NTR
import p4gf_util

LOG = logging.getLogger(__name__).getChild('row')  # subcategory of G2PMatrix.

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
                             , _mode_str(mode), col_ct))

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
                .format( sha1       = _quiet_none(
                                      p4gf_util.abbrev( self.sha1))
                       , mode       = _mode_str(        self.mode)
                       , p4_request = _quiet_none(      self.p4_request)
                       , p4filetype = _quiet_none(      self.p4filetype)
                       , gwt_path   =                   self.gwt_path
                       , depot_path =                   self.depot_path))

    def cell(self, index):
        '''
        Return the requested cell.

        Create, insert, then return a new cell if we've not yet populated
        that cell.

        Does NOT extend cell list, you should have handled that at initializer
        time with a correct col_ct.
        '''
        if not self.cells[index]:
            self.cells[index] = G2PMatrixCell()
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
                         , mode       = _quiet_none(_mode_str(self.mode))
                         , gwt_path   = self.gwt_path
                         , depot_path = self.depot_path
                         , p4_request = _quiet_none(self.p4_request)
                         , p4filetype = _quiet_none(self.p4filetype)
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

# =============================================================================

class Decided:
    '''
    What we've decided to do.
    '''

    # If integ fails to open a file for integ, do what?
    NOP      = NTR('NOP')
    RAISE    = NTR('RAISE')
    FALLBACK = NTR('FALLBACK')

    # Debugging dump strings and keys for asserting legal value.
    ON_INTEG_FAILURE = [ NOP
                       , RAISE
                       , FALLBACK ]


                # pylint:disable=R0913
                # Too many arguments
                # This is intentional. I prefer to fully construct an instance
                # with a single call to an initializer, not construct, then
                # assign, assign, assign.

    def __init__( self
                , integ_flags        = None
                , resolve_flags      = None
                , on_integ_failure   = RAISE
                , integ_fallback     = None
                , p4_request         = None
                , integ_input        = None
                ):

        assert(   integ_flags == None
               or on_integ_failure in self.ON_INTEG_FAILURE)

        # Must specify a fallback when specifying to _use_ a fallback.
        if on_integ_failure == self.FALLBACK:
            assert integ_fallback and isinstance(integ_fallback, str)

        # If an integ error occurs, the fallback will overwrite p4_request. This
        # might be what you want, or might require a minor redesign. Talk to Zig
        # before removing this assert(). Unless you _are_ Zig. If you _are_ Zig,
        # talking to yourself is a sign of impending mental collapse.
        assert not (integ_fallback and p4_request)

                # If integrating, how?
                #
                # Does not include '-i' or '-b', which outer code supplies.
                # ''   : integrate, but I have no fancy flags for you.
                # None : do not integrate
                #
                # Space-delimited string.
                #
        self.integ_flags        = integ_flags

                # If integrating, how to resolve?
                #
                # Space-delimited string. Empty string prohibited (empty string
                # triggers interactive resolve behavior, which won't work in an
                # automated Git Fusion script.)
                # None not permitted unless integ_flags is also None.
                #
        self.resolve_flags      = resolve_flags

                # If integrate fails to open this file for integ, do what?
                #
                # NOP      : failure okay, this integ was helpful
                #            but not required.
                # RAISE    : failure fatal. Raise exception, revert, exit.
                # FALLBACK : Run whatever command is in .integ_fallback
                #
        self.on_integ_failure   = on_integ_failure

                # What to run if integ ran, failed to open file
                # for integ, AND on_integ_failure set to FALLBACK.
                #
                # One of None, 'add', 'edit', 'delete'
                #
        self.integ_fallback     = integ_fallback

                # What to run, unconditionally, after any integ, resolve.
                #
                # One of [None, 'add', 'edit', 'delete']
                #
        self.p4_request         = p4_request

                # For debugging, just what exactly did we feed to the
                # integ decision matrix?
                #
        self.integ_input        = integ_input

                # Not used in Matrix 1, but still accessed by Matrix 1/2 shared
                # code in p4gf_g2p_matrix_dump.py.
                #
        self.add_delete         = False

    def __repr__(self):
        fmt = ('int:{integ:<6} res:{resolve:<3}'
               ' on_int_fail:{on_integ_failure:<8}'
               ' fb:{integ_fallback:<6} p4_req:{p4_request:<6}')
        return fmt.format( integ            = _quiet_none(self.integ_flags)
                         , resolve          = _quiet_none(self.resolve_flags)
                         , on_integ_failure = self.on_integ_failure
                         , integ_fallback   = _quiet_none(self.integ_fallback)
                         , p4_request       = _quiet_none(self.p4_request)
                         )

    def add_git_action(self, git_action):
        '''
        Convert a git-fast-export or git-diff-tree action to a Perforce
        action and store it as p4_request.

        Clobbers any previously stored p4_request.
        '''
        self.p4_request = {  'A' : 'add'
                           , 'M' : 'edit'
                           , 'T' : 'edit'
                           , 'D' : 'delete'
                           }[git_action]

    def has_integ(self):
        '''Do we have a request to integrate?'''
        return self.integ_flags != None

    def has_p4_action(self):
        '''
        Do we have an integ or add/edit/delete request?'''
        return (   (self.integ_flags != None)
                or (self.p4_request  != None))

    @staticmethod
    def from_integ_matrix_row(integ_matrix_row, integ_matrix_row_input):
        '''
        Create and return a new Decided instance that captures an integ
        decision from the big p4gf_g2p_matrix_integ decision matrix.
        '''

        # The integ decision matrix compresses a lot of data into three little
        # columns. This makes for a concise and expressive decision table
        # (good), but it's time to decompress it so that our do() code can be
        # simpler.
        if integ_matrix_row.integ_flags != None:
            if integ_matrix_row.fallback == p4gf_g2p_matrix_integ.FAIL_OK:
                on_integ_failure = Decided.NOP
                integ_fallback   = None
                p4_request       = None
            elif integ_matrix_row.fallback == None:
                on_integ_failure = Decided.RAISE
                integ_fallback   = None
                p4_request       = None
            else:
                on_integ_failure = Decided.FALLBACK
                integ_fallback   = integ_matrix_row.fallback
                p4_request       = None
        else:
            # Not integrating. Might have a simple p4 request though.
            on_integ_failure = Decided.NOP
            integ_fallback   = None
            p4_request       = integ_matrix_row.fallback

        return Decided( integ_flags      = integ_matrix_row.integ_flags
                      , resolve_flags    = integ_matrix_row.resolve_flags
                      , on_integ_failure = on_integ_failure
                      , integ_fallback   = integ_fallback
                      , integ_input      = integ_matrix_row_input
                      , p4_request       = p4_request  )


class G2PMatrixCell:
    '''
    A file's intersection with a single branch.

    in "How does this branch contribute to this file?

    Actual contents vary by column. Usually a dict with results
    from some Git or Perforce operation.
    '''
    def __init__(self):

                            # Contents vary by column. Usually a dict if
                            # anything discovered, None if not.
        self.discovered    = None

                            # Decided instance if we're doing something,
                            # None if not.
        self.decided       = None

    def to_log_level(self, level):
        '''Debugging dump.'''
        if level <= logging.DEBUG3:
            return (NTR('decided: {dec}\n{disc}')
                    .format( disc = pprint.pformat(self.discovered)
                           , dec  = str(self.decided) ))
        else:
            d = self.discovered
            remainder = p4gf_util.dict_not(d, [ NTR('action')
                                              , NTR('type')
                                              , NTR('change')
                                              , NTR('depotFile')
                                              , NTR('rev')
                                              , NTR('time') ])
            if not remainder:
                remainder = ''

            disc = (NTR('{action:<10} {filetype:<10} {change:<5}'
                    ' {depot_path}{rev} {remainder}')
                    .format( action     = _lv(NTR('act:' ), NTR('action'   ), d)
                           , filetype   = _lv(NTR('type:'), NTR('type'     ), d)
                           , change     = _lv(NTR('ch:'  ), NTR('change'   ), d)
                           , depot_path = _lv(NTR(''     ), NTR('depotFile'), d)
                           , rev        = _lv(NTR('#'    ), NTR('rev'      ), d)
                           , remainder  = remainder))
            return (NTR('decided: {dec} {disc}')
                    .format( disc = disc
                           , dec  = str(self.decided) ))

def _lv(label, key, diict):
    '''Return label:value if defined, empty string if not.'''
    val = diict.get(key)
    if not val:
        return ''
    return label + val


def _quiet_none(x):
    '''Convert None to empty string for quieter debug dumps.'''
    if x == None:
        return ''
    return x

def _mode_str(x):
    '''Convert octal integer to string, return all others unchanged.'''
    if isinstance(x, int):
        return NTR('{:06o}').format(x)
    return x

