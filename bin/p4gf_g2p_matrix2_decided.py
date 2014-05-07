#! /usr/bin/env python3.3
'''
The "what have we decided to do about this one file from this one depot branch?"
half of one cell in a G2PMatrix.
'''
import logging

import p4gf_g2p_matrix2_integ                        as p4gf_g2p_matrix_integ
from   p4gf_l10n                import NTR
import p4gf_util

LOG = logging.getLogger('p4gf_g2p_matrix2').getChild('decided')  # subcategory of G2PMatrix.

                        # Both 'M'odify contents and 'T'ype change are 'p4 edit'
GIT_TO_P4_ACTION = { 'A'  : 'add'
                   , 'M'  : 'edit'
                   , 'T'  : 'edit'
                   , 'D'  : 'delete'
                   , None : None
                   }

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
                , add_delete         = False
                , integ_input        = None
                , ghost_p4filetype   = None
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

                # Must add a placeholder file, submit, then delete.
                #
                # This is how we propagate an ancestor depot branch's "p4 delete"
                # file action that occurs AFTER our fully populated basis.
                #
        self.add_delete         = add_delete

                # For debugging, just what exactly did we feed to the
                # integ decision matrix?
                #
        self.integ_input        = integ_input

                # Used only for GHOST actions, left None for all others.
        self.ghost_p4filetype   = ghost_p4filetype

    def __repr__(self):
        fmt = ('int:{integ:<6} res:{resolve:<3}'
               ' on_int_fail:{on_integ_failure:<8}'
               ' fb:{integ_fallback:<6} p4_req:{p4_request:<6}')
        return fmt.format( integ            = p4gf_util.quiet_none(self.integ_flags)
                         , resolve          = p4gf_util.quiet_none(self.resolve_flags)
                         , on_integ_failure = self.on_integ_failure
                         , integ_fallback   = p4gf_util.quiet_none(self.integ_fallback)
                         , p4_request       = p4gf_util.quiet_none(self.p4_request)
                         )

    def add_git_action(self, git_action):
        '''
        Convert a git-fast-export or git-diff-tree action to a Perforce
        action and store it as p4_request.

        Clobbers any previously stored p4_request.
        '''
        self.p4_request = GIT_TO_P4_ACTION[git_action]

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


