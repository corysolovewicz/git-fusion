#! /usr/bin/env python3.3
'''
A single cell in a G2PMatrix.
'''

import logging
import pprint

from   p4gf_g2p_matrix2_decided import Decided
from   p4gf_l10n                import NTR
import p4gf_util

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

    def decided_(self):
        '''
        Guaranteed to return a Decided instance. Creates one if necessary.
        '''
        if not self.decided:
            self.decided = Decided()
        return self.decided

    def discovered_(self):
        '''
        Guaranteed to return a Decided instance. Creates one if necessary.
        '''
        if not self.discovered:
            self.discovered = {}
        return self.discovered

    @staticmethod
    def safe_discovered(cell, key):
        '''
        If cell exists and has a value discovered for key, return that value.
        If not, return None.
        '''
        if (    cell
            and cell.discovered):
            return cell.discovered.get(key)
        return None

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
                                              , NTR('time')
                                              ])
            if not remainder:
                remainder = ''

            disc = (NTR('{action:<10} {filetype:<10} {change:<5}'
                    ' {depot_path}{rev} {remainder}')
                    .format( action     = _lv(NTR('act:' ), NTR('action'   ), d)
                           , filetype   = _lv(NTR('type:'), NTR('type'     ), d)
                           , change     = _lv(NTR('ch:'  ), NTR('change'   ), d)
                           , depot_path = _lv(NTR(''     ), NTR('depotFile'), d)
                           , rev        = _lv(NTR('#'    ), NTR('rev',     ), d)
                           , remainder  = remainder))
            return (NTR('decided: {dec} {disc}')
                    .format( disc = disc
                           , dec  = str(self.decided) ))

# -- module-wide --------------------------------------------------------------

def _lv(label, key, diict):
    '''Return label:value if defined, empty string if not.'''
    val = diict.get(key)
    if not val:
        return ''
    return label + val


