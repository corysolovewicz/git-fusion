#! /usr/bin/env python3.3
'''Remember which changelists we already integrated.'''

from   p4gf_l10n      import NTR

class IntegratedUpTo:
    '''
    After integrating across branches, remember the source changelist number so
    that you can use it to accelerate future 'p4 integ' requests.

    A little bookkeeping on our side can save the Perforce server from scanning
    far back in history to look for unintegrated file actions.
    '''
    def __init__(self):
        self._key_to_change_num = {}

    def set(self, from_branch, to_branch, change_num):
        '''Remember.'''
        self._key_to_change_num[_key( from_branch = from_branch
                                    , to_branch   = to_branch  )] = change_num

    def get(self, from_branch, to_branch):
        '''Tell me.'''
        r = self._key_to_change_num.get(_key( from_branch = from_branch
                                            , to_branch   = to_branch  ), None)
        return r

def _to_str(branch):
    '''
    Convert a branch to a string, which we use internally
    as part of our dict key.
    '''
    if not (branch or branch.branch_id):
        return None
    return branch.branch_id

def _key(from_branch, to_branch):
    '''
    Convert a pair of branches to a key for our dict.
    '''
    return NTR('{f} {t}').format(f = _to_str(from_branch), t = _to_str(to_branch))
