#! /usr/bin/env python3.3

'''
Tell me about 'p4 filelog' 'how' values.
'''
from   p4gf_l10n    import NTR

# All known 'how' values.
# Useful mostly as copy-and-paste fodder for later lookup dicts.

ALL = [
      NTR('add from')    , NTR('add into')
    , NTR('branch from') , NTR('branch into')
    , NTR('copy from')   , NTR('copy into')
    , NTR('delete from') , NTR('delete into')
    , NTR('edit from')   , NTR('edit into')
    , NTR('ignored')     , NTR('ignored by')      # Note different preposition.
    , NTR('merge from')  , NTR('merge into')
    , NTR('move from')   , NTR('move into')
    ]

# Integrating from some other path. 'file' contains source.
FROM = [
      NTR('add from')
    , NTR('branch from')
    , NTR('copy from')
    , NTR('delete from')
    , NTR('edit from')
    , NTR('ignored')         # Note different preposition.
    , NTR('merge from')
    , NTR('move from')
    ]
def is_from(how):
    '''
    Does this action specify an integ-like action _from_ somewhere?
    If so, then 'p4 filelog's corresponding 'file' value will be a source.
    '''
    return how in FROM

# Integrating to some other path. 'file' contains destination.
TO = [
      NTR('add into')
    , NTR('branch into')
    , NTR('copy into')
    , NTR('delete into')
    , NTR('edit into')
    , NTR('ignored by')      # Note different preposition.
    , NTR('merge into')
    , NTR('move into')
    ]
def is_to(how):
    '''
    Does this action specify an integ-like action _to_ somewhere?
    If so, then 'p4 filelog's corresponding 'file' value will be a destination.
    '''
    return how in TO

