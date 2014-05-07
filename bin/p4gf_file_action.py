#! /usr/bin/env python3.3

'''
Tell me about fstat 'headAction' values.

Also most other 'action' values such as 'p4 describe'.
'''
from p4gf_l10n import NTR

ADD         = NTR('add')
ARCHIVE     = NTR('archive')
BRANCH      = NTR('branch')
COPY        = NTR('copy')
DELETE      = NTR('delete')
EDIT        = NTR('edit')
IGNORE      = NTR('ignore')
IMPORT      = NTR('import')
INTEGRATE   = NTR('integrate')
MERGE       = NTR('merge')
MOVE_ADD    = NTR('move/add')
MOVE_DELETE = NTR('move/delete')
PURGE       = NTR('purge')


# All known file actions.
# Useful mostly as copy-and-paste fodder for later lookup dicts.
ALL =   [ ADD
        , ARCHIVE
        , BRANCH
        , COPY
        , DELETE
        , EDIT
        , IGNORE
        , IMPORT
        , INTEGRATE
        , MERGE
        , MOVE_ADD
        , MOVE_DELETE
        , PURGE
        ]

