#! /usr/bin/env python3.3
'''
class TempP4BranchMapping
'''
import logging

import p4gf_const
from   p4gf_l10n      import _, NTR
import p4gf_util

LOG = logging.getLogger(__name__)

class TempP4BranchMapping:
    '''
    A class that can create or overwrite a Perforce branch spec.

    Generates its own temporary branch name.
    '''
    def __init__(self):
        self.name    = p4gf_const.P4GF_BRANCH_TEMP_N.format(p4gf_util.uuid(p4=None))
        self.written = False

    def write_map(self, p4, p4map):
        '''
        Write our spec to Perforce.
        '''
        spec = p4gf_util.p4run_logged(p4, ['branch', '-o', self.name])[0]
        spec['Options']     = NTR('unlocked') # 'locked' complicates cleanup/delete.
        spec['View']        = p4map.as_array()
        spec['Description'] = _("Temporary mapping created during 'git push'.")
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('write_map() name={} view={}'.format(self.name, spec['View']))
        else:
            LOG.debug2('write_map() name={}'.format(self.name))
        p4.save_branch(spec)
        self.written = True

    def delete(self, p4):
        '''
        Remove our branch spec.
        '''
        if not self.written:
            return

        with p4.at_exception_level(p4.RAISE_NONE):
            p4gf_util.p4run_logged(p4, ['branch', '-d', self.name])
