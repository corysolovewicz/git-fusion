#! /usr/bin/env python3.3
"""Exception thrown by p4gf_env_config when P4GF_ENV names non-existant path"""
from p4gf_const import P4GF_ENV_NAME
from  p4gf_l10n  import _

class MissingConfigPath(Exception):
    '''Exception thrown when P4GF_ENV names missing file.'''
    def __init__(self, filename):
        self.filename = filename
        self.message = _("Git Fusion environment: path {0} in {1} does not exist."). \
            format(self.filename, P4GF_ENV_NAME)
        super(MissingConfigPath, self).__init__(self.message)
    def __str__(self):
        return self.message

