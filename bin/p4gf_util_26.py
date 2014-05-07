#! /usr/bin/env python
"""
Utility functions for P4GF that are Python 2.6 compatible, for use by
the OVA management scripts. All other users should import p4gf_util.
"""

import os
import socket
import sys
import p4gf_const

Hostname = None

                        # Localization/translation support.
                        # Don't let unwanted Python 3.3 code in p4gf_l10n prevent
                        # this script from running in 2.6.
try:
    from p4gf_l10n import _, NTR
except ImportError:
                        # pylint:disable=C0103
                        # Invalid name NTR()
    def NTR(x):
        '''No-TRanslate: Localization marker for string constants.'''
        return x
    _ = NTR
                        # pylint:enable=C0103


def get_hostname():
    """Return the short name of the machine the Python interpreter is running on.
    """
    global Hostname
    if Hostname is None:
        Hostname = socket.gethostname()
        dot = Hostname.find('.')
        if dot > 0:
            Hostname = Hostname[:dot]
    return Hostname


def server_id_file_path():
    '''
    Return the path to P4GF_HOME/server-id
    '''
    return os.path.join(p4gf_const.P4GF_HOME, p4gf_const.P4GF_ID_FILE)


def read_server_id_from_file():
    '''
    If there is a P4GF_HOME/server_id file, return the ID from that.
    If not, return None.
    '''
    path = server_id_file_path()
    if os.path.exists(path):
        with open(path, 'r') as f:
            words = f.read().split()
            if words:
                return words[0]
    return None


def get_server_id():
    '''
    If there is a P4GF_HOME/server-id file, return the ID from that file.
    If not, return None.
    '''
    server_id = read_server_id_from_file()
    if not server_id:
        raise RuntimeError(_('server-id not set. Run p4gf_super_init.py and try again.'))
    return server_id


def get_object_client_name():
    """Produce the name of the host-specific object client for the Git Fusion depot.
    """
    return p4gf_const.P4GF_OBJECT_CLIENT.format(server_id=get_server_id())

def get_12_2_object_client_name():
    """Produce the name of the host-specific object client for the Git Fusion depot.
    """
    return p4gf_const.P4GF_OBJECT_CLIENT_12_2.format(hostname=get_hostname())


def has_server_id_or_exit(log=None):
    '''
    Check if the server-id file is present, exiting if not.
    '''
    if read_server_id_from_file() is None:
        formed = _("Git Fusion is missing '{0}' file '{1}'.").format(
            p4gf_const.P4GF_ID_FILE, server_id_file_path())
        sys.stderr.write(formed + _(' Please contact your administrator.\n'))
        if log is not None:
            log.error(formed + _(' Please contact your administrator.\n'))
        sys.exit(os.EX_SOFTWARE)
