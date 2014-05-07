#! /usr/bin/env python3.3
'''Functions for reading and writing the Git Fusion configuration
file, which contains settings such as the Perforce client name.
'''

import configparser
import logging
import os

import p4gf_const
from   p4gf_l10n      import NTR
import p4gf_path

RC_FILE = p4gf_const.P4GF_RC_FILE
P4_SECTION = NTR('perforce')
LOG = logging.getLogger(__name__)

# ConfigParser instances keyed by their paths. Tried using
# weakref.WeakValueDictionary but they would disappear from the cache when
# the garbage collector ran, and since they have a leaking problem, would
# never really get cleaned up.
_config_cache = dict()


def root_to_rc_path(root):
    """Convert <root> to <root>/.git-fusion-rc"""
    return os.path.join(root, RC_FILE)


def cwd_to_rc_path():
    """Scan current and ancestor directories for an existing
    .git-fusion-rc file.

    Return path to .git-fusion-rc file if found, None if not.
    """
    return p4gf_path.cwd_to_rc_file()


def set_if(config, section, option, value):
    """If config lacks a value for the given option, or if it has different
    value, set that value in the config and return True. If not, do nothing
    and return False.
    """

    modified = False
    if not config.has_section(section):
        config.add_section(section)
        modified = True

    if (    (not config.has_option(section, option))
         or (config.get(section, option) != value)):
        config.set(section, option, value)
        return True

    return modified


def update_file(rc_path, client_name, view_name):
    """Create the config file if it does not yet exist. Add or set
    client and view name, write to file.
    Retain any old values.
    Don't write file if we changed nothing.
    """

    config = configparser.ConfigParser(interpolation=None)
    config.read(rc_path)
    modified  = set_if(config, P4_SECTION, NTR('p4client'), client_name)
    modified |= set_if(config, P4_SECTION, NTR('view'), view_name)

    if modified:
        _config_cache[rc_path] = config
        LOG.debug("updating RC file {}".format(rc_path))
        with open(rc_path, 'w') as rc_file:
            config.write(rc_file)
    return config


def calc_rc_path(root, rc_path):
    """If rc_path supplied, use that. If not, but root supplied, use
    that as the parent of the rc_file. If nothing supplied, use cwd and
    go find where the rc file should go.
    """
    if rc_path:
        return rc_path
    if root:
        return root_to_rc_path(root)
    return cwd_to_rc_path()


def read_config(root=None, rc_path=None):
    """Load the configuration file found in the given directory, and
    return the ConfigParser object. If the file is not present, the
    returned config object will be empty.

    Keyword arguments:
    root -- path to the configuration file's parent directory.
            If omitted, default cwd_to_rc_path() goes and finds it next
            to the .git/ directory somewhere in cwd or higher.
            Used only if rc_
    """
    rc_path = calc_rc_path(root, rc_path)
    LOG.debug("read_config root={r} got path={p}"
              .format(r=root, p=rc_path))
    if not rc_path:
        return None
    if rc_path in _config_cache:
        return _config_cache[rc_path]
    config = configparser.ConfigParser(interpolation=None)
    LOG.debug3('read_config: creating configparser for path {}'.format(rc_path))
    config.read(rc_path)
    _config_cache[rc_path] = config
    return config


def get_view(config):
    """Retrieve the Git Fusion 'view' name, if present.

    Keyword arguments:
    config -- ConfigParser from which to read settings

    """
    return config.get(P4_SECTION, 'view')
