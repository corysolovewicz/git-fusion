#! /usr/bin/env python3.3
'''Directory operations that both p4gf_util and p4gf_log need.'''

# These are imported into p4gf_util, you usually want to import that
# instead of p4gf_ensure_dir.

import errno
import os

def parent_dir(local_path):
    '''Return the path to local_path's immediate parent.'''
    return os.path.dirname(local_path)


def ensure_dir(local_dir_path):
    '''If dir_path does not already exist, create it.'''
    try:
        os.makedirs(local_dir_path)
    except OSError as e:
        # ignore 'File exists' error (errno 17)
        # why not test existence first? because os.path.exists lies.
        if not e.errno == errno.EEXIST:
            raise


def ensure_parent_dir(local_path):
    '''
    If local_path's immediate parent directory does not already
    exist, create it.
    '''
    ensure_dir(parent_dir(local_path))
