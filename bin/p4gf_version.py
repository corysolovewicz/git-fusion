#! /usr/bin/env python3.3
'''Functions to implement Perforce's -V version string.'''

# Python version is no longer explicitly checked in this module.
# Instead the SheBang on line 1 of scripts invokes the required python version.
# Thus, this module is no longer to be imported for python version checking.
# It IS to be imported into modules which  require its methods.

try:
    # pylint: disable=W0611
    import P4
except ImportError:
    # pylint: disable=W9903
    # Not importing l10n for just this one error message.
    print("Missing P4 Python module")
    exit(1)

# Yeah we're importing *. Because we're the internal face for
# p4gf_version_26.py and I don't want ANYONE importing p4gf_version26.
#
# pylint: disable=W0401
# Wildcard import p4gf_version_26
#
# pylint: disable=W0614
# Unused import %s from wildcard import
#
# pylint: disable=E0602
# Undefined variable '_'
# Defined (really imported) by p4gf_version_26
#
from p4gf_version_26 import *

if __name__ == '__main__':
    for h in ['-?', '-h', '--help']:
        if h in sys.argv:
            print(_('Git Fusion version information.'))
    print(as_string())
    exit(0)
