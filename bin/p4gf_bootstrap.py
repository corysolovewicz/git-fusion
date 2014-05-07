#! /usr/bin/env python3.3
"""Code to bootstrap Git Fusion."""

import logging

#
# Do _NOT_ import other Git Fusion code here. This is the basis of all of
# Git Fusion, nothing can come before this module. As such, anything that
# other modules rely upon that cannot be added elsewhere without creating
# circular imports, may be added here.
#

# -- Installing two new log levels, DEBUG2 and DEBUG3 -------------------------
                    # pylint:disable=W0212
                    # W0212 Access to a protected member %s of a client class
                    # We're intentionally poking new levels into module logging.

logging.DEBUG2 = 8
logging.DEBUG3 = 7

logging._levelNames['DEBUG2'] = logging.DEBUG2
logging._levelNames['DEBUG3'] = logging.DEBUG3
logging._levelNames[logging.DEBUG2] = 'DEBUG2'
logging._levelNames[logging.DEBUG3] = 'DEBUG3'


def debug2(self, msg, *args, **kwargs):
    """For logging details deeper than logger.debug()."""
    if self.isEnabledFor(logging.DEBUG2):
        self._log(logging.DEBUG2, msg, args, **kwargs)


def debug3(self, msg, *args, **kwargs):
    """For log-crushing details deeper than logger.debug()."""
    if self.isEnabledFor(logging.DEBUG3):
        self._log(logging.DEBUG3, msg, args, **kwargs)

logging.Logger.debug2 = debug2
logging.Logger.debug3 = debug3

                    # pylint:enable=W0212
# -----------------------------------------------------------------------------
