#! /usr/bin/env python3.3
'''
Support for localization.

Ideally you only need to import this file once in each top-level script.

This script is imported by both Python 3.3 and Python 2.6 scripts.
Avoid 3.3-only code.
'''
import gettext
import os
import os.path
import sys

# Do not import other p4gf_xxx modules. Doing so is almost
# certain to introduce a circular import loop.

_LOCALE_DIR  = None


                        # pylint:disable=C0103
                        # Invalid function name "NTR"
                        #
                        # Too short for usual names, but this is used too frequently to
                        # permit a longer name. I'd make it shorter if I could but that
                        # would be a bit too cryptic.
def NTR(x):
    '''Marker for No-TRanslate'''
    return x
                        # pylint:enable=C0103

def _import():
    '''
    Load our translation .mo file (if any).

    This function is run once per Python process, the first time some other
    Python script says "import p4gf_l10n" or "from p4gf_l10n import ....".
    Called from module-level code down near bottom of this file.
    '''
                            # Find the directory that holds all our Localized translations:
                            #   bin/lang/
    global _LOCALE_DIR
    _script_path = os.path.realpath(__file__)
    _bin_dir     = os.path.dirname(_script_path)
    _LOCALE_DIR  = os.path.join(_bin_dir, 'mo')


                            # Tell gettext that all of our text exists in domain "git-fusion".
    gettext.bindtextdomain('git-fusion', localedir=_LOCALE_DIR)
    gettext.textdomain('git-fusion')

                            # "Install" gettext.gettext() as function _ in our global
                            # namespace so that all our Python modules can use it without
                            # duplicating this gettext code.
    gettext.install('git-fusion', localedir=_LOCALE_DIR)


def mo_file_path():
    '''
    Return path to existing, found, loaded, .mo file.
    '''
    return gettext.find('git-fusion', localedir=_LOCALE_DIR)


def mo_dir():
    '''
    Return path to existing, found, loaded, .mo file.
    '''
    mo_file = mo_file_path()
    if not mo_file:
        return None
    return os.path.dirname(mo_file)


def diagnostics():
    '''
    Return a list of lines suitable for logging.
    From where did we get our translations?
    '''
    fmt = '{:<13}: {}'
    lines = []
    lines.append(fmt.format(NTR('argv[0]'), sys.argv[0]))
    lines.append(fmt.format(NTR('locale dir'), _LOCALE_DIR))

    for x in ['LANGUAGE', 'LC_ALL', 'LC_MESSAGES', 'LANG']:
        lines.append(fmt.format(x, os.environ.get(x)))

                            # You'll get GNUTranslations.gettext if gettext
                            # found a .mo file, or NullTranslations.gettext if
                            # not.
    lines.append(fmt.format(NTR('gettext()'), _))

                            # Full path to translation .mo file.
    _mo_path = mo_file_path()
    lines.append(fmt.format(NTR('.mo file'), _mo_path))
    return lines


def log_l10n():
    '''
    Record diagnostics to debug log.

    Do NOT do this at import time, since import time occurs before log
    configuration. Call this explicitly AFTER configuring the log.
    '''
    import logging
    log = logging.getLogger('p4gf_l10n')
    if log.isEnabledFor(logging.DEBUG2):
        lines = diagnostics()
        for l in lines:
            log.debug2(l)


# -- begin top-level always run at import time --------------------------------
_import()
_ = gettext.gettext
