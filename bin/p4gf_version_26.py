#! /usr/bin/env python
'''
Functions to check p4d version. This file is also used by Python 2.6
scripts in the OVA setup web UI, so this file must avoid Python 2.7/3.2
requirements.
'''

#
# Note that this file should _not_ have 2to3 run on it, but rather
# do what we can to both make pylint (in Python 3.x) happy as well
# as run with Python 2.6+. May eventually need to split out the
# pieces needed for the OVA setup scripts...
#

from __future__ import print_function
# Place imports for Perforce-related modules below the version check code
# found below, to avoid any spurious import errors.
import hashlib
import logging
import os
import re
from   subprocess import Popen, PIPE
import sys

from   P4 import P4Exception, P4
import p4gf_const
import p4gf_p4msg
import p4gf_p4msgid

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

# git-fusion/bin/Version file contains standard Perforce version
# info. This Python script loads that file and parses it.
_VERSION_FILENAME = NTR('Version')

# Constant strings that are part of as_dict()'s response.
_PRODUCT_DICT = NTR({
'product_abbrev' : 'Git Fusion',
'product_long'   : 'Perforce Git Fusion',
'copyright'      : 'Copyright 2012 Perforce Software.  All rights reserved.',
'company'        : 'Perforce - The Fast Software Configuration Management System.',
})

# Required Git version (e.g. (1, 2, 3) => '1.2.3')
_GIT_VERSION = (1, 7, 11, 3)

# Required P4Python version.  This means the PATCHLEVEL reported by P4Python.
_P4PYTHON_VERSION = 573569

LOG = logging.getLogger('p4gf_version')

def as_string():
    """Return a version string as a single string of multiple lines."""
    return _dict_to_string(as_dict())


def as_dict():
    """Return all of our version fields broken out into dict elements."""
    result = _parse_version_file_contents(_load_version_file())
    for key in _PRODUCT_DICT:
        result[key] = _PRODUCT_DICT[key]

    result['bin_sha1'] = checksum_scripts()
    result['git'] = git_version()
    result['python'] = python_version()
    result['p4python'] = p4python_version()
    return result

def log_version():
    """Record 'p4gf_version.py -V' output to debug log.

    p4gf_auth_server is the main entry point, calls this. You can add more calls
    to log_version() from other top-level scripts, but maybe keep it down to
    p4gf_init, p4gf_init_repo, and p4gf_auth_server.
    """
    logging.getLogger("version").info(as_string())

def _dict_to_string(d):
    """Convert as_dict()'s result into a multiline string suitable for user display."""

    if 'release-codeline' in d:
        template = NTR("""{company}
{copyright}
Rev. {product_abbrev}/{release_year}.{release_sub}.{release_codeline}/{patchlevel} ({date_year}/{date_month}/{date_day}).
SHA1: {bin_sha1}
Git: {git}
Python: {python}
P4Python: {p4python}
""")
    else:
        template = NTR("""{company}
{copyright}
Rev. {product_abbrev}/{release_year}.{release_sub}/{patchlevel} ({date_year}/{date_month}/{date_day}).
SHA1: {bin_sha1}
Git: {git}
Python: {python}
P4Python: {p4python}
""")
    return template.format(**d)


def _load_version_file():
    """Read file git-fusion/bin/Version and return it as a string array."""

    bin_dir_path = os.path.realpath(os.path.dirname(__file__))
    version_file_path = os.path.join(bin_dir_path, _VERSION_FILENAME)
    with open(version_file_path, 'r') as f:
        return f.readlines()

def _parse_release_val(release_val):
    """Return release year and sub, and optional codeline"""

    ysc = re.match(r'(20..) (\d) ([\w-]*)', release_val)
    result = {}
    if ysc:
        result['release_year'    ] = ysc.group(1).strip()
        result['release_sub'     ] = ysc.group(2).strip()
        result['release_codeline'] = ysc.group(3).strip()
        return result

    ys = re.match(r'(20..) (\d)', release_val)
    result = {}
    if ys:
        result['release_year'    ] = ys.group(1).strip()
        result['release_sub'     ] = ys.group(2).strip()
        return result

    return result

def _parse_version_file_contents(line_array):
    """Extract RELEASE, PATCHLEVEL and SUPPDATE out of a Version file
    and return them as fields in a dict.
    Further break down RELEASE and SUPPDATE into subfields.
    """

    result = { }
    re_release    = re.compile(r'RELEASE\s*=\s*(.*)\s*;')
    re_patchlevel = re.compile(r'PATCHLEVEL =\s*(.*)\s*;')
    re_suppdate   = re.compile(r'SUPPDATE\s*=\s*(.*)\s*;')

    for line in line_array:

        m = re_release.match(line)
        if m:
            release = m.group(1).strip()
            result['release'] = release
            r = _parse_release_val(release)
            for k in r:
                result[k] = r[k]
            continue

        m = re_patchlevel.match(line)
        if m:
            result['patchlevel'] = m.group(1).strip()
            continue

        m = re_suppdate.match(line)
        if m:
            date = m.group(1).strip()
            result['date'] = date
            ymd = re.match(r'(20..) (\d+) (\d+)', date)
            if ymd:
                result['date_year' ] = ymd.group(1).strip()
                result['date_month'] = ymd.group(2).strip()
                result['date_day'  ] = ymd.group(3).strip()
            continue

    return result


def as_single_line():
    """Return a single-line version string suitable for use as a program name.

    P4GF/2012.1.PREP-TEST_ONLY/415678 (2012/04/14)
    """
    d = as_dict()

    if 'release_codeline' in d:
        return NTR("{product_abbrev}/{release_year}.{release_sub}"
                   ".{release_codeline}/{patchlevel} ({date_year}"
                   "/{date_month}/{date_day})").format(**d)
    else:
        return NTR("{product_abbrev}/{release_year}.{release_sub}"
                   "/{patchlevel} ({date_year}"
                   "/{date_month}/{date_day})").format(**d)


def argv_requests_version():
    """Did the user pass -V on the command line?"""
    return args_request_version(sys.argv)


def args_request_version(args):
    """Do any of these args include -V?"""
    return '-V' in args


def print_and_exit_if_argv():
    """If argv includes -V, then dump version and exit.

    Intended to be called near the start of every top-level
    script's main().
    """
    if argv_requests_version():
        print(as_string())
        exit(0)

def git_version():
    """Return 'git --version' as a single line, no line ending."""

    try:
        p = Popen([p4gf_const.GIT_BIN, '--version'], stdout=PIPE, stderr=PIPE)
        fd = p.communicate()
        # pylint: disable=E1101
        # Instance of '' has no '' member
        if p.returncode:
            LOG.error("Error checking Git version, git --version returned %d",
                      p.returncode)
            return None
    # pylint: disable=W0703
    # Catching too general exception
    except Exception:
        LOG.error("Error checking Git version, unable to locate and/or run git")
        return None

    return fd[0].splitlines()[0].decode('utf-8')

def parse_git_version(version_string):
    '''
    "git version 1.2.3.4" ==> [1,2,3,4] as integers.

    rc2? Ignore rc and anything after its alphanoise.
    '''
    version_word = version_string.split()[2]
    m = re.search(r'([0-9\.]+)', version_word)
    version_word = m.group(1)
    version_elements = version_word.split('.')
    version_ints = [int(x) for x in version_elements if len(x)]
    return version_ints

def git_version_acceptable(version_string):
    '''Return True if the version string meets the requirement for the Git
    version, as defined in _GIT_VERSION.
    '''
    if not version_string:
        return False
    got_list      = parse_git_version(version_string)
    required_list = _GIT_VERSION
    if len(got_list) < len(required_list):
        got_list += [0, 0, 0, 0] # Pad with 0: 1.7 == 1.7.0.0

    for got, required in zip(got_list, required_list):
        if got < required:
            return False
        if got > required:
            return True
        # if ==, continue to next number in list.

    # If exactly match, that's good too.
    return True

def git_version_check():
    '''
    Raise exception if git too old.
    '''
    git_ver = git_version()
    if not git_ver:
        raise RuntimeError(_("Unable to determine Git version"))
    if not git_version_acceptable(git_ver):
        vers = ".".join([str(v) for v in _GIT_VERSION])
        raise RuntimeError(_("Git version {0} or greater required.").format(vers))


def python_version():
    """Return python version number '2.7.3'."""
    return NTR("{major}.{minor}.{micro}").format(major=sys.version_info[0],
                                            minor=sys.version_info[1],
                                            micro=sys.version_info[2])


def p4python_version():
    """Return p4python patch level."""
    return P4.identify().split('\n')[-2]


def p4python_version_acceptable(version):
    '''Return True if the version string meets the requirement for the
    P4Python version, as defined in _P4PYTHON_VERSION.
    '''
    # pylint: disable=C0301
    #Rev. P4PYTHON/DARWIN106X86_64/2013.1.main/505185 (2013.1.MAIN-TEST_ONLY/557152 API) (2012/08/03).
    # pylint: enable=C0301
    m = re.search(r'Rev. P4PYTHON/[^/]+/[^/]+/(\d+).*', version)
    patchlevel = int(m.group(1))
    return patchlevel >= _P4PYTHON_VERSION


def p4python_version_check():
    '''
    Raise exception if p4python is too old.
    '''
    if not p4python_version_acceptable(p4python_version()):
        raise RuntimeError(NTR('P4Python patch level {0} or greater required.')
                           .format(_P4PYTHON_VERSION))


def version_check():
    '''
    Raise exception if anything is too old.
    '''
    git_version_check()
    p4python_version_check()


def p4d_version_string(p4):
    '''
    Return the serverVersion string from 'p4 info':

    P4D/LINUX26X86_64/2012.2.PREP-TEST_ONLY/506265 (2012/08/07)
    '''
    r = p4.run_info()
    key = NTR('serverVersion')
    for e in r:
        if isinstance(e, dict) and key in e:
            return e[key]
    return None


def parse_p4d_version_string(version_string):
    '''
    Convert a long server version string to a dict of parts:

    P4D/LINUX26X86_64/2012.2.PREP-TEST_ONLY/506265 (2012/08/07)

    product_abbrev  : P4D
    platform        : LINUX26X86_64
    release_year    : 2012
    release_sub     : 2
    release_codeline: PREP-TEST_ONLY
    patchlevel      : 506265
    date_year       : 2012
    date_month      : 08
    date_day        : 07
    '''

    a = version_string.split(' ')
    b = a[0].split('/')
    result = {}
    result['product_abbrev' ] = b[0]
    result['platform'       ] = b[1]

    r = b[2]
    m = re.search(r'^(\d+)\.(\d+)', r)
    result['release_year'   ] = m.group(1)
    result['release_sub'    ] = m.group(2)

    # Optional codeline
    m = re.search(r'^(\d+)\.(\d+)\.(.*)', r)
    if m:
        result['release_codeline'] = m.group(3)

    result['patchlevel'     ] = b[3]

    m = re.search(r'(\d+)/(\d+)/(\d+)', a[1])
    result['date_year'      ] = m.group(1)
    result['date_month'     ] = m.group(2)
    result['date_day'       ] = m.group(3)
    return result

# What patch level required for each year.sub?
_P4D_REQ_PATCH = { 2012.2: 684894
                 , 2013.1: 685046
                 , 2013.2: 671876
                 }
_P4D_REQ_YEAR_SUB = 2012.2
_P4D_MAX_YEAR_SUB = max(_P4D_REQ_PATCH.keys())

def p4d_version_required(version_string):
    '''
    Return a 2-tuple (2010.2, 503309) of the (year.sub, patch) REQUIRED
    for the given p4d year.sub.
    '''
    d = parse_p4d_version_string(version_string)
    year_sub_string = d['release_year'] + '.' + d['release_sub']
    year_sub = float(year_sub_string)

    if year_sub < _P4D_REQ_YEAR_SUB:
        return (_P4D_REQ_YEAR_SUB, _P4D_REQ_PATCH[_P4D_REQ_YEAR_SUB])

    if year_sub in _P4D_REQ_PATCH:
        return (year_sub, _P4D_REQ_PATCH[year_sub])

    if year_sub < 2012.2:
        LOG.warning("Unknown p4d year_sub: {0}".format(year_sub))

    return (2012.2, _P4D_REQ_PATCH[2012.2])


def p4d_version_acceptable(version_string):
    '''
    Is this a P4D with the 'p4 reviews -C' patch?
    '''
    d = parse_p4d_version_string(version_string)
    year_sub_string = d['release_year'] + '.' + d['release_sub']
    year_sub = float(year_sub_string)
    patch_level = int(d['patchlevel'])
    #print('year.sub={0} patch_level={1}'.format(year_sub, patch_level))

    if year_sub < _P4D_REQ_YEAR_SUB:
        return False

    if _P4D_MAX_YEAR_SUB < year_sub:
        return True

    if year_sub not in _P4D_REQ_PATCH:
        LOG.warning("Unknown p4d year_sub: {0}".format(year_sub))
        return False

    return _P4D_REQ_PATCH[year_sub] <= patch_level


def p4d_version_check(p4):
    """Check that p4d version is acceptable

    if p4 is not connected, it will be connected/disconnected
    if already connected, it will be left connected
    """

    version_string = p4d_version_string(p4)
    # Record value so we can reuse later rather than re-run 'p4 info'.
    # Bypass P4.__set_attr__() override.
    p4d_version_cache_set(p4, version_string)

    acceptable     = p4d_version_acceptable(version_string)
    if not acceptable:
        vr = p4d_version_required(version_string)

        msg = (_('Unsupported p4d version: {actual}'
                '\nGit Fusion requires version {req_v}/{req_ch} or later').format(
            actual=version_string,
            req_v=vr[0],
            req_ch=vr[1]))
        raise RuntimeError(msg)


_KEY_VERSION_YEAR_SUB = '_version_year_sub'
def p4d_version_cache_set(p4, version_string):
    '''
    Record p4d's version number for later. Avoid repeated calls to 'p4 info'.
    '''
    d = parse_p4d_version_string(version_string)
    year_sub_string = d['release_year'] + '.' + d['release_sub']
    year_sub = float(year_sub_string)
    p4.__dict__[_KEY_VERSION_YEAR_SUB] = year_sub


def p4d_version(p4):
    '''
    Return p4d's version as a float: 2011.1

    Cache it so we don't have to call 'p4 info' over and over.
    '''
    if not _KEY_VERSION_YEAR_SUB in p4.__dict__:
        p4d_version_cache_set(p4, p4d_version_string(p4))
    return p4.__dict__[_KEY_VERSION_YEAR_SUB]


def p4d_supports_protects(p4):
    '''
    Check that p4d is ALREADY configured to give git-fusion-user
    permission to run 'p4 protects -u'. Return False if not.

    *** This function does not really belong here! ***
    This is NOT a version check, but I'm sticking it in this
    file anyway because the OVA web UI needs this function, and the OVA
    web UI already has access to p4gf_version_26.py.
    '''
    okay = False
    try:
        p4.run('protects', '-u', p4gf_const.P4GF_USER, '-m')
        # if we can use -u, we're good to go
        okay = True
    except P4Exception:
        e = sys.exc_info()[1]
        LOG.warn("'protects -u' failed: {0}".format(e))
        if p4gf_p4msg.find_msgid(p4, p4gf_p4msgid.MsgDm_ProtectsEmpty):
            okay = True
        # All other errors are fatal
    return okay


def checksum_scripts():
    """Compute a SHA1 checksum of the Python scripts in the bin directory
    to allow detecting unsupported modifications.
    """
    # scan for .py files in bin directory
    bin_dir_path = os.path.dirname(os.path.realpath(__file__))
    pyfiles = []
    for root, _, files in os.walk(bin_dir_path):
        for name in files:
            if name.startswith('p4gf_') and name.endswith('.py'):
                pyfiles.append(os.path.join(root, name))
    # sort by name to ensure consistency
    pyfiles.sort()
    # compute SHA1 checksum of files
    # pylint: disable=E1101
    s = hashlib.sha1()
    for py in pyfiles:
        with open(py) as f:
            txt = f.read()
            s.update(txt.encode('utf-8'))
    return s.hexdigest()


if __name__ == '__main__':
    for h in ['-?', '-h', '--help']:
        if h in sys.argv:
            print(_('Git Fusion version information when running Python 2.6.'))
    print(as_string())
    exit(0)
