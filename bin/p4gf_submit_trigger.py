#! /usr/bin/env python

"""Git Fusion submit triggers.

    These triggers coordinate with Git Fusion to support git atomic pushes.
    Service user accounts use p4 user Reviews to manage list of locked files.
    There is one service user per Git Fusion instance
    and one for non Git Fusion submits.
    This trigger is compatible with python versions 2.x >= 2.6 and >= 3.3
    The trigger is compatible with p4d versions >= 2012.2.
    For distributed p4d (>= 2014.1) triggers are installed only on the commit server.
    Submits from edge servers are handled by the commit server.
"""
# pylint:disable=W9903
# Skip localization/translation warnings about config strings
# here at the top of the file.

# -- Configuration ------------------------------------------------------------
# Edit these constants to match your p4d server and environment.

CHARSET = []
# For unicode servers uncomment the following line
#CHARSET = ['-C', 'utf8']

# Set to the location of the p4 binary.
# When in doubt, change this to an absolute path.
P4GF_P4_BINARY = "p4"

# For Windows systems use no spaces in the p4.exe path
#P4GF_P4_BINARY = "C:\PROGRA~1\Perforce\p4.exe"
# -----------------------------------------------------------------------------

import sys

# Determine python version
PYTHON3 = True
if sys.hexversion < 0x03000000:
    PYTHON3 = False

# Exit codes for triggers, sys.exit(CODE)
P4PASS = 0
P4FAIL = 1

KEY_VIEW = 'view'

# Import the configparser - either from python2 or python3
# pylint:disable=F0401
# Unable to import
# pylint:disable=C0103
# Invalid class name
try:
    # python3.x import
    import configparser
    PARSING_ERROR = configparser.ParsingError
except ImportError:
    # python2.x import
    import cStringIO
    import ConfigParser
    PARSING_ERROR = ConfigParser.ParsingError
# pylint:enable=C0103
# pylint:enable=F0401

P4GF_USER = "git-fusion-user"
P4D_14_OR_LATER = False

# Permit Git Fusion to operate without engaging its own triggers.
# Triggers are to be applied only to non P4GF_USERS.
# With one exception: apply the change-commit trigger defined for p4gf_config files.
# Git Fusion edits the p4gf_config files.
#
if (len(sys.argv) >= 4
     and sys.argv[3] == P4GF_USER
     and sys.argv[1] != "change-commit-p4gf-config"):
    sys.exit(P4PASS)   # continue the submit but skip the trigger for GF

# these imports here to avoid unneeded processing before the early exit test above

import os
import re
from   subprocess import Popen, PIPE
import marshal
import time
import datetime
import tempfile
import calendar
import getopt

                        # Optional localization/translation support.
                        # If the rest of Git Fusion's bin folder
                        # was copied along with this file p4gf_submit_trigger.py,
                        # then this block loads LC_MESSAGES .mo files
                        # to support languages other than US English.
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
                        # pylint:enable=W9903


# Find the 'p4' command line tool.
# If this fails, edit P4GF_P4_BINARY in the "Configuration"
# block at the top of this file.

import distutils.spawn
P4GF_P4_BIN = distutils.spawn.find_executable(P4GF_P4_BINARY)
if not P4GF_P4_BIN:
    print(_("Git Fusion Submit Trigger cannot find p4 binary: '{0}'"
            "\nPlease update this trigger using the full path to p4").
                    format(P4GF_P4_BINARY))
    sys.exit(P4FAIL) # Cannot find the binary

# disallow SPACE in path name
if ' ' in P4GF_P4_BIN:
    print(_("Please edit p4gf_submit_trigger.py and set P4GF_P4_BIN to a path without spaces."))
    sys.exit(P4FAIL) # Space in binary path

# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
#
# Normal usage of Git Fusion should not require changing of the
# P4GF_DEPOT constant. If a site requires a different depot name
# then set this constant on ALL Git Fusion instances to the same
# depot name.
#
# This depot should be created by hand prior to running any Git
# Fusion instance. Wild card and revision characters are not
# allowed in depot names (*, ..., @, #) and non-alphanumeric
# should typically be avoided.

P4GF_DEPOT         = NTR('.git-fusion')

#
#                 End block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------

# second block

# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
#
# Atomic Push
#
# Atomic view locking requires special counters and users to insert Reviews into
# the user spec Each Git Fusion server has its own lock.
#
P4GF_REVIEWS_GF                     = NTR('git-fusion-reviews-') # Append GF server_id.
P4GF_REVIEWS__NON_GF                = P4GF_REVIEWS_GF + NTR('-non-gf')
P4GF_REVIEWS__ALL_GF                = P4GF_REVIEWS_GF + NTR('-all-gf')
P4GF_REVIEWS_NON_GF_SUBMIT          = NTR('git-fusion-non-gf-submit-')
P4GF_REVIEWS_NON_GF_RESET           = NTR('git-fusion-non-gf-')
DEBUG_P4GF_REVIEWS__NON_GF          = NTR('DEBUG-') + P4GF_REVIEWS__NON_GF
DEBUG_SKIP_P4GF_REVIEWS__NON_GF     = NTR('DEBUG-SKIP-') + P4GF_REVIEWS__NON_GF
P4GF_REVIEWS_SERVICEUSER            = P4GF_REVIEWS_GF + '{0}'
NON_GF_REVIEWS_BEGIN_MARKER_PATTERN = '//GF-{0}/BEGIN'
NON_GF_REVIEWS_END_MARKER_PATTERN   = '//GF-{0}/END'

# Is the Atomic Push submit trigger installed and at the correct version?
#
P4GF_COUNTER_PRE_TRIGGER_VERSION    = NTR('git-fusion-pre-submit-trigger-version')
P4GF_COUNTER_POST_TRIGGER_VERSION   = NTR('git-fusion-post-submit-trigger-version')
P4GF_TRIGGER_VERSION                = NTR('00004')

#
#                 End block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------

GF_BEGIN_MARKER = ''
GF_END_MARKER   = ''
CHANGE_FOUND_BEGIN = False
CHANGE_FOUND_END = False

P4GF_HEARTBEATS             = "git-fusion-view-*-lock-heartbeat"
HEARTBEAT_TIMEOUT_SECS      = 60

# Value for counter P4GF_REVIEWS_NON_GF_SUBMIT when submit trigger decided this
# changelist requires no further processing by this trigger.
#
# Value must not be a legal depot path. Lack of leading // works.
#

TRIGGER_TYPES = ['change-submit', 'change-commit',
                 'change-content', 'change-commit-p4gf-config']
TRIGGER_TYPES_14 = ['change-commit', 'change-content',
                    'change-failed', 'change-commit-p4gf-config']
# Messages for human users.
# Complete sentences.
# Except for trigger spec, hardwrap to 78 columns max, 72 columns preferred.
MSG_LOCKED_BY_GF            = _("\nFiles in the changelist are locked by Git Fusion user '{0}'.")
MSG_PRE_SUBMIT_FAILED       = _("Git Fusion pre-submit trigger failed.")
MSG_POST_SUBMIT_FAILED      = _("Git Fusion post-submit trigger failed.")
MSG_TRIGGER_FAILED          = _("\nGit Fusion '{0}' trigger failed: {1}")
MSG_ARGS                    = _("user: '{0}' changelist: {1}")
MSG_MISSING_ARGS            = _("Git Fusion trigger missing arguments.")
MSG_TRIGGER_FILENAME        = _("p4gf_submit_trigger.py")
MSG_MALFORMED_CONFIG        = _("p4gf_config file submitted, but will not work for Git Fusion.")
# pylint: disable = C0301,W1401
# Line too long;  Anomalous backslash in string
MSG_TRIGGER_SPEC = NTR("""
    GF-change-submit         change-submit  //...                       "/path/to/python /path/to/p4gf_submit_trigger.py change-submit             %changelist% %user% %client% %serverport%"
    GF-change-content         change-content //...                       "/path/to/python /path/to/p4gf_submit_trigger.py change-content            %changelist% %user% %client% %serverport%"
    GF-change-commit        change-commit  //...                       "/path/to/python /path/to/p4gf_submit_trigger.py change-commit             %changelist% %user% %client% %serverport% %oldchangelist%"
    GF-change-commit-config change-commit  //""" + P4GF_DEPOT +"""/repos/*/p4gf_config "/path/to/python /path/to/p4gf_submit_trigger.py change-commit-p4gf-config %changelist% %user% %client% %serverport% %oldchangelist%"
""")
MSG_TRIGGER_SPEC_14_1 = NTR("""
    GF-change-content       change-content //...  "/path/to/python /path/to/p4gf_submit_trigger.py change-content            %changelist% %user% %client% %serverport% %command% %args%"
    GF-change-commit        change-commit  //...  "/path/to/python /path/to/p4gf_submit_trigger.py change-commit             %changelist% %user% %client% %serverport% %oldchangelist% %command% %args%"
    GF-change-failed        change-failed  //...  "/path/to/python /path/to/p4gf_submit_trigger.py change-failed             %changelist% %user% %client% %serverport% %command% %args%"
    GF-change-commit-config change-commit  //""" + P4GF_DEPOT +"""/repos/*/p4gf_config "/path/to/python /path/to/p4gf_submit_trigger.py change-commit-p4gf-config %changelist% %user% %client% %serverport% %oldchangelist% %command% %args%"
""")
MSG_EXAMPLE_UNIX = NTR('python p4gf_submit_trigger.py --generate-trigger-entries "/absolute/pathto/python[3]" "/absolute/pathto/p4gf_submit_trigger.py" P4PORT')
MSG_EXAMPLE_DOS  = NTR('python p4gf_submit_trigger.py --generate-trigger-entries "C:\\absolute\\pathto\\python[3]" "C:\\absolute\\pathto\\p4gf_submit_trigger.py" P4PORT')
MSG_USAGE = _("""

    Git Fusion requires a submit trigger to be installed on your Perforce server
    to properly support atomic commits from Git.

    Installing Triggers
    -------------------
    Install triggers for each Perforce server configured for Git Fusion:

    1) Copy 'p4gf_submit_trigger.py' to your Perforce server machine.
    2) These triggers require Python 2.6+ or Python 3.2+ on the
       Perforce server machine.
    3) Update the p4d trigger spec.
       As a Perforce super user run 'p4 triggers' and add the
       Git Fusion trigger entries displayed for your server by the following command.

        {MSG_EXAMPLE_UNIX}

        (for Windows):
        {MSG_EXAMPLE_DOS}

    Logging in Perforce users
    -------------------------
    Running p4gf_super_init.py on the Git Fusion server creates the users below,
    prompting for and setting a shared password.

    After running p4gf_super_init.py, you must log each user into the
    Perforce server using 'p4 login':
        - for the Git Fusion server, p4 login under the unix account running Git Fusion.
        - for the Git Fusion triggers, p4 login under the OS account running p4d

    Logins for Git Fusion users are required as listed below.
                                            Git Fusion Server      p4d server
        git-fusion-user                       login                 login
        git-fusion-reviews-<server-id>        login                 login (used by --reset)
        git-fusion-reviews--non-gf            login                 login
        git-fusion-reviews--all-gf                                  login



    Configure Git Fusion that Triggers Are Installed
    -------------------------------------------
    Configure Git Fusion that these triggers are installed, and
    thus avoid 'triggers are not installed' or 'triggers need updating'
    error messages:

        python p4gf_submit_trigger.py --set-version-counter P4PORT

    Clearing Locks
    --------------
    To clear any locks created by previous executions of this trigger or of Git Fusion:

        python p4gf_submit_trigger.py --reset P4PORT [superuser]

    This removes all 'p4 reviews' and 'p4 counters -u' data stored
    by this trigger and Git Fusion used to provide atomic locking for 'git push'.

    Defining Depot Paths Managed by Git Fusion
    ------------------------------------------
    To rebuild the list of Perforce depot paths currently part of any
    Git Fusion repo:

        python p4gf_submit_trigger.py --rebuild-all-gf-reviews P4PORT [superuser]

    By default this command runs as Perforce user 'git-fusion-reviews--all-gf'.
    The optional superuser parameter must be a Perforce super user.


""").format(MSG_EXAMPLE_UNIX = MSG_EXAMPLE_UNIX
           , MSG_EXAMPLE_DOS  = MSG_EXAMPLE_DOS )
# pylint: enable = C0301,W1401


# time.sleep() accepts a float, which is how you get sub-second sleep durations.
MS = 1.0 / 1000.0

# How often we retry to acquire the lock.
_RETRY_PERIOD = 100 * MS

# By default P4PORT is set from the p4d trigger %serverport% argument.
# Admins optionally may override the %serverport% by setting P4PORT here to a non-empty string.
P4PORT = None
P4D_VERSION = None
SEPARATOR  = '...'

# Valid fields when updating the user spec
USER_FIELDS = NTR(['User', 'Type', 'Email', 'Update', 'Access',
    'FullName', 'JobView', 'Password', 'Reviews'])

# regex
LR_SEPARATOR       = re.compile(r'(.*?)([\t ]+)(.*)')
QUOTE_PLUS_WHITE   = re.compile(r'(.*[^"]+)("[\t ]+)(.*)')
# Edit these as needed for non-English p4d error messages
NOLOGIN_REGEX         = re.compile(r'Perforce password \(P4PASSWD\) invalid or unset')
CONNECT_REGEX         = re.compile(r'.*TCP connect to.*failed.*')
CHANGE_UNKNOWN_REGEX  = re.compile(r'Change \d+ unknown')
TRUST_REGEX  = re.compile(r"^.*authenticity of '(.*)' can't.*fingerprint.*p4 trust.*$",
    flags=re.DOTALL)
TRUST_MSG  = _("""
\nThe Git Fusion trigger has not established trust with its ssl enabled server.
Contact your adminstrator and have them run """) + NTR("'p4 trust'.")
# values for "action" argument to update_reviews()
ACTION_REMOVE = NTR('remove')
ACTION_RESET  = NTR('reset')
ACTION_UNSET  = NTR('unset')
ACTION_ADD    = NTR('add')

def mini_usage(invalid=False):
    """Argument help"""
    _usage = ''
    if invalid:
                        # Newline moved out to make l10n.t script easier.
        _usage += _("Unrecognized or invalid arguments.") + "\n"
                        # pylint:disable=W9904
                        # quotation marks part of command line syntax, required.
    _usage += _("""
Usage:
    p4gf_submit_trigger.py --generate-trigger-entries "/absolute/path/to/python[3]" "/absolute/path/to/p4gf_submit_trigger.py" "P4PORT"
    p4gf_submit_trigger.py --set-version-counter P4PORT
    p4gf_submit_trigger.py --reset P4PORT [superuser]
    p4gf_submit_trigger.py --rebuild-all-gf-reviews P4PORT [superuser]
    p4gf_submit_trigger.py --help
""")
                        # pylint:enable=W9904
    print(_usage)
    if invalid:
        print(_("    args: {0}").format(sys.argv))


def get_p4d_version():
    '''
    Return the serverVersion string from 'p4 info':

    P4D/LINUX26X86_64/2012.2.PREP-TEST_ONLY/506265 (2012/08/07)
    '''
    r = p4_run(['info'])
    key = 'serverVersion'
    for e in r:
        if isinstance(e, dict) and key in e:
            p4d_version = e[key]
            p4d_version = p4d_version.split('/')
            m = re.search(r'^(\d+\.\d+)', p4d_version[2])
            p4d_version = m.group(1)
            return p4d_version
    return None

def set_p4d_server_type():
    '''Set the p4d server version settings'''
    global P4D_VERSION, P4D_14_OR_LATER, TRIGGER_TYPES
    P4D_VERSION = get_p4d_version()
    if float(P4D_VERSION) >= 2014.1:
        P4D_14_OR_LATER = True
        TRIGGER_TYPES = TRIGGER_TYPES_14

# pylint: disable=E1101
# has no member
def generate_trigger_entries(generate_args):
    '''Display Git Fusion trigger entries for local paths'''

    global P4PORT
    path_to_python  = generate_args[0]
    path_to_trigger = generate_args[1]
    P4PORT = generate_args[2]
    set_p4d_server_type()
    if P4D_14_OR_LATER:
        trigger_entries = MSG_TRIGGER_SPEC_14_1
    else:
        trigger_entries = MSG_TRIGGER_SPEC

    trigger_entries = trigger_entries.replace(
        '/path/to/' + MSG_TRIGGER_FILENAME, path_to_trigger)
    trigger_entries = trigger_entries.replace(
        '/path/to/python', path_to_python)
    print(trigger_entries)

def usage():
    '''Display full usage.'''
    print (MSG_USAGE)


class ContentTrigger:
    """ContentTrigger class for the change-content trigger.

    This class does most of the trigger work.
    Used for p4 submit, p4 submit -e, p4 populate.

    1) It adds the list of files from the current changelist to
         the Reviews field of the git-fusion-reviews--non-gf user
    2) It calls 'p4 reviews' and passes/fails the submit based
       on the results of a reported collision with Git Fusion Reviews.
    3) If the Reviews fail the submit, the changelist files are removed from
           git-fusion-reviews--non-gf Reviews by this change-content trigger
    4) If it passed the submit, the subsequent change-commit trigger
        removes the same set of files from the Reviews


    There are three key determinations made by the triggers.
    Q1. What is the submit command? - submit, submit -e, or populate.
       This determines (2)
    Q2. Which arguments are used with 'p4 reviews' to determine contention for file updates
    Q3. How are the Review entries, which effect the atomic lock protections
       removed in case a submit fails after the GF change-content trigger succeeds.

    The various versions of p4d supported by Git Fusion ( >= 2012.2) require
    different approaches in determining Q1.

    Q1:
    Prior to p4d 2014.1, the change_submit trigger makes this
    determinations and sets a counter 'git-fusion-non-gf-submit-NN' either
    to 'submit' or 'submit -e'. 'p4 populate does not invoke the
    change_submit trigger, and thus by the counter's absence the trigger
    infers a populate is in progress.

    Beginning with p4d 2014.1, the submit command is passed as a trigger argument.
    This feature obviates the need of the change_submit trigger described just above.

    Q2:
    This issue is not p4d server dependent, but command dependent.
    'submit' and 'submit -e' run with the context of a p4 client.
    As such they may use the optimized 'p4 reviews -c -C' and
    avoid passing a list of files as arguments.
    For 'submit -e' the client name is actually the changelist number.

    'populate' does not run in the context of a client as must pass
    the list of changelist files as arguments.

    Q3:

    Prior to p4d 2014.1, there is no change-failed trigger.
    Thus the removal of locking entries for a submit which fails
    after our change_content trigger succeeds must
    be removed by a following submit, as the change-commit trigger
    is not invoked in case of a later failure by p4d. This is cleanup
    of Reviews is performed by invoking cleanup_previous_submits() with each trigger execution.
    The necessary information for normal or failed cleanup is stored in
    git-fusion-non-gf-submit-NNN counter.
    File entries in Reviews will not be removed for pending changelists.

    Beginning with p4d 2014.1, the addition of the change-failed trigger
    permits the trigger to remove files of the current failed changelist from
    the Reviews of the git-fusion-reviews--non-gf user.

    """

    def __init__(self, change, client, command, args):
        self.change = change
        self.client = client
        self.command = command
        self.args = args
        self.cfiles  = None          # depot files from current change
        self.reviews_file = None     # tmp file used to pass cfiles to p4 reviews for populate
        self.is_locked = False
        self.is_in_union = False
        self.countername = get_trigger_countername(change)
        self.reviews_with_client = True
        self.has_gf_submit_counter = False
        self.submit_type = None
        if P4D_14_OR_LATER:
            # beginning with p4d 2014.1 no change-submit trigger
            # is installed so no counter will exist - yet
            # command type is passed as an argument
            if command == 'user-populate':
                self.submit_type = 'populate'
                self.reviews_with_client = False
            else:
                self.submit_type = 'submit'
                if ((self.command == 'user-submit' and self.args and ('-e' in self.args)) or
                        self.command == 'rmt-SubmitShelf'):
                    self.submit_type = 'submit -e'

        else:
            # for p4d <= 2013.x, change-submit trigger creates
            # the counter for 'p4 submit' and 'p4 submit -e'
            # populate will have no counter because the change-submit trigger is not invoked
            # there is no support for command arguments in <- 2013.x
            submit_type = get_counter(self.countername)
            if str(submit_type) != "0":    # we have counter so we have a submit
                self.submit_type = submit_type
                self.has_gf_submit_counter = True
            else:
                self.submit_type = 'populate'
                self.reviews_with_client = False

        # client is the changelist number for 'submit -e' for calling 'reviews -C client -c change'.
        self.reviews_client = self.change if self.submit_type == 'submit -e' else self.client

    def __str__(self):
        return "\nContentTrigger:\n" + \
            "change:{0} client:{1} command:{2} args {3}". \
             format(self.change, self.client, self.command, self.args) + \
            " reviews_with_client {0} has_counter:{1} submit_type:{2}". \
            format(self.reviews_with_client, self.has_gf_submit_counter, self.submit_type)

    def check_if_locked_by_review(self):
        '''Call the proper p4 reviews methods to check if GF has a lock on these submitted files.'''
        if self.reviews_with_client :
            self.is_locked, self.is_in_union = \
                    get_reviews_using_client(self.change, self.reviews_client)
        else:
            # For populate, 'reviews' requires the list of changelist files
            # which is saved in file 'reviews_file' and passed as a file argument.
            # The file is preserved self.reviews_file for a second reviews call after adding
            # the file list to the git-fusion-reviews--non-gf user.
            self.get_cfiles()
            self.is_locked, self.reviews_file, self.is_in_union = \
                get_reviews_using_filelist(self.cfiles, self.reviews_file)


    def get_cfiles(self):
        ''' Lazy load of files from changelist.'''
        if not self.cfiles:
            self.cfiles = p4_files_at_change(self.change)
        return self.cfiles

    def cleanup_populate_reviews_file(self):
        '''Remove the reviews_file which exist only in the populate case.'''

        # remove the input file to 'p4 -x file reviews'
        if self.reviews_file:
            remove_file(self.reviews_file)

def cleanup_previous_submits():
    """Remove non-Git Fusion Review data for previous submits.
    For each of the trigger counters, remove the reviews and counter
    for changelists which are:
            completed
            pending and not a submit -e and have no files
    """

    counters = p4_run(['counters', '-u', '-e', P4GF_REVIEWS_NON_GF_SUBMIT + '*'])
    for counter in counters:
        if isinstance(counter, dict) and 'counter' in counter:
            value = counter['counter']
            change = value.replace(P4GF_REVIEWS_NON_GF_SUBMIT, '')
            # pylint: disable=W0612
            if is_int(change):
                (submit_type, client, filecount) = \
                        get_submit_counter_data(counter['value'])
                if can_cleanup_change(change):
                    remove_counter_and_reviews(change)

def gf_reviews_user_name_list():
    '''
    Return a list of service user names that match our per-server reviews user.
    '''
    expr = P4GF_REVIEWS_SERVICEUSER.format('*')
    r = p4_run(['users', '-a', expr])
    result = []
    for rr in r:
        if isinstance(rr, dict) and 'User' in rr:
            result.append(rr['User'])
    return result

def user_exists(user):
    '''
    Return True if users exists.
    '''
    r = p4_run(['users', '-a', user])
    for rr in r:
        if isinstance(rr, dict) and 'User' in rr:
            return True
    return False


def p4_write_data(cmd, data, stdout=None):
    """ Execute command with data passed to stdin"""
    cmd = [P4GF_P4_BIN, "-p", P4PORT] + CHARSET + cmd
    process = Popen(cmd, bufsize=-1, stdin=PIPE, shell=False, stdout=stdout)
    pipe = process.stdin
    val = pipe.write(data)
    pipe.close()
    if not stdout is None:
        pipe = process.stdout
        pipe.read()
    if process.wait():
        raise Exception(_('Command failed: %s') % str(cmd))
    return val


def _encoding_list():
    """
    Return a list of character encodings, in preferred order,
    to use when attempting to read bytes of unknown encoding.
    """
    return ['utf8', 'latin_1', 'shift_jis']


def encode(data):
    """
    Attempt to encode using one of several code encodings.
    """

    if not PYTHON3:
        return data

    for encoding in _encoding_list():
        try:
            s = data.encode(encoding)
            return s
        except UnicodeEncodeError:
            pass
        except Exception as e:
            print(str(e))
    # Give up, re-create and raise the first error.
    data.encode(_encoding_list[0])


def decode(bites):
    """
    Attempt to decode using one of several code pages.
    """
    for encoding in _encoding_list():
        try:
            s = bites.decode(encoding)
            return s
        except UnicodeDecodeError:
            pass
        except Exception as e:
            print(str(e))
    # Give up, re-create and raise the first error.
    bites.decode(_encoding_list[0])


def _convert_bytes(data):
    """
    For python3, convert the keys in maps from bytes to strings. Recurses through
    the data structure, processing all lists and maps. Returns a new
    object of the same type as the argument. Any value with a decode()
    method will be converted to a string.
    For python2 - return data
    """
    def _maybe_decode(key):
        """
        Convert the key to a string using its decode() method, if
        available, otherwise return the key as-is.
        """
        return decode(key) if 'decode' in dir(key) else key

    if not PYTHON3:
        return data

    if isinstance(data, dict):
        newdata = dict()
        for k, v in data.items():
            newdata[_maybe_decode(k)] = _convert_bytes(v)
    elif isinstance(data, list):
        newdata = [_convert_bytes(d) for d in data]
    else:
        # convert the values, too
        newdata = _maybe_decode(data)
    return newdata


def p4_print(depot_path):
    """Accumulate multiple 'data' entries to assemble content
    from p4 print
    """

    result = p4_run(['print', '-q', depot_path])
    contents = ''
    for item in result:
        if 'data' in item and item['data']:
            contents += item['data']
    return contents


_unicode_error = [{'generic': 36,
                   'code': NTR('error'),
                   'data': _('Unicode server permits only unicode enabled clients.\n'),
                   'severity': 3}]

# pylint: disable=R0912
# Too many branches
def p4_run(cmd, stdin=None, user=P4GF_USER):
    """Use the -G option to return a list of dictionaries."""
    raw_cmd = cmd
    global CHARSET
    while True:
        cmd = [P4GF_P4_BIN, "-p", P4PORT, "-u", user, "-G"] + CHARSET + raw_cmd
        try:
            process = Popen(cmd, shell=False, stdin=stdin, stdout=PIPE, stderr=PIPE)
        except (OSError, ValueError) as e:
            print(_("Error calling Popen with cmd: {0}").format(cmd))
            print(_("Error: {0}").format(e))
            sys.stdout.flush()
            sys.exit(1)

        data = []
        try:
            while True:
                data.append(marshal.load(process.stdout))
        except EOFError:
            pass
        ret = process.wait()
        if data:
            data = _convert_bytes(data)
        if ret != 0:
            # check for unicode error:
            if (not CHARSET) and (not stdin) and data == _unicode_error:
                #set charset and retry
                CHARSET = ['-C', 'utf8']
                continue

            else:
                error = process.stderr.read().splitlines()
                if error and len(error) > 1:
                    for err in error:
                        if CONNECT_REGEX.match(_convert_bytes(err)):
                            print (_("Cannot connect to P4PORT: {0}").format(P4PORT))
                            sys.stdout.flush()
                            # pylint: disable=W0212
                            os._exit(P4FAIL)
                            # pylint: enable=W0212
            data.append({"Error": ret})
        break
    if len(data) and 'code' in data[0] and data[0]['code'] == 'error':
        if NOLOGIN_REGEX.match(data[0]['data']):
            print(_("\nGit Fusion Submit Trigger user '{0}' is not logged in.\n{1}").
                    format(user, data[0]['data']))
            sys.exit(P4FAIL)
        m = TRUST_REGEX.match(data[0]['data'])
        if m:
            print(TRUST_MSG)
            sys.exit(P4FAIL)
    if ret:
        print (_("Error in Git Fusion Trigger \n{0}").format(data[0]['data']))
        sys.exit(P4FAIL)
    return data

def p4_run_ztag(cmd, stdin=None, user=P4GF_USER):
    """Call p4 using the -ztag option to stdout.

    This is required to avoid sorting dictionary data when
    calling p4 reviews.
    """
    raw_cmd = cmd
    cmd = [P4GF_P4_BIN, "-p", P4PORT, "-u", user, "-ztag"] + CHARSET + raw_cmd
    try:
        process = Popen(cmd, shell=False, stdin=stdin, stdout=PIPE, stderr=PIPE)
    except (OSError, ValueError) as e:
        print(_("Error calling Popen with cmd: {0}").format(cmd))
        print(_("Error: {0}").format(e))
        sys.stdout.flush()
        sys.exit(1)

    data = []
    count = 0
    while True:
        count +=1
        if count > 100:
            break
        line = process.stdout.readline().strip()
        line = _convert_bytes(line)
        if line != '':
            data.append(line[4:])
        else:
            break
    ret = process.wait()
    if data:
        data = _convert_bytes(data)
    if ret:
        print (_("Error in Git Fusion Trigger \n{0}").format(data[0]['data']))
        sys.exit(P4FAIL)
    return data
# pylint: enable=E1101,R0912


def is_super(user):
    """Determine if user is a super user"""
    results = p4_run(['protects', '-u',  user], user=user)
    for r in results:
        if 'code' in r and r['code'] == 'error':
            return False
        if 'perm' in r and r['perm'] == 'super':
            return True
    return False


def set_counter(name, value):
    """Set p4 counter"""
    p4_run(['counter', '-u', name, value])


def inc_counter(name, user=P4GF_USER):
    """Increment p4 counter."""
    counter = p4_run(['counter', '-u', '-i', name], user=user)[0]
    return counter['value']


def delete_counter(name, user=P4GF_USER):
    """Delete p4 counter."""
    p4_run(['counter', '-u', '-d', name], user=user)

def delete_counter_ifexists(name, user=P4GF_USER):
    """Delete p4 counter if exists."""
    if get_counter(name, user) != '0':
        delete_counter(name, user)

def get_counter(name, user=P4GF_USER):
    """Get p4 counter."""
    counter = p4_run(['counter', '-u',  name], user=user)[0]
    return counter['value']


def get_counter_lock(name, user=P4GF_USER):
    """Increment and test counter for value == 1."""
    return '1' == inc_counter(name, user=user)


def counter_exists(name):
    """Boolean on counter exists"""
    return str(get_counter(name)) != "0"


def release_counter_lock(name, user=P4GF_USER):
    """Delete counter lock."""
    delete_counter_ifexists(name, user)


def get_local_depots():
    """Get list of local depots"""
    depot_pattern = re.compile(r"^" + re.escape(P4GF_DEPOT))
    data = p4_run(['-ztag', 'depots'])
    depots = []
    for depot in data:
        if (    (depot['type'] == 'local' or depot['type'] == 'stream')
            and not depot_pattern.search(depot['name'])):
            depots.append(depot['name'])
    return depots


def p4_files_at_change(change):
    """Get list of files in changelist

    p4 files@=CNN provides a valid file list during the change_content trigger.

    """
    depot_files = []
    depots = get_local_depots()
    for depot in depots:
        cmd = ['files']
        cmd.append("//{0}/...@={1}".format(depot, change))
        data = p4_run(cmd)
        for item in data:
            if 'depotFile' in item:
                depot_files.append(enquote_if_space(item['depotFile']))
    return depot_files


def is_int(candidate):
    '''Is the candidate an int?'''
    try:
        int(candidate)
        return True
    except ValueError:
        return False

def can_cleanup_change(change):
    '''Determine whether the Reviews may be cleaned from a changelist'''
    if not is_int(change):
        return False

    data = p4_run(['describe', '-s', change])[0]
    if not data:
        print("can_clean change {0} does not exist".format(change))
        return True

    if 'code' in data and data['code'] == 'error' and 'data' in data:
        if re.search('no such changelist', data['data']):
            return True
        else:
            raise Exception(_("error in describe for change {0}: {1}").format(change, data))

    submitted = False
    pending = False
    no_files = True

    shelved = 'shelved' in data
    if 'status' in data:
        pending   = data['status'] == 'pending'
        submitted = data['status'] == 'submitted'
    if not shelved and pending:
        if 'depotFile0' in data:
            no_files = False
        else:
            no_files = len(p4_files_at_change(change)) == 0

    # These tests enumerate the conditions permitting cleanup of Reviews
    # Test order is critical
    if pending and shelved:    # pending shelf
        return False
    if pending and no_files:   # pending non-shelf with no files
        return True
    if submitted:              # not pending
        return True
    return False


def unlock_changelist(changelist, client):
    """Unlock the files in the failed changelist so GF may continue.

    Called as git-fusion-user with admin priviledges.
    """
    p4_run(['-c', client, 'unlock', '-f', '-c', changelist ])


def delete_all_counters():
    """Delete all non-Git Fusion counters."""
    counters = p4_run(['counters', '-u', '-e', P4GF_REVIEWS_NON_GF_RESET + '*'])
    for counter in counters:
        if 'counter' in counter:
            delete_counter(counter['counter'])


def remove_file(file_):
    """Remove file from file system."""
    try:
        os.remove(file_.name)
    except IOError:
        pass


def check_heartbeat_alive(heartbeat):
    """Compares the time value in the lock contents to the current time
    on this system (clocks must be synchronized closely!) and if the
    difference is greater than HEARTBEAT_TIMEOUT_SECS then assume the lock
    holder has died.

    Returns True if lock is still valid, and False otherwise.
    """
    try:
        then = int(re.split(NTR(r'\s'), heartbeat)[4])

    except ValueError:
        return False
    now = calendar.timegm(time.gmtime())
    return now < then or (now - then) < HEARTBEAT_TIMEOUT_SECS


def gf_has_fresh_heartbeat():
    """ Examine all heartbeats. If any is alive
    then return True - else False
    """
    heartbeats = p4_run(['counters', '-u', '-e', P4GF_HEARTBEATS ])
    have_alive_heartbeat = False
    for heartbeat in heartbeats:
        if check_heartbeat_alive(heartbeat['value']):
            have_alive_heartbeat = True
            break
    return have_alive_heartbeat


def find_depot_prefixes(depot_paths):
    """ For each depot, find the longest common prefix """
    prefixes = {}
    if not depot_paths:
        return prefixes
    last_prefix = None
    depot_pattern = re.compile(r'^//([^/]+)/')
    for dp in depot_paths:
        dp = dequote(dp)
        # since depot_paths is probably sorted, it's very likely
        # the current depot_path starts with the last found prefix
        # so check that first and avoid hard work most of the time
        if last_prefix and dp.startswith(last_prefix):
            continue
        # extract depot from the path and see if we already have a prefix
        # for that depot
        m = depot_pattern.search(dp)
        depot = m.group(1)
        depot_prefix = prefixes.get(depot)
        if depot_prefix:
            prefixes[depot] = last_prefix = os.path.commonprefix([depot_prefix, dp])
        else:
            prefixes[depot] = last_prefix = dp
    return prefixes.values()

def get_depot_patterns(depot_path_list):
    """ Generate the reviews patterns for file list """
    return [enquote_if_space(p + "...") for p in find_depot_prefixes(depot_path_list)]


def get_reviews_using_filelist(files, ofile=None):
    """Check if locked files in changelist are locked by GF in Reviews."""
    is_locked = False
    common_path_files = get_depot_patterns(files)
    if not ofile:
        ofile = write_lines_to_tempfile(NTR("islocked"), common_path_files)
    #else use the ofile which is passed in

    cmd = NTR(['-x', ofile.name, 'reviews'])
    users = p4_run(cmd)
    change_is_in_union = False
    for user in users:
        if 'code' in user and user['code'] == 'error':
            raise Exception(user['data'])
        _user = user['user']
        if _user.startswith(P4GF_REVIEWS_GF):
            if _user == P4GF_REVIEWS__ALL_GF:
                change_is_in_union = True
            elif _user != P4GF_REVIEWS__NON_GF:
                if gf_has_fresh_heartbeat():
                    print (MSG_LOCKED_BY_GF.format(user['user']))
                    # reject this submit which conflicts with GF
                    change_is_in_union = True
                    is_locked =  True
                    break
    return  (is_locked, ofile, change_is_in_union)


def get_reviews_using_client(change, client):
    """Check if locked files in changelist are locked by GF in Reviews."""
    is_locked = False

    cmd = NTR(['reviews', '-C', client, '-c', change])
    users = p4_run(cmd)
    change_is_in_union = False
    for user in users:
        if 'code' in user and user['code'] == 'error':
            raise Exception(user['data'])
        _user = user['user']
        if _user.startswith(P4GF_REVIEWS_GF):
            if _user == P4GF_REVIEWS__ALL_GF:
                change_is_in_union = True
            elif _user != P4GF_REVIEWS__NON_GF:
                if gf_has_fresh_heartbeat():
                    print (MSG_LOCKED_BY_GF.format(user['user']))
                    # reject this submit which conflicts with GF
                    change_is_in_union = True
                    is_locked =  True
                    break
    return  (is_locked, change_is_in_union)


def set_submit_counter(countername, file_count, submit_type, client):
    """Set submit counter using -x file input"""
    value = "{0}{1}{2}{3}{4}".format(
        submit_type,
        SEPARATOR,
        client,
        SEPARATOR,
        file_count)
    set_counter(countername, value)


def write_lines_to_tempfile(prefix_, lines):
    """Write list of lines to tempfile."""
    file_ = tempfile.NamedTemporaryFile(prefix='p4gf-trigger-' + prefix_, delete=False)
    for line in lines:
        ll = "%s\n" % dequote(line)
        file_.write(encode(ll))
    file_.flush()
    file_.close()
    return file_


def enquote_if_space(path):
    """Wrap path is double-quotes if SPACE in path."""
    if ' ' in path and not path.startswith('"'):
        path = '"' + path + '"'
    return path


def dequote(path):
    """Remove wrapping double quotes"""
    if path.startswith('"'):
        path = path[1:-1]
    return path

def shelved_files(change):
    ''' Return list of shelved files.'''
    cfiles = []
    shelved_data = p4_run(['describe', '-S', change])[0]
    for key, value in shelved_data.items():
        if key.startswith('depotFile'):
            cfiles.append(enquote_if_space(value))
    return cfiles


def update_userspec(userspec, user, p4user=P4GF_USER):
    """Reset P4 userspec from local userspec dictionary."""
    newspec = ""
    for key, val in userspec.items():
        if key == 'Reviews':
            reviews = '\n' + key + ":\n"
            for line in val.splitlines():
                reviews = reviews + "\t" + line + "\n"
        else:
            newspec = "{0}\n{1}:\t{2}".format(newspec, key, val)

    newspec = newspec + reviews
    file_ = tempfile.NamedTemporaryFile(prefix='p4gf-trigger-userspec', delete=False)
    line = "%s" % newspec
    file_.write(encode(line))
    file_.close()      # not deleted - so windows can re-open this file under p4
    if p4user != P4GF_USER:  # assume this is the super user as called by p4gf_super_init
        # Called by p4gf_super_init as 'super' to  --rebuild-all-gf-reviews
        command = NTR("{0} -p {1} {2} -u {3} user -f  -i < {4}")\
                  .format(P4GF_P4_BIN, P4PORT,
                          ' '.join(CHARSET), p4user, file_.name)
    else:
        command = NTR("{0} -p {1} {2} -u {3} user -i < {4}")\
                  .format(P4GF_P4_BIN, P4PORT,
                          ' '.join(CHARSET), user, file_.name)
    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    stderr_data = p.communicate()[1]
    # pylint: disable=E1101
    # has no member 'returncode'
    if p.returncode:
        print ("{0}".format(stderr_data.decode('utf-8')))
        print(MSG_MALFORMED_CONFIG)
    # pylint: enable=E1101
    try:
        os.remove(file_.name)
    except IOError:
        pass


def append_reviews(user, depot_files, change, p4user=P4GF_USER):
    '''Add the files to Reviews to the user's user spec.'''
    update_reviews(user, depot_files, change, action=ACTION_ADD, p4user=p4user)


def remove_reviews(user, change, p4user=P4GF_USER):
    '''Remove the files to Reviews to the user's user spec.'''
    update_reviews(user, None, change, action=ACTION_REMOVE, p4user=p4user)


def reset_reviews(user, depot_files, p4user=P4GF_USER):
    '''Add the files to Reviews to the user's user spec.'''
    update_reviews(user, depot_files, None, action=ACTION_RESET, p4user=p4user)


def unset_reviews(user, p4user=P4GF_USER):
    '''Remove all files Reviews from the user's user spec.'''
    update_reviews(user, None, None, action=ACTION_UNSET, p4user=p4user)


def review_path_in_changelist(path):
    """ Return True if path lies between (inclusive) the GF change markers.
    The path argument is passed in the list sequence from Reviews.
    """

    global CHANGE_FOUND_BEGIN, CHANGE_FOUND_END
    if not CHANGE_FOUND_BEGIN:
        if path == GF_BEGIN_MARKER:
            CHANGE_FOUND_BEGIN =True
            return True
        else:
            return False
    else:
        if CHANGE_FOUND_END:
            return False
        else:
            if path == GF_END_MARKER:
                CHANGE_FOUND_END  = True
            return True



def update_reviews(user, depot_files, change, action=ACTION_ADD, p4user=P4GF_USER):
    """
    Add or remove Reviews to the user spec

    add == Add the set of files
    remove == Remove the set of files
    unset == Set Reviews to none
    reset   == Set Reviews to these files
    """
    # pylint: disable=R0912
    # Too many branches
    global GF_BEGIN_MARKER,  GF_END_MARKER, CHANGE_FOUND_BEGIN, CHANGE_FOUND_END
    thisuser = p4user if p4user != P4GF_USER else user
    userspec = p4_run_ztag(['user', '-o', user], user=thisuser)
    newspec = {}
    current_reviews = []
    # Fetch the current reviews from userspec which contains the 'ReviewsNNN' fields
    # And the other fields into a dictionary
    for item in userspec:
        space = item.find(' ')
        if not item.startswith('Review'):
            newspec[item[:space]] = item[space:]
        else:
            current_reviews.append(item[space:].strip())

    if action == ACTION_UNSET:
        newspec['Reviews'] = '\n'     # Set to empty
    else:
        if action == ACTION_ADD:
            if user == P4GF_REVIEWS__NON_GF:
                current_reviews.append(NON_GF_REVIEWS_BEGIN_MARKER_PATTERN.format(change))
            current_reviews += depot_files
            if user == P4GF_REVIEWS__NON_GF:
                current_reviews.append(NON_GF_REVIEWS_END_MARKER_PATTERN.format(change))
        elif action == ACTION_RESET:
            current_reviews = depot_files
        else:   # remove by change is only called for P4GF_REVIEWS__NON_GF
            CHANGE_FOUND_BEGIN = False
            CHANGE_FOUND_END = False
            GF_BEGIN_MARKER = NON_GF_REVIEWS_BEGIN_MARKER_PATTERN.format(change)
            GF_END_MARKER = NON_GF_REVIEWS_END_MARKER_PATTERN.format(change)
            current_reviews = [x for x in current_reviews if not review_path_in_changelist(x)]
        if len(newspec) > 0:
            newspec['Reviews'] = '\n'.join(current_reviews)
    update_userspec(newspec, user, p4user=p4user)


def add_non_gf_reviews(content_trigger):
    """Add files in changelist to Reviews of non-GF user.

    trigger type determines name of counter
    and method of getting list of files from changelist
    """
    set_submit_counter(content_trigger.countername, len(content_trigger.cfiles),
            content_trigger.submit_type, content_trigger.client)
    append_reviews(P4GF_REVIEWS__NON_GF, content_trigger.cfiles, content_trigger.change)


def remove_counter_and_reviews(change):
    """Remove counter and its reviews from non-gf user Spec"""

    counter = get_trigger_countername(change)
    remove_reviews(P4GF_REVIEWS__NON_GF, change)
    delete_counter(counter)


def get_trigger_countername(change):
    ''' Get the counter name.'''
    countername = P4GF_REVIEWS_NON_GF_SUBMIT + change
    return countername


def change_submit_trigger(change, client):
    '''
    Store the trigger_type in P4GF_REVIEWS_NON_GF_SUBMIT-NNN.

    Configured in p4 triggers for p4d <= 2013.x
    This informs the following change_content trigger
    that a submit initiated these GF triggers - not populate.
    All collision detection work will be done in the change_content trigger.
    If this counter is not detected in the change_content trigger, then
    we have a 'p4 populate' event.
    This counter will be updated in the change_content_trigger() by either:
            1) replacing the value with the list of change list files
         or 2) removing the counter if the filelist if not in the all-gf union
    '''
    # This trigger should not be installed for p4d >= 2014.1.
    # Report message is this is not the case
    if P4D_14_OR_LATER:
        print (_("GF-change-submit trigger should not be installed on p4d servers " + \
                 "version 2014.1 or greater."))
        print (_("It is a noop on 2014.1. The submit will proceed unaffected."))
        print (_("Please contact your Perforce administrator"))
        return P4PASS

    returncode = P4PASS
    counter = get_trigger_countername(change)
    # In the case of 'submit -c NNN' remove the preceding counter and reviews
    # And then reset the counter
    # pylint:disable=W0703
    lock_acquired = False
    try:
        value = get_counter(counter)
        if str(value) != "0":    # valid non-gf counter
            acquire_counter_lock(P4GF_REVIEWS__NON_GF)
            lock_acquired = True
            remove_counter_and_reviews(change)
    except Exception as exce:
        print (MSG_POST_SUBMIT_FAILED)
        print (exce.args)
        returncode = P4FAIL
    finally:
        if lock_acquired:
            release_counter_lock(P4GF_REVIEWS__NON_GF)


    # For <= 13.x populate does not invoke the change-submit trigger
    # Only 'submit' - not 'submit -e' - has opened files
    # Set the counter value with the command type.
    # The change-content trigger will reset the counter with the list of files
    data = p4_run(['opened', '-m', '1' , '-c',  change, '-C', client])
    submit_command = 'submit' if data else 'submit -e'
    counter = get_trigger_countername(change)
    p4_run(['counter', '-u', counter, submit_command])
    return returncode

# pylint: disable=R0912
# Too many branches
def change_content_trigger(change, client, command, args):
    """Reject p4 submit if change overlaps Git Fusion push.


    Beginning with 13.1 p4d 'p4 reviews' added the -C option.
      'The -C flag limits the files to those opened in the specified clients
       workspace,  when used with the -c flag limits the workspace to files
       opened in the specified changelist.'

    Using this option eliminates need for the files argument to 'p4 reviews'.
    However calls to 'p4 populate' may not take advantage of this featurer,
    there being no workspace associated with the populate.
    Thus triggers for 'p4 submit' and 'p4 populate' must handle the 'p4 reviews'
    differently.


    Additionally for p4d <= 2013.x, p4 populate does not engage the change-submit trigger.
    Thus Git Fusion uses the change-submit trigger to do no more than
    set a counter to distinguish submit and submit -e from populate.
    The following change-content trigger uses this counter to
    select the method of calling p4 reviews and resets its value with
    the list of changelist files.. All other work is identical.
    ContentTrigger class contains all data and p4 calls to handle
    the GF collision detection.

    Beginning with p4d 14.1, the submit command is available in the trigger arguments.
    Thus the change-submit trigger is not used.
    """

    returncode = P4PASS
    counter_lock_acquired = False
    content_trigger = None

    try:
        # set methods and data for this change_content trigger
        content_trigger = ContentTrigger(change, client, command, args)
        content_trigger.check_if_locked_by_review()
        if content_trigger.is_locked:
            # Already locked by GF, is this current command a submit?
            # then remove now unneeded placeholder counter
            if content_trigger.has_gf_submit_counter:
                delete_counter(content_trigger.countername)
            # Now reject this submit  before we add any Reviews data
            returncode = P4FAIL
        elif content_trigger.is_in_union:   # needs protection from GF
            # Get the change list files into content_trigger.cfiles
            content_trigger.get_cfiles()

            # Now get the user spec lock
            acquire_counter_lock(P4GF_REVIEWS__NON_GF)
            counter_lock_acquired = True
            if not P4D_14_OR_LATER:
                cleanup_previous_submits()
            # add our Reviews
            add_non_gf_reviews(content_trigger)
            # now check again
            content_trigger.check_if_locked_by_review()
            if content_trigger.is_locked:
                # Locked by GF .. so remove the just added locked files from reviews
                remove_counter_and_reviews(content_trigger.change)
                returncode = P4FAIL
        # not locked and not is_in_union do nothing
        # unless this was a submit - we need to remove the its submit counter
        # which marked this submit started in the change_submit trigger
        elif content_trigger.has_gf_submit_counter:
            delete_counter(content_trigger.countername)
    # pylint: disable=W0703
    # Catch Exception
    except Exception as exce:
        print (MSG_PRE_SUBMIT_FAILED)
        print (_("Exception: {0}").format(exce))
        returncode = P4FAIL
    # pylint: enable=W0703
    finally:
        if content_trigger:
            content_trigger.cleanup_populate_reviews_file()
        if counter_lock_acquired:
            release_counter_lock(P4GF_REVIEWS__NON_GF)
        if returncode == P4FAIL:
            # p4 unlock the files so that GF may proceed
            unlock_changelist(change, client)
    return returncode
# pylint: enable=R0912

def _read_string(config, depot_path_msg_only, contents):
    '''
    If unable to parse, convert generic ParseError to one that
    also contains a path to the unparsable file.
    '''
    if PYTHON3:
        try:
            config.read_string(contents)
            return True
        except PARSING_ERROR as e:
            msg = _("Unable to read config file {0}.\n{1}").format(depot_path_msg_only, e)
            print(msg)
            return False

    else:
        try:
            infp = cStringIO.StringIO(str(contents))
            config.readfp(infp)
            return True
        except PARSING_ERROR as e:
            msg = _("Unable to read config file {0}.\n{1}").format(depot_path_msg_only, e)
            print(msg)
            return False


def get_lhs(view_, file_path):

    '''Extract the left map from the a config view line
    If the left map starts with " it may not contain embedded quotes
    If the left map does not start with " it may contain embedded quotes
    If the left map starts with " only then may it contain embedded space
    '''
    view = view_.strip()
    quote = '"'
    quote_r = -1
    quoted_view = False
    double_slash = view.find('//')
    quote_l = view.find(quote)
    lhs = None
    if quote_l > -1 and quote_l < double_slash:
        quoted_view = True

    if quoted_view:
        search = QUOTE_PLUS_WHITE.search(view[quote_l+1:])
        if search:
            quote_r = search.start(2)
            lhs = view[:quote_r+ + quote_l + 2]   # +2 because the search started at 1 (not 0)
        else:
            msg = _("didn't find end of quote : for '{0}' in '{1}'").format(view, file_path)
            print(msg)

    else:
        search = LR_SEPARATOR.search(view)
        if search:
            lhs = search.groups()[0]
    return lhs


def get_repo_views(file_path, contents):
    """Return array of left maps from p4gf_config file.
    contents == array
    """
    all_views = []

    if PYTHON3:
        config = configparser.ConfigParser(interpolation=None, strict=False)
    else:
        config = ConfigParser.RawConfigParser()
    valid_config = _read_string(config, depot_path_msg_only = file_path
                               , contents= str(contents))
    if not valid_config:
        return all_views   # invalid config so so nothing

    branches  = [ sec for sec in config.sections() if not sec.startswith('@')]
    view_lines = []
    for s in branches:
        if config.has_option(s, KEY_VIEW):
            view_lines = config.get(s, KEY_VIEW)
        # pylint: disable=E1103
        # has no member
        if isinstance(view_lines, str):
            view_lines = view_lines.splitlines()
        # pylint: enable=E1103
        # Common: first line blank, view starts on second line.
        if view_lines and not len(view_lines[0].strip()):
            del view_lines[0]

        for v in view_lines:
            lhs = get_lhs(v, file_path)
            if lhs and not (lhs.startswith('-') or lhs.startswith('"-')):
                all_views.append(lhs)

    return all_views


def rebuild_all_gf_reviews(user=P4GF_USER):
    """Rebuild git-fusion-reviews--all-gf Reviews from //P4GF_DEPOT/repos/*/p4gf_config.
    """
    returncode = P4PASS
    action = ACTION_RESET
    config_files = []
    repo_views = []

    # pylint:disable=W0703
    # Catch Exception
    try:
        acquire_counter_lock(P4GF_REVIEWS__ALL_GF, user=user)

        # Get list of all repos/*/p4gf_config files
        data = p4_run(['files', '//{0}/repos/*/p4gf_config'.format(P4GF_DEPOT)], user=user )
        for _file in data:
            if 'depotFile' in _file and 'action' in _file:
                if not 'delete' in _file['action']:
                    config_files.append(_file['depotFile'])

        # if no p4gf_config files - then remove all views
        if not config_files:
            action = ACTION_UNSET

        # From each p4gf_config file extract the views with a regex
        for depot_file in config_files:
            contents = p4_print(depot_file)
            views = get_repo_views(depot_file, contents)
            if views and len(views):
                repo_views.extend(views)
                # Report to caller - not invoked as a trigger
                repo_l = len('//{0}/repos/'.format(P4GF_DEPOT))
                repo_r = depot_file.rfind('/')
                print(_("Rebuild '{0}' Reviews: adding repo views for '{1}'")
                        .format(P4GF_REVIEWS__ALL_GF, depot_file[repo_l:repo_r]))

        if not repo_views:
            action = ACTION_UNSET

        if repo_views or action == ACTION_UNSET:
            update_reviews(P4GF_REVIEWS__ALL_GF, repo_views, None
                          , action=action, p4user=user)
    except Exception as exce:
        print (_("Exception: {0}").format(exce))
        returncode = P4FAIL
    finally:
        release_counter_lock(P4GF_REVIEWS__ALL_GF, user=user)
    return returncode

def add_repo_views_to_union(change, user=P4GF_USER):
    """Add all views in a p4gf_config to the P4GF_REVIEWS__ALL_GF Reviews user.

    Do not consider p4gf_config2 files - as these views are not user accessbible.
    Currently deletes are ignored. Reviews grow until recreate with --rebuild-all-gf-reviews.
    """
    config_files = []
    repo_views = []

    # Get the changelist file set
    data = p4_run(['describe', '-s', change])[0]

    # check against this regex for the p4gf_config file
    config_pattern = re.compile(r'^//' + re.escape(P4GF_DEPOT) + '/repos/[^/]+/p4gf_config$')
    for key, value in data.items():
        if key.startswith('depotFile'):
            action_key = key.replace('depotFile','action')
            if not 'delete' in data[action_key]:
                if config_pattern.match(value):
                    config_files.append(enquote_if_space(value))

    # From each p4gf_config file extract the views with a regex
    for depot_file in config_files:
        contents = p4_print(depot_file)
        views = get_repo_views(depot_file, contents)
        if views and len(views):
            repo_views.extend(views)

    # Add to Reviews
    if repo_views:
        append_reviews(P4GF_REVIEWS__ALL_GF, repo_views, None
                      , p4user=user)



def change_commit_p4gf_config(change, user=P4GF_USER):
    """Post submit trigger on changes //P4GF_DEPOT/repos/*/p4gf_config.

    Add p4gf_config views to git-fusion-reviews--all-gf Reviews:.
    """
    returncode = P4PASS
    # pylint:disable=W0703
    # Catch Exception

    try:
        acquire_counter_lock(P4GF_REVIEWS__ALL_GF, user=user)
        add_repo_views_to_union(change, user=user)
    except Exception as exce:
        print (MSG_PRE_SUBMIT_FAILED)
        print (_("Exception: {0}").format(exce))
        returncode = P4FAIL
    finally:
        release_counter_lock(P4GF_REVIEWS__ALL_GF, user=user)
    return returncode

def get_submit_counter_data(counter_value):
    """Extract the data from the submit counter."""
    submit_type = None
    client = None
    filecount = 0
    if counter_value:
        data = counter_value.split(SEPARATOR)
        submit_type = data[0]
        if len(data) >= 2:
            client = data[1]
            try:
                filecount  = int(data[2])
            except ValueError:
                pass

    return (submit_type, client, filecount)


def change_commit_trigger(change):
    """Post-submit trigger for Git Fusion.

    Cleanup files from reviews for non-GF user.
    Main calls this with the old changelist
    """
    # pylint: disable=W0612
    # unused values ... subit_type, client
    returncode = P4PASS
    lock_acquired = False
    # pylint: disable=W0703
    try:
        countername = get_trigger_countername(change)
        value = get_counter(countername)
        acquire_counter_lock(P4GF_REVIEWS__NON_GF)
        lock_acquired = True
        if str(value) != "0":    # valid non-gf counter
            # the first 2 counter array items are submit_type and client
            (submit_type, client, filecount) = get_submit_counter_data(value)
            remove_counter_and_reviews(change)
        else:
            # counter does not exist - likely a renamed one if p4d before 2014.x.
            # for populate with 13.x the old changelist is incorrectly set to new changelist
            # thus we must rely on the heuristic cleanup approach
            # for 14.x the old changelist parameter is accurate and no cleanup code is required
            if not P4D_14_OR_LATER:
                cleanup_previous_submits()
    except Exception as exce:
        print (MSG_POST_SUBMIT_FAILED)
        print (exce.args)
        returncode = P4FAIL
    finally:
        if lock_acquired:
            release_counter_lock(P4GF_REVIEWS__NON_GF)
    return returncode


def change_failed_trigger(change):
    """Post-submit trigger for Git Fusion.

    Cleanup files from reviews for non-GF user.
    """
    return change_commit_trigger(change)


def acquire_counter_lock(name, user=P4GF_USER):
    """Get Reviews lock for non-gf user."""
    while True:
        if get_counter_lock(name, user):
            return
        time.sleep(_RETRY_PERIOD)


def reset_all(user):
    """Tool to remove all GF and trigger Reviews and counters"""
    print(_("Removing all non-Git Fusion initiated reviews and counters"))
    # pylint: disable=W0703
    # Catch Exception
    delete_all_counters()
    for counter in [ P4GF_COUNTER_PRE_TRIGGER_VERSION
                   , P4GF_COUNTER_POST_TRIGGER_VERSION ]:
        set_counter( counter,
            "{0} : {1}".format(P4GF_TRIGGER_VERSION, datetime.datetime.now()))
    for user_name in gf_reviews_user_name_list():
        if user_name != P4GF_REVIEWS__ALL_GF:   # preserve the all-gf reviews
            unset_reviews(user_name, p4user=user)
        release_counter_lock(user_name)

def set_version_counter():
    ''' Reset the Git Fusion Trigger version counters.'''
    validate_port()
    if not user_exists(P4GF_USER):
        print (_("'{0}' does not exist. Have you run p4gf_super_init.py? Exiting.").
                format(P4GF_USER))
        sys.exit(P4FAIL)
    _version = "{0} : {1}".format(P4GF_TRIGGER_VERSION, datetime.datetime.now())
    set_counter(P4GF_COUNTER_PRE_TRIGGER_VERSION, _version)
    set_counter(P4GF_COUNTER_POST_TRIGGER_VERSION, _version)
    print (_("Setting '{0}' = '{1}'").format(P4GF_COUNTER_PRE_TRIGGER_VERSION, _version))
    print (_("Setting '{0}' = '{1}'").format(P4GF_COUNTER_POST_TRIGGER_VERSION, _version))
    sys.exit(P4PASS) # Not real failure but trigger should not continue

def validate_port():
    """Calls sys_exit if we cannot connect."""
    colon = re.match(r'(.*)(:{1,1})(.*)', P4PORT)
    if colon:
        port = colon.group(3)
    else:
        port = P4PORT
    if not port.isdigit():
        print(_("Server port '{0}' is not numeric. Stopping.").format(P4PORT))
        print(_("args: {0}").format(sys.argv))
        sys.exit(P4FAIL)
    p4_run(["info"])

def get_user_from_args(option_args, super_user_index=None):
    '''Return P4GF_USER or super user if present'''
    validate_port()  # uses global P4PORT
    user = P4GF_USER
    if super_user_index and len(option_args) == super_user_index+1:
        super_user = option_args[super_user_index]
    else:
        super_user = None
    if super_user:
        if not is_super(super_user):
            print (_("'{0}' is not super user. Exiting.").format(super_user))
            sys.exit(P4FAIL)
        else:
            user = super_user
    return user

class Args:
    '''an argparse-like class to receive arguments from
    getopt parsing.
    '''
    def __init__(self):
        self.reset                    = None
        self.rebuild_all_gf_reviews   = None
        self.set_version_counter      = None
        self.generate_trigger_entries = None
        self.optional_command         = False
        self.oldchangelist            = None
        self.trigger_type             = None
        self.change                   = None
        self.user                     = None
        self.client                   = None
        self.serverport               = None
        self.command                  = None   # parsing invalid value will remain = None
        self.args                     = None   # parsing invalid value will remain = None
        self.parameters               = []

    def __str__(self):
        return '  '.join(self.parameters)

    def __repr__(self):
        return self.__str__()


def display_usage_and_exit(mini=False, invalid=False):
    '''Display mini or full usage.'''
    if mini:
        mini_usage(invalid)
    else:
        usage()
    sys.stdout.flush()
    if invalid:
        sys.exit(1)
    else:
        sys.exit(0)


def validate_option_or_exit(minimum, maximum, positional_len):
    '''Validate option count.'''
    if positional_len >= minimum and positional_len <= maximum:
        return True
    else:
        display_usage_and_exit(True, True)

# pylint: disable=R0912
# Too many branches
def parse_argv():
    '''Parse the command line options. '''
    trigger_opt_base_count = 5
    args = Args()
    short_opt = 'h'
    long_opt = NTR(['reset', 'rebuild-all-gf-reviews',
                'set-version-counter', 'generate-trigger-entries', 'help'])
    try:
        options, positional = getopt.getopt(sys.argv[1:], short_opt, long_opt)
    except getopt.GetoptError as err:
        print(_("Command line options parse error: {0}").format(err))
        display_usage_and_exit(True, True)

    positional_len = len(positional)
    options_len     = len(options)
    if options_len > 1 :
        display_usage_and_exit(True, True)
    elif options_len == 1:
        args.optional_command = True
        opt = options[0][0]
        if opt in ("-h", "--help"):
            display_usage_and_exit(opt == '-h')
        elif opt == "--reset" and validate_option_or_exit(1, 2, positional_len):
            args.reset = positional
        elif opt == "--rebuild-all-gf-reviews" and validate_option_or_exit(1, 2, positional_len):
            args.rebuild_all_gf_reviews = positional
        elif opt == "--set-version-counter" and validate_option_or_exit(1, 1, positional_len):
            args.set_version_counter = positional
        elif  opt == "--generate-trigger-entries" and validate_option_or_exit(3, 3, positional_len):
            args.generate_trigger_entries = positional
    else:  # we have a trigger invocation from the server
        # p4d <= 13.1  does not support %command%
        if positional_len >= trigger_opt_base_count:
            args.parameters = positional
            args.trigger_type = positional[0]
            args.change = positional[1]
            args.user = positional[2]
            args.client = positional[3]
            args.serverport = positional[4]
            idx  = 5
            # the change-commit server contains the %oldchangelist% parameter
            if positional_len >= (idx + 1) and args.trigger_type == 'change-commit':
                args.oldchangelist = positional[idx]
                idx = idx + 1

            # Do we have 14.1 arguments?
            if (positional_len >= (idx + 1)  and
                    positional[idx] != '%command%' ):
                args.command = positional[idx]
                idx = idx + 1
            if (positional_len >= (idx + 1)  and
                    positional[idx] != '%args%' ):
                args.args = []
                while idx < positional_len:
                    args.args.append(positional[idx])
                    idx = idx + 1
        else:
            display_usage_and_exit(True, True)


    return args
# pylint: enable=R0912
GF_TRIGGER_DEBUG = 'GF_TRIGGER_DEBUG' in os.environ
def print_debug(msg):
    '''Conditionally print.'''
    if GF_TRIGGER_DEBUG:
        print("\n" + str(msg))


# pylint: disable=R0912
# Too many branches
# pylint: disable=R0915
def main():
    """Execute Git Fusion submit triggers."""
    args = parse_argv()
    global P4PORT
    exitcode = P4PASS
    missing_args = False
    if not args.optional_command:
    # we have been called as a p4d trigger
        if len(args.parameters) < 5:
            missing_args = True
        if len(args.parameters) >= 5:
            # Set P4PORT from %serverport% only if not set above to non-empty string
            # See P4PORT global override at top of this file
            if not P4PORT:
                P4PORT = args.serverport
            set_p4d_server_type()
            # pylint: disable=W0703
            #print("p4gf_submit_trigger Args:\n{0}\n".format(args))
            if args.trigger_type in TRIGGER_TYPES:
                if args.trigger_type == 'change-submit':
                    exitcode = change_submit_trigger(args.change, args.client)
                elif args.trigger_type == 'change-content':
                    exitcode = change_content_trigger(args.change, args.client
                                                    , args.command, args.args)
                elif args.trigger_type == 'change-commit':
                    # the change-commit trigger sets the oldchangelist - use it
                    if args.oldchangelist:
                        args.change = args.oldchangelist
                    exitcode = change_commit_trigger(args.change)
                elif args.trigger_type == 'change-failed':
                    exitcode = change_failed_trigger(args.change)
                elif args.trigger_type == 'change-commit-p4gf-config':
                    exitcode = change_commit_p4gf_config(args.change)
            else:
                print(_("Invalid trigger type: {0}").format(args.trigger_type))
                exitcode = P4FAIL
    else:
        # we have been called with optional args to perform a support task
        if args.set_version_counter:
            P4PORT = args.set_version_counter[0]
            set_version_counter()

        elif args.generate_trigger_entries:
            generate_trigger_entries(args.generate_trigger_entries)
        elif args.reset:
            P4PORT = args.reset[0]
            # Check if an optional user arg was passed and whether it is a super user
            user = get_user_from_args(args.reset, super_user_index=1)
            # Remove all the counters and reviews to reset
            reset_all(user)

        elif args.rebuild_all_gf_reviews:
            P4PORT = args.rebuild_all_gf_reviews[0]
            # Check if an optional user arg was passed and whether it is a super user
            user = get_user_from_args(args.rebuild_all_gf_reviews, super_user_index=1)
            exitcode = rebuild_all_gf_reviews(user=user)

    if missing_args:
        mini_usage(invalid=True)
        exitcode = P4FAIL

    sys.exit(exitcode)


if __name__ == "__main__":
    if sys.hexversion < 0x02060000 or \
            (sys.hexversion > 0x03000000  and sys.hexversion < 0x03020000):
        print(_("Python 2.6+ or Python 3.2+ is required"))
        sys.exit(P4FAIL)
    main()
