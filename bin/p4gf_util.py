#! /usr/bin/env python3.3
"""some utility functions"""

import argparse
import base64
from   collections import namedtuple, deque
import copy
import hashlib
import logging
import os
import pprint
import re
import shutil
import stat
import tempfile
import time
import traceback
from   uuid import uuid4
import zlib
from   subprocess import check_output, STDOUT

import pygit2

import P4

import p4gf_bootstrap  # pylint: disable=W0611
import p4gf_const
from   p4gf_ensure_dir import parent_dir, ensure_dir, ensure_parent_dir
from   p4gf_l10n       import _, NTR, mo_dir
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_path
import p4gf_proc
import p4gf_rc
import p4gf_version
import p4gf_view_dirs

# Import the 2.6 compatible pieces, which are shared with OVA scripts.
#
# pylint: disable=W0401
# Wildcard import p4gf_version_26
#
# pylint: disable=W0614
# Unused import %s from wildcard import
from p4gf_util_26 import *

LOG = logging.getLogger(__name__)
LOG_test_vars = logging.getLogger("p4gf_rc")
SHA1_REF_RE = re.compile(r'^([0-9a-f]{40})\s+(.*)$')
DIFF_RE = re.compile(r'^(\d+) (\w+) (\w+)\t(.+)$')


def create_arg_parser(desc, epilog=None, usage=None, help_custom=None):
    """Creates and returns an instance of ArgumentParser configured
    with the options common to all Git Fusion commands. The caller
    may further customize the parser prior to calling parse_args().

    Keyword arguments:
    desc -- the description of the command being invoked

    """
    class VersionAction(argparse.Action):
        """Custom argparse action to display version to stdout (instead
        of stderr, which seems to be the default in argparse)."""
        def __call__(self, parser, namespace, values, option_string=None):
            print(p4gf_version.as_string())
            exit(0)

    class HelpAction(argparse.Action):
        '''Dump help and exit.'''
        def __call__(self, parser, namespace, values, option_string=None):
            print(help_custom)
            exit(0)

    # argparse wraps the description and epilog text by default, but
    # could customize using formatter_class
    parser = argparse.ArgumentParser( description = desc
                                    , epilog      = epilog
                                    , usage       = usage
                                    , add_help    = not help_custom)
    parser.add_argument("-V", action=VersionAction, nargs=0,
                        help=_('displays version information and exits'))
    # We normally get -h and --help for free: prints programmatically
    # generated help then exit. Bypass and supply our own help dumper if
    # custom help provided.
    if help_custom:
        parser.add_argument('-h', '--help'
                          , nargs   = 0
                          , action  = HelpAction)

    return parser


def print_dictionary_list(dictlist):
    """Dump a dictlist of dictionaries, for debugging purposes"""
    c = 0
    for adict in dictlist:
        c += 1
        print("\n--%d--" % c)
        for key in adict.keys():
            print("%s: %s" % (key, adict[key]))

def service_user_exists(p4, user):
    """"Check for service user"""
    # Scanning for users when there are NO users? p4d returns ERROR "No such
    # user(s)." instead of an empty result. That's not an error to us, so
    # don't raise it.
    # need '-a' option to list service users
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run(['users', '-a', user])
        for user_ in r:
            if user_['User'] == user:
                return True
    return False

def _spec_exists_by_list_scan(p4, spec_info, spec_id):
    """"Table scan for an exact match of id"""
    # Scanning for users when there are NO users? p4d returns ERROR "No such
    # user(s)." instead of an empty result. That's not an error to us, so
    # don't raise it.
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run(spec_info['cmd_list'])
        for spec in r:
            if spec[spec_info['id_list']] == spec_id:
                return True
    return False


def _spec_exists_by_e(p4, spec_info, spec_id):
    """run 'p4 clients -e <name>' to test for existence."""
    r = p4.run(spec_info['cmd_list'], '-e', spec_id)
    for spec in r:
        if spec[spec_info['id_list']] == spec_id:
            return True
    return False


# pylint:disable=C0103
def _spec_exists_by_F(p4, spec_info, spec_id):
    """run 'p4 streams -F "Stream=<name>"' to test for existence."""
    id_key = spec_info['id_list']
    r = p4.run(spec_info['cmd_list'], '-F', "{}={}".format(id_key, spec_id))
    for spec in r:
        if spec[id_key] == spec_id:
            return True
    return False
# pylint:enable=C0103


# Instructions on how to operate on a specific spec type.
#
# How do we get a single user? A list of users?
# How do we determine whether a spec already exists?
#
# Fields:
#     cmd_one         p4 command to fetch a single spec: 'p4 client -o'
#                     (the '-o' is implied, not part of this value)
#     cmd_list        p4 command to fetch a list of specs: 'p4 clients'
#     id_one          dict key that holds the spec ID for results of cmd_one: 'Client'
#     id_list         dict key that holds the spec ID for results of cmd_list: 'client'
#     test_exists     function that tells whether a single specific spec
#                     already exists or not
SpecInfo = NTR({
    'client' : { 'cmd_one'     : 'client',
                 'cmd_list'    : 'clients',
                 'id_one'      : 'Client',
                 'id_list'     : 'client',
                 'test_exists' : _spec_exists_by_e },
    'depot'  : { 'cmd_one'     : 'depot',
                 'cmd_list'    : 'depots',
                 'id_one'      : 'Depot',
                 'id_list'     : 'name',
                 'test_exists' : _spec_exists_by_list_scan },
    'protect': { 'cmd_one'     : 'protect',
                 'cmd_list'    : None,
                 'id_one'      : None,
                 'id_list'     : None,
                 'test_exists' : None },
    'user'   : { 'cmd_one'     : 'user',
                 'cmd_list'    : ['users', '-a'],
                 'id_one'      : 'User',
                 'id_list'     : 'User',
                 'test_exists' : _spec_exists_by_list_scan },
    'group'  : { 'cmd_one'     : 'group',
                 'cmd_list'    : 'groups',
                 'id_one'      : 'Group',
                 'id_list'     : 'group',
                 'test_exists' : _spec_exists_by_list_scan },
    'stream' : { 'cmd_one'     : 'stream',
                 'cmd_list'    : 'streams',
                 'id_one'      : 'Stream',
                 'id_list'     : 'Stream',
                 'test_exists' : _spec_exists_by_F },
})


def spec_exists(p4, spec_type, spec_id):
    """Return True if the requested spec already exists, False if not.

    Raises KeyError if spec type not known to SpecInfo.
    """

    si = SpecInfo[spec_type]
    return si['test_exists'](p4, si, spec_id)


def _to_list(x):
    """Convert a set_spec() args value into something you can += to a list to
    produce a longer list of args.

    A list is fine, pass through unchanged.

    But a string must first be wrapped as a list, otherwise it gets decomposed
    into individual characters, and you really don't want "-f" to turn into
    ['-', 'f']. That totally does not work in 'p4 user -i - f'.

    No support for other types.
    """
    cases = { str : lambda t: [t],
              list: lambda t:  t  }
    return cases[type(x)](x)

# pylint:disable=R0913
# Too many arguments (7/5). Converting all these default
# options to a dict or struct is even worse.

def set_spec( p4
            , spec_type
            , spec_id=None
            , values=None
            , args=None
            , cached_vardict=None):
    """Create a new spec with the given ID and values.

    spec_id     : string name of fetch+set
    values : vardict of key/values to set
    args   : string or array of additional flags to pass for set.
             Intended for those rare cases when you need -f or -u.

    cached_vardict:
            A dict returned by a prior call to set_spec(). Saves us a call to
            '<spec> -o' to fetch the contents of the spec before modifying it.
            CHANGED IN PLACE. If you don't want the dict modified,
            pass us a copy.

    Returns the vardict used as input to <spec> -i

    Raises KeyError if spec_type not known to SpecInfo.
    """
    si = SpecInfo[spec_type]
    _args = ['-o']
    if spec_id:
        _args.append(spec_id)

    if cached_vardict:
        vardict = cached_vardict
    else:
        r = p4.run(si['cmd_one'], _args)
        vardict = first_dict(r)

    if values:
        for key in values:
            if values[key] is None:
                if key in vardict:
                    del vardict[key]
            else:
                vardict[key] = values[key]

    _args = ['-i']
    if args:
        _args += _to_list(args)
    p4.input = vardict
    try:
        p4.run(si['cmd_one'], _args)
        return vardict
    except:
        LOG.debug("failed cmd: set_spec {type} {id} {dict}"
              .format(type=spec_type, id=spec_id, dict=vardict))
        raise

# pylint:enable=R0913

def ensure_spec(p4, spec_type, spec_id, args=None, values=None):
    """Create spec if it does not already exist, NOP if already exist.

    Return True if created, False if already existed.

    You probably want to check values (see ensure_spec_values) if
    ensure_spec() returns False: the already-existing spec might
    contain values that you do not expect.
    """
    if not spec_exists(p4, spec_type, spec_id):
        LOG.debug("creating %s %s", spec_type, spec_id)
        set_spec(p4, spec_type, spec_id, args=args, values=values)
        return True
    else:
        LOG.debug("%s %s already exists", spec_type, spec_id)
        return False


def ensure_user_gf(p4):
    """Create user git-fusion-user it not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    return ensure_spec(p4, NTR('user'), spec_id=p4gf_const.P4GF_USER,
                       args='-f',
                       values={'FullName': NTR('Git Fusion')})


def ensure_user_reviews(p4):
    """Create user git-fusion-reviews it not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    user = gf_reviews_user_name()
    return ensure_spec(p4, NTR('user'), spec_id=user,
                       args='-f',
                       values={'FullName': _('Git Fusion Reviews'), 'Type':'service'})

def ensure_user_reviews_non_gf(p4):
    """Create user git-fusion-reviews--non-gf it not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    return ensure_spec(p4, NTR('user'), spec_id=p4gf_const.P4GF_REVIEWS__NON_GF,
                       args='-f',
                       values={'FullName': _('Git Fusion Reviews Non-GF'), 'Type':'service'})

def ensure_user_reviews_all_gf(p4):
    """Create user git-fusion-reviews--non-gf_union it not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    return ensure_spec(p4, NTR('user'), spec_id=p4gf_const.P4GF_REVIEWS__ALL_GF,
                       args='-f',
                       values={'FullName': _('Git Fusion Reviews Non-GF Union'), 'Type':'service'})


def ensure_depot_gf(p4):
    """Create depot P4GF_DEPOT if not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    return ensure_spec(p4, NTR('depot'), spec_id=p4gf_const.P4GF_DEPOT,
                       values={'Owner'      : p4gf_const.P4GF_USER,
                               'Description': _('Git Fusion data storage.'),
                               'Type'       : NTR('local'),
                               'Map'        : '{depot}/...'
                                              .format(depot=p4gf_const.P4GF_DEPOT)})


def ensure_spec_values(p4, spec_type, spec_id, values):
    """
    Spec exists but holds unwanted values? Replace those values.

    Does NOT create spec if missing. The idea here is to ensure VALUES,
    not complete spec. If you want to create an entire spec, you
    probably want to specify more values that aren't REQUIRED to match,
    such as Description.
    """
    spec = first_dict(p4.run(spec_type, '-o', spec_id))
    mismatches = {key:values[key] for key in values if spec.get(key) != values[key]}
    LOG.debug2("ensure_spec_values(): want={want} got={spec} mismatch={mismatch}"
               .format(spec=spec,
                       want=values,
                       mismatch=mismatches))

    if mismatches:
        set_spec(p4, spec_type, spec_id=spec_id, values=mismatches)
        LOG.debug("successfully updated %s %s", spec_type, spec_id)
    return mismatches

def is_legal_view_name(name):
    """
    Ensure that the view name contains only characters which are accepted by
    Perforce for client names. This means excluding the following character
    sequences: @ # * , / " %%x ...
    """
    # According to usage of 'p4 client' we get the following:
    # * Revision chars (@, #) are not allowed
    # * Wildcards (*, %%x, ...) are not allowed
    # * Commas (,) not allowed
    # * Slashes (/) ARE now allowed - supporting slashed git urls
    # * Double-quote (") => Wrong number of words for field 'Client'.
    # Additionally, it seems that just % causes problems on some systems,
    # with no explanation as to why, so for now, prohibit them as well.
    if re.search('[@#*,"]', name) or '%' in name or '...' in name:
        return False
    return True


def escape_path(path):
    '''
    Filesystem/Git-to-Perforce  @#%*

    Convert special characters fromthat Perforce prohibits from file paths
    to their %-escaped format that Perforce permits.
    '''
    return path.replace('%','%25').replace('#', '%23').replace('@', '%40').replace('*', '%2A')


def unescape_path(path):
    '''
    Perforce-to-filesystem/Git  @#%*

    Unescape special characters before sending to filesystem or Git.
    '''
    return path.replace('%23', '#').replace('%40', '@').replace('%2A', '*').replace('%25','%')


def argv_to_view_name(argv1):
    """Convert a string passed in from argv to a usable view name.

    Provides a central place where we can switch to unicode if we ever want
    to permit non-ASCII chars in view names.

    Also defends against bogus user input like shell injection attacks:
    "p4gf_init.py 'myview;rm -rf *'"

    Raises an exception if input is not a legal view name.
    """
    # To switch to unicode, do this:
    # argv1 = argv1.decode(sys.getfilesystemencoding())
    if not is_legal_view_name(argv1):
        raise RuntimeError(_("Gitr Fusion: Not a valid client name: '{view}'").format(view=argv1))
    return argv1


def to_path_rev(path, rev):
    '''
    Return file#rev.
    '''
    return '{}#{}'.format(path, rev)


def strip_rev(path_rev):
    '''
    Convert "file#rev" to file.
    '''
    r = path_rev.split('#')
    return r[0]


def to_path_rev_list(path_list, rev_list):
    '''
    Given a list of paths, and a corresponding list of file revisions, return
    a single list of path#rev.

    path_list : list of N paths. Could be depot, client, or local syntax
                (although local syntax with unescaped # signs will probably
                cause problems for downstream code that uses our result.)
    rev_list  : list of N revision numbers, one for each path in path_list.
    '''
    return ['{}#{}'.format(path, rev)
            for (path, rev) in zip(path_list, rev_list)]


def reset_git_enviro(p4=None):
    """Clear GIT_DIR and other GIT_xxx  environment variables,
    then chdir to GIT_WORK_TREE.

    This undoes any strangeness that might come in from T4 calling 'git
    --git-dir=xxx --work-tree=yyy' which might cause us to erroneously
    operate on the "client-side" git repo when invoked from T4.

    or from git-receive-pack chdir-ing into the .git dir.
    """
    git_env_key = [k for k in os.environ if k.startswith("GIT_")]
    for key in git_env_key:
        del os.environ[key]

    # Find our view name, use that to calculate and chdir into our GIT_WORK_TREE.
    rc_path = p4gf_path.cwd_to_rc_file()
    if rc_path:
        view_name = rc_path_to_view_name(rc_path)
        LOG.debug("reset_git_enviro rc_path_to_view_name({rc_path}) returned {view_name}"
                  .format(rc_path=rc_path, view_name=view_name))
        p4gf_dir    = rc_path_to_p4gf_dir(rc_path)
        if not p4gf_dir and p4:
            p4gf_dir = p4_to_p4gf_dir(p4)
        LOG.debug("reset_git_enviro rc_path_to_p4gf_dir({rc_path}) returned {p4gf_dir}"
                  .format(rc_path=rc_path, p4gf_dir=p4gf_dir))
        view_dirs  = p4gf_view_dirs.from_p4gf_dir(p4gf_dir, view_name)
        os.chdir(view_dirs.GIT_WORK_TREE)


def sha1_exists(sha1):
    '''
    Check if there's an object in the repo for the given sha1.
    '''
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
        return sha1 in repo
    except KeyError:
        return False
    except ValueError:
        return False


def git_rev_list_1(commit):
    """Return the sha1 of a single commit, usually specified by ref.

    Return None if no such commit.
    """
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
        obj = repo.revparse_single(commit)
        return obj.hex
    except KeyError:
        return None
    except ValueError:
        return None


def git_sha1_to_parents(child_sha1):
    '''
    Retrieve the list of all parents of a single commit.
    '''
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
        obj = repo.get(child_sha1)
        if obj.type == pygit2.GIT_OBJ_COMMIT:
            return [parent.hex for parent in obj.parents]
        return None
    except KeyError:
        return None
    except ValueError:
        return None


def sha1_for_branch(branch):
    """Return the sha1 of a Git branch reference.

    Return None if no such branch.
    """
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
        ref = repo.lookup_reference(fully_qualify(branch))
        return ref.hex
    except KeyError:
        return None
    except ValueError:
        return None


def git_empty():
    """Is our git repo completely empty, not a single commit?"""
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
        return len(repo.listall_references()) == 0
    except KeyError:
        return True
    except ValueError:
        return True


def fully_qualify(branch_ref_name):
    '''
    What we usually call 'master' is actually 'refs/heads/master'.

    Does not work for remote branches!
    It's stupdily expensive. I'm not digging through 'git remotes' to
    see if your partial name matches 'refs/{remote}/{partial-name} for
    each possible value of {remote}.
    '''
    if branch_ref_name.startswith('refs/'):
        return branch_ref_name
    return 'refs/heads/' + branch_ref_name


def git_ref_list_to_sha1(ref_list):
    '''
    Dereference multiple refs to their corresponding sha1 values.

    Output a dict of ref to sha1.
    '''
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
    except KeyError:
        return None
    except ValueError:
        return None
    result = {}
    for n in ref_list:
        try:
            ref = repo.lookup_reference(fully_qualify(n))
            result[n] = ref.hex
        except KeyError:
            result[n] = None
        except ValueError:
            result[n] = None
    return result


def git_checkout(sha1):
    """
    Switch to the given sha1.

    Returns True if the checkout was successful (exit status of 0),
    and False otherwise.
    """
    result = p4gf_proc.popen_no_throw(['git', 'checkout', '-f', sha1])
    return result['ec'] == 0


def checkout_detached_head():
    """
    Detach HEAD so that we have no current branch.  Now we can modify any
    branch without triggering 'can't ___ current branch' errors.
    """
    # no_throw because brand new repos have no commits at all, so
    # even HEAD~0 is an invalid reference.
    if not git_empty():
        p4gf_proc.popen_no_throw(['git', 'checkout', 'HEAD~0'])


class HeadRestorer:
    """An RAII class that restores the current working directory's HEAD and
    working tree to the sha1 it had when created.

    with p4gf_util.HeadRestorer() :
        ... your code that can raise exceptions...
    """

    def __init__(self):
        """
        Remember the current HEAD sha1.
        """
        self.__sha1__ = git_rev_list_1('HEAD')
        if not self.__sha1__:
            logging.getLogger("HeadRestorer").debug(
                "get_head_sha1() returned None, will not restore")

    def __enter__(self):
        """nop"""
        return None

    def __exit__(self, _exc_type, _exc_value, _traceback):
        """Restore then propagate"""
        ref = self.__sha1__
        if ref:
            p4gf_proc.popen(['git', 'reset', '--hard', ref])
            p4gf_proc.popen(['git', 'checkout', ref])
        return False  # False == do not squelch any current exception


def cwd_to_view_name():
    """Glean the view name from the current working directory's path:
    it's the 'foo' in 'foo/.git/'. If the Git Fusion RC file is
    present, then the view name will be read from the file.
    """
    config = p4gf_rc.read_config()
    view_name = p4gf_rc.get_view(config)
    if view_name is None:
        # Fall back to using directory name as view name.
        path = p4gf_path.cwd_to_dot_git()
        (path, _git) = os.path.split(path)
        (path, view_name) = os.path.split(path)
    return view_name


def dict_not(d, key_list):
    '''Return a dict that omits any values for keys in key_list.'''
    r = copy.copy(d)
    for key in key_list:
        if key in r:
            del r[key]
    return r


def quiet_none(x):
    '''Convert None to empty string for quieter debug dumps.'''
    if x == None:
        return ''
    return x


def test_vars():
    """Return a dict of test key/value pairs that the test script controls.

    Used to let test scripts control internal behavior, such as causing
    a loop to block until the test script has a chance to introduce a
    conflict at a known time.

    Eventually this needs to be read from env or a file or something
    that the test script controls.

    Return an empty dict if not testing (the usual case).
    """
    config = p4gf_rc.read_config()
    if not config:
        LOG_test_vars.debug("test_vars no config.")
        return {}
    if not config.has_section(p4gf_const.P4GF_TEST):
        LOG_test_vars.debug("test_vars config, no [test].")
        return {}
    d = {i[0]:i[1] for i in config.items(p4gf_const.P4GF_TEST)}
    LOG_test_vars.debug("test_vars returning {}".format(d))
    return d


def test_vars_apply():
    '''
    Read RC file for any test hooks and apply them now.
    '''
    tv = test_vars()
    if tv.get(p4gf_const.P4GF_TEST_UUID_SEQUENTIAL) or \
            (p4gf_const.P4GF_TEST_UUID_SEQUENTIAL in os.environ):
        global _uuid
        _uuid = uuid_sequential
        LOG.debug("UUID generator switched to sequential")


def rc_path_to_view_name(rc_path):
    """Read the rc file at rc_path and return the view name stored
    within the rc file."""
    config = p4gf_rc.read_config(rc_path=rc_path)
    return p4gf_rc.get_view(config)


def rc_path_to_p4gf_dir(rc_path):
    """Return the path to the outer ".git-fusion" container of all
    things Git Fusion.

    This is an alternative to p4_to_p4gf_dir() which avoids a trip to
    Perforce to read an object client root, and will probably become a
    source of bugs later if the admin changes the client root but does
    not move the .git-fusion dir.
    """
    return p4gf_path.find_ancestor(rc_path, p4gf_const.P4GF_DIR)


def p4_to_p4gf_dir(p4):
    """Return the local filesystem directory that serves as
    the root of all things Git Fusion.

    N.B. If you have a p4gf_context.Context, you can get the P4GF
    client root from ctx.gitrootdir and save the call to P4.

    This is the client root of the host-specific object client.

    This is also the direct ancestor of any .git-fusion.rc file,
    and an ancestor of views/<view>/... per-repo/view directories.

    By default this is set in p4gf_config as P4GF_HOME to $HOME/.git-fusion
    admin is free to
    change this later.
    """
    spec = p4.fetch_client(get_object_client_name())
    return spec['Root']


def view_to_client_name(repo_name):
    """construct client name using server id and repo name"""
    return p4gf_const.P4GF_REPO_CLIENT.format( server_id = get_server_id()
                                             , repo_name = repo_name )


def client_to_view_name(client_name):
    """parse repo name from client name"""
    prefix = view_to_client_name('')
    return client_name[len(prefix):]


def view_list(p4):
    '''
    Return a list of all known Git Fusion views.
    Reads them from 'p4 clients'.

    Omits the host-specific 'git-fusion--*' object clients.

    Return empty list if none found.
    '''
    prefix = view_to_client_name('')
    prefix_len = len(prefix)
    r = p4.run('clients', '-e', prefix + '*')
    return [spec['client'][prefix_len:] for spec in r]


def first_dict(result_list):
    '''
    Return the first dict result in a p4 result list.

    Skips over any message/text elements such as those inserted by p4broker.
    '''
    for e in result_list:
        if isinstance(e, dict):
            return e
    return None


def first_dict_with_key(result_list, key):
    '''
    Return the first dict result that sets the required key.
    '''
    for e in result_list:
        if isinstance(e, dict) and key in e:
            return e
    return None


def first_value_for_key(result_list, key):
    '''
    Return the first value for dict with key.
    '''
    for e in result_list:
        if isinstance(e, dict) and key in e:
            return e[key]
    return None


def read_bin_file(filename):
    '''
    Return the contents of bin/xxx.txt.

    Used to fetch help.txt and other such text templates.

    Returns False if not found.
    Return empty string if found and empty.
    '''
                        # Check for localized/translated version
                        # in bin/mo/xx_YY/LC_MESSAGES/
    _mo_dir = mo_dir()

    file_path = None
    if _mo_dir:
        file_path = os.path.join(_mo_dir, filename)

                        # Fall back to bin/ directory.
    if not file_path or not os.path.exists(file_path):
        file_path = os.path.join(os.path.dirname(__file__), filename)

    if not os.path.exists(file_path):
        return False

    with open(file_path, "r") as file:
        text = file.read()

    return text


def depot_to_local_path(depot_path, p4=None, client_spec=None):
    '''
    Where does this depot path land on the local filesystem?

    If we have a client spec, use its Root and View to calculate a depot
    path's location on the local filesystem. If we lack a client spec,
    but have a P4 connection, use that connection (and its implicit
    Perforce client spec) to calculate, using 'p4 where'.
    '''
    if client_spec:
        p4map = P4.Map(client_spec['View'])
        client_path = p4map.translate(depot_path)
        if not client_path:
            raise RuntimeError(_('Depot path {dp} not in client view client={cn}')
                  .format(dp=depot_path, cn=client_spec['Client']))
        client_name = client_spec['Client']
        client_root = client_spec['Root']
        rel_path = client_path.replace('//{}/'.format(client_name), '')
        rel_path_unesc = unescape_path(rel_path)
        local_path = os.path.join(client_root, rel_path_unesc)
        return local_path

    if p4:
        return first_dict(p4.run('where', depot_path))['path']

    raise RuntimeError(_('Bug: depot_to_local_path() called with neither a' \
                       ' client spec nor a p4 connection. depot_file={}')
                       .format(depot_path))


def make_writable(local_path):
    '''
    chmod existing file to user-writable.
    NOP if no such file or already writable.
    '''
    if not os.path.exists(local_path):
        return
    s = os.stat(local_path)
    if not s.st_mode & stat.S_IWUSR:
        sw = s.st_mode | stat.S_IWUSR
        LOG.debug('chmod {:o} {}'.format(sw, local_path))
        os.chmod(local_path, sw)


class NumberedChangelist:
    '''RAII class to create a numbered change, open files into it and submit it
    On exit, if change has not been successfully submited, all files are reverted
    and the change is deleted.
    '''
                        # pylint:disable=R0913
                        # Too many arguments (6/5)
    def __init__(self,
                 p4=None, ctx=None, gfctx=None
                 , description=_('Created by Git Fusion')

                        # If None, creates a new numbered pending changelist
                        # (what you usually want). Pass in a change_num of an
                        # existing numbered pending changelist if you want to
                        # use that instead.
                 , change_num = None
                 ):
        '''Call with exactly one of p4, ctx or gfctx set.
        In the case of ctx or gfctx, this numbered changelist will be attached
        to the context such that p4run() or p4gfrun() respectively will add
        the -c changelist option to applicable commands.
        '''
        assert bool(p4) ^ bool(ctx) ^ bool(gfctx)
        self.ctx = ctx
        self.gfctx = gfctx
        if ctx:
            self.p4 = ctx.p4
            assert not ctx.numbered_change
            ctx.numbered_change = self
        elif gfctx:
            self.p4 = gfctx.p4gf
            assert not gfctx.numbered_change_gf
            gfctx.numbered_change_gf = self
        else:
            self.p4 = p4

        self.change_num = 0
        self.submitted  = False
        self.shelved    = False

        if change_num:
            change = self.p4.fetch_change(change_num)
            self.change_num = change_num
            LOG.debug('reusing numbered change {} with description {}'
                      .format(self.change_num, change["Description"]))
            self.submitted  = False
            self.shelved    = True

                        # Reclaim ownership of this change so that we can use it.
            change['User'] = p4gf_const.P4GF_USER
            self.p4.input = change
            p4run_logged(self.p4, ['change', '-f', '-i'])

        else:
            self._open_new(description)
                        # pylint:enable=R0913

    def _open_new(self, description):
        '''
        Create a new numbered pending changelist.
        '''
        change = self.p4.fetch_change()
        change["Description"] = description
        self.p4.input = change
        result = p4run_logged(self.p4, ["change", "-i"])
        self.change_num = int(result[0].split()[1])
        LOG.debug('created numbered change {} with description {}'
                  .format(self.change_num, description))
        self.submitted = False
        self.shelved   = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, _traceback):
        # attempt to revert change if anything goes wrong

        # unhook from context first, so that we can use -c on revert
        # whether there's a context or not
        if self.ctx:
            self.ctx.numbered_change = None
        elif self.gfctx:
            self.gfctx.numbered_change_gf = None

        if not (self.submitted or self.shelved) and self.change_num:
            if exc_type:
                LOG.debug("numbered_change_exception: {}".format(exc_value))
                LOG.debug2("tb: {}".format(traceback.format_tb(_traceback)))
            LOG.debug('numbered change not submitted, reverting and deleting')
            with self.p4.at_exception_level(P4.P4.RAISE_ERROR):
                p4run_logged(self.p4, ["revert", "-c", self.change_num, "//..."])
            p4run_logged(self.p4, ["change", "-d", self.change_num])
        return False

    def p4run(self, cmd):
        '''add -c option to cmd and run it'''
        with self.p4.at_exception_level(P4.P4.RAISE_ALL):
            p4run_logged(self.p4, self.add_change_option(cmd))

    def submit(self):
        '''submit the numbered change and remember its new change number
        returns result of submit command'''
        with self.p4.at_exception_level(P4.P4.RAISE_ALL):
            r = p4run_logged(self.p4, self.add_change_option(["submit"]))
        self.change_num = self._changelist_from_submit_result(r)
        self.submitted = not self.change_num == None
        return r

    def shelve(self, replace = False):
        '''
        Shelve all pending file actions.

        Caller probably wants to revert all pending file actions after this,
        but that's not part of this function.
        '''
        if replace:
            cmd = [ NTR('shelve')
                  , '-r'    # replace-all shelved files
                            # with currently open files
                  , '-c', self.change_num
                  ]
        else:
            cmd = [ NTR('shelve')
                  , '-c', self.change_num
                  , '//...']
        p4run_logged(self.p4, cmd)
        self.shelved = True

    def add_change_option(self, cmd):
        '''add p4 option to operate on a numbered pending changelist
        cmd should not already contain a -c option
        '''
        if not self._cmd_needs_change(cmd[0]):
            return cmd
        if self.submitted:
            raise RuntimeError(_("Change already submitted"))
        assert not "-c" in cmd
        return cmd[:1] + ["-c", self.change_num] + cmd[1:]

    @staticmethod
    def _cmd_needs_change(cmd):
        '''check if a command needs the -c changelist option'''
        return cmd in NTR(['add', 'edit', 'delete', 'copy', 'integ',
                           'opened', 'revert', 'reopen', 'unlock',
                           'resolve', 'submit'])

    @staticmethod
    def _changelist_from_submit_result(r):
        """"Search for 'submittedChange'"""
        for d in r:
            if 'submittedChange' in d:
                return d['submittedChange']
        return None

    def second_open(self, description):
        '''
        Open a new numbered pending changelist to replace our current
        changelist that we just submitted.
        '''
        self._open_new(description)

def add_depot_file(p4, depot_path, file_content, client_spec=None):
    '''
    Create a new local file with file_content, add and submit to
    Perforce, then sync#0 it away from our local filesystem: don't leave
    the local file around as a side effect of adding.
    If added, return True.

    If already exists in Peforce, return False.

    If unable to add to Perforce (probably because already exists) raise
    Perforce exception why not.

    Uses and submits a numbered pending changelist.
    '''

    # Where does the file go?
    local_path = depot_to_local_path(depot_path, p4, client_spec)

    # File already exists in perforce and not deleted at head revision?
    with p4.at_exception_level(p4.RAISE_NONE):
        stat_ = p4.run('fstat', '-T', 'headAction', depot_path)
        if stat_ and 'headAction' in stat_[0]:
            action = stat_[0]['headAction']
            if action != 'delete' and action != 'move/delete':
                return False

    LOG.debug("add_depot_file() writing to {}".format(local_path))
    ensure_dir(parent_dir(local_path))
    with open(local_path, 'w') as f:
        f.write(file_content)

    filename = depot_path.split('/')[-1]
    desc = _("Creating initial '{filename}' file.").format(filename=filename)

    with NumberedChangelist(p4=p4, description=desc) as nc:
        nc.p4run(["add", depot_path])
        nc.submit()

    p4.run('sync', '-q', depot_path + "#0")
    return True


def edit_depot_file(p4, depot_path, file_content, client_spec=None):
    '''
    p4 sync + edit + submit a single file in Perforce.
    Removes file from workspace when done: sync#0

    File must already exist in Perforce.

    Uses and submits a numbered pending changelist.
    '''

    p4.run('sync', '-q', depot_path)

    filename = depot_path.split('/')[-1]
    desc = _("Update '{filename}'.").format(filename=filename)

    with NumberedChangelist(p4=p4, description=desc) as nc:
        nc.p4run(["edit", depot_path])
        # Where does the file go?
        local_path = depot_to_local_path(depot_path, p4, client_spec)
        with open(local_path, 'w') as f:
            f.write(file_content)
        nc.submit()

    p4.run('sync', '-q', depot_path + "#0")


def print_depot_path_raw(p4, depot_path):
    '''
    p4 print a file, using raw encoding, and return the raw bytes of that file.
    '''
    tempdir = tempfile.TemporaryDirectory(prefix=p4gf_const.P4GF_TEMP_DIR_PREFIX)
    with tempdir:
        tf = tempfile.NamedTemporaryFile( dir=tempdir.name
                                        , prefix='print-'
                                        , delete=False )
        tf.close()                      # Let p4, not Python, write to this file.
        with RawEncoding(p4):
            p4.run('print', '-o', tf.name, depot_path)
        with open(tf.name, mode='rb') as f2:
            b = f2.read()
        return b


def depot_file_exists(p4, depot_path):
    '''
    Does this file exist in the depot, and is its head revision not deleted?
    '''
    with p4.at_exception_level(p4.RAISE_NONE):
        head_action = first_value_for_key(p4.run( 'fstat'
                                                , '-TheadAction'
                                                , depot_path)
                                         , 'headAction')
    if head_action and 'delete' not in head_action:
        return True
    else:
        return False


_UUID_SEQUENTIAL_PREV_VALUE = 0
_UUID_SEQUENTIAL_COUNTER = "uuid-sequential"

def uuid_sequential(p4, namespace=None):
    '''
    Replacement for uuid() that returns a deterministic sequence of
    values so that test scripts can more easily test for exepected results.

    Keep the differentiating part of the UUID in the first 7 chars, since
    many debug log statements only print that much of the UUID.
    '''
    if p4 and namespace:
        r = p4.run('counter', '-u', '-i',
                   '{}-{}'.format(_UUID_SEQUENTIAL_COUNTER, namespace))
        value =  int(r[0]['value'])
    elif p4:
        r = p4.run('counter', '-u', '-i', _UUID_SEQUENTIAL_COUNTER)
        value =  int(r[0]['value'])
    else:
        global _UUID_SEQUENTIAL_PREV_VALUE
        _UUID_SEQUENTIAL_PREV_VALUE += 1
        value = _UUID_SEQUENTIAL_PREV_VALUE
    return NTR('{:05}-uuid').format(value)


def uuid_real(_p4_unused, _namespace_unused):
    '''
    Return a globally unique identifier.

    Returns a 128-bit GUID, encoded as a 24-character base64 string.
    '''
    # p4 argument ignored - used only by uuid_sequential
    u       = uuid4()   # is uuid.uuid4(), not sure how to invoke that
    byteses = u.bytes
                        # urlsafe_b64encode() vs.  b64encode():
                        # b64encode() uses /, which can cause problems if we
                        # insert our own / dividers next to one of these b64
                        # / chars. Perforce does not permit "//" anywhere in a
                        # depot path except at the front. urlsafe_b64encode()
                        # uses _ instead of /.
    b64     = base64.urlsafe_b64encode(byteses)
    return b64.decode()


_uuid = uuid_real    # How to generate UUIDs. See test_vars_apply()
def uuid(p4, namespace=None):
    '''
    Call our UUID generator, usually real UUIDs, sometimes
    sequential if running under a test that needs deterministic results.
    '''
    return _uuid(p4, namespace)


def log_collection(log, coll):
    '''
    If DEBUG3, return entire collection (ouch).
    If just DEBUG, return just collection length.
    If not DEBUG at all, return None.
    '''
    if log.isEnabledFor(logging.DEBUG):
        if log.isEnabledFor(logging.DEBUG3):
            return coll
        else:
            return len(coll)
    else:
        return None


def is_bare_git_repo():
    '''
    Is the Git repo already loaded for --bare?
    '''
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
        return repo.is_bare
    except KeyError:
        return False
    except ValueError:
        return False


def first_of(c):
    '''
    Return first non-None element of c.
    '''
    for x in c:
        if x:
            return x
    return None


def gf_reviews_user_name():
    '''
    Return a service user name for a per GF instance service user that we use to
    record client views as Reviews.
    '''
    return p4gf_const.P4GF_REVIEWS_SERVICEUSER.format(get_server_id())


class Handler:
    '''
    RAII class to set p4.handler on entry, restore on exit.
    '''
    def __init__(self, p4, handler):
        self.p4      = p4
        self.handler = handler

    def __enter__(self):
        self.p4.handler = self.handler

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.p4.handler = None


class RawEncoding:
    '''
    RAII class to set p4.encoding to "raw" on entry, restor on exit.
    '''
    def __init__(self, p4):
        self.p4            = p4
        self.save_encoding = p4.encoding

    def __enter__(self):
        self.p4.encoding = NTR('raw')

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.p4.encoding = self.save_encoding


class UserClientHost:
    '''
    RAII class to set p4.user, p4.client, and p4.host on entry, restore on exit.
    '''
    def __init__(self, p4, user, client, host):
        self.p4 = p4
        self.new_user = user
        self.new_client = client
        self.new_host = host
        self.save_user = p4.user
        self.save_client = p4.client
        self.save_host = p4.host

    def __enter__(self):
        self.p4.user = self.new_user
        self.p4.client = self.new_client
        self.p4.host = self.new_host

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.p4.user = self.save_user
        self.p4.client = self.save_client
        self.p4.host = self.save_host


class ClientUnlocker:
    '''
    RAII class to unlock a client, restore original setting on exit.
    '''
    def __init__(self, p4, client):
        '''
        Initialize ClientUnlocker with the given P4API and client map.
        '''
        self.p4 = p4
        self.client = client
        self.options = client['Options'].split()
        self.was_locked = 'locked' in self.options

    def __enter__(self):
        if self.was_locked:
            self.options[self.options.index(NTR('locked'))] = NTR('unlocked')
            self.client['Options'] = " ".join(self.options)
            self.p4.save_client(self.client, '-f')

    def __exit__(self, _exc_type, _exc_value, _traceback):
        if self.was_locked:
            self.options[self.options.index(NTR('unlocked'))] = NTR('locked')
            self.client['Options'] = " ".join(self.options)
            self.p4.save_client(self.client, '-f')


class FileDeleter:
    '''
    RAII object to delete all registered files.
    '''
    def __init__(self):
        '''
        Initialize ClientUnlocker with the given P4API and client map.
        '''
        self.file_list = []

    def __enter__(self):
        pass

    def __exit__(self, _exc_type, _exc_value, _traceback):
        LOG.debug('FileDeleter deleting'.format(self.file_list))
        for f in self.file_list:
            os.unlink(f)


def enslash(path_element):
    '''
    Break a single path element into multiple nested parts. This restricts the
    number of files in any single depot or filesystem directory to something
    that most GUIs can handle without bogging down.

    Input string must not contain any slashes.
    Input string must be at least 5 characters long.
    '''
    return (       path_element[0:2]
           + '/' + path_element[2:4]
           + '/' + path_element[4: ] )


def octal(x):
    '''
    Convert a string such as a git file mode "120000" to an integer 0o120000.

    Integers pass unchanged.
    None converted to 0.

    See mode_str() for counterpart.
    '''
    if not x:
        return 0
    if isinstance(x, int):
        return x
    return int(x, 8)


def mode_str(x):
    '''
    Convert octal integer to string, return all others unchanged.

    See octal() for counterpart.
    '''
    if isinstance(x, int):
        return NTR('{:06o}').format(x)
    return x


def chmod_644_minimum(local_path):
    '''
    Grant read access to all unix accounts.
    '''
    old_mode = os.stat(local_path).st_mode
    new_mode = old_mode | 0o000644
    os.chmod(local_path, new_mode)


def _force_clear(dest_local):
    '''
    Clear the way before a copy/link_file_forced().

    rm -rf is very dangerous. You would be wise to pass
    absolute paths for dest_local.
    '''
    if os.path.exists(dest_local):
        if os.path.isdir(dest_local):
            shutil.rmtree(dest_local)
        else:
            os.unlink(dest_local)


def write_server_id_to_file(server_id):
    '''
    Write server_id to P4GF_HOME/server-id.

    NOP if that file already holds server_id.
    '''
    # NOP if already set to server_id.
    if server_id == read_server_id_from_file():
        return

    path = server_id_file_path()
    ensure_parent_dir(path)
    with open(path, 'w') as f:
        f.write('{}\n'.format(server_id))


def p4map_lhs_canonical(lhs):
    '''
    Strip quotes and leading +/-.

    Return 2-tuple of (leading +/-, line).
    '''
    mod = ''
    if not lhs:
        return (mod, lhs)
    r   = lhs
    dequoted = False          # You only get one level of dequote.
    if r[0] == r[-1] == '"':
        r = r[1:-1]
        if not r:
            return (mod, r)
    if r[0] in ['-', '+']:
        mod = r[0]
        r   = r[1:]
        if not r:
            return (mod, r)
    if not dequoted and r[0] == r[-1] == '"':
        r = r[1:-1]
    return (mod, r)


def p4map_lhs_line_replace_root(lhs, old_root, new_root):
    '''
    Convert a branch view mapping line's lhs from one depot root to another.

    root strings assumed to end in slash. Not doing that here because
    it's usually more efficient to do that outside of this function
    which is usually called in a loop.

    Strips any double-quotes from lhs. Those shouldn't have survived
    config-to-view anyway.
    '''
    (mod, orig_lhs_path) = p4map_lhs_canonical(lhs)
    if orig_lhs_path.startswith(old_root):
        new_lhs_path = new_root + orig_lhs_path[len(old_root):]
    else:
        new_lhs_path = orig_lhs_path
    return mod + new_lhs_path


def create_p4map_replace_lhs_root(orig_p4map, old_root, new_root):
    '''
    Create a new P4.Map instance, similar to orig_p4map but with the
    lhs of each set to a new depot root.

    old_root and new_root should both end in / delimiter.
    '''
    result = P4.Map()
    for (orig_lhs, rhs) in zip(orig_p4map.lhs(), orig_p4map.rhs()):
        new_lhs = p4map_lhs_line_replace_root(orig_lhs, old_root, new_root)
        result.insert(new_lhs, rhs)
    return result


def gmtime_str_iso_8601(seconds_since_epoch=None):
    '''
    Return an ISO 8601-formatted timestamp in UTC time zone (gmtime)
        YYYY-MM-DDThh:mm:ssZ

    seconds_since_epoch is usually something from time.gmtime() or a P4 date
    number. If omitted, current time is used. If supplied, caller is responsible
    for converting from time zone to UTC before passing to us.
    '''
    t = seconds_since_epoch if seconds_since_epoch else time.gmtime()
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)


KTEXT_KEYWORD_PATTERN = re.compile(r'\$(?P<keyword>Author|Change|Date|DateTime'
                             + r'|File|Header|Id|Revision):[^$\n]*\$')


def unexpand_ktext_line(line):
    """unexpand a line from keyword expanded file
    line is a string in latin-1 encoding.
    """
    return KTEXT_KEYWORD_PATTERN.sub(NTR(r'$\g<keyword>$'), line)


def unexpand_ktext_list(local_path_list, tempdir_path):
    '''
    Replace expanded ktext RCS keywords with their unexpanded value.

    Optimized to reuse a single tempfile location over and over. Pass a
    single-element list if you don't need that optimization.
    '''
    # Use the same temp file as to read each ktext file
    tmpfile = tempfile.NamedTemporaryFile(delete=False, dir=tempdir_path, prefix='unexpand-')
    tmpfile.close()
    for _file in local_path_list:
        shutil.move(_file, tmpfile.name)   # mv the original to the temp
        with open(tmpfile.name, encoding='latin-1', mode='r') as _readf:
            with open(_file, mode='wb') as _writef:   # write the original - unexpanded
                for line in _readf:
                    _writef.write(unexpand_ktext_line(line).encode('latin-1'))
    os.unlink(tmpfile.name)


def p4run_logged( p4, cmd
                , log_warnings = logging.WARNING
                , log_errors   = logging.ERROR
                , numbered_change = None):
    '''
    Log a command and its response.

    log_warnings and log_errors specify what logging level to use when recording
    Perforce warnings and errors. We run some Perforce commands where we expect
    warnings (especially "no such file(s)" and we really don't need to pollute
    the log with those expected warnings.)
    '''
    assert isinstance(cmd, list)
    if numbered_change:
        cmd = numbered_change.add_change_option(cmd)
    log_p4_request(cmd)
    results = p4.run(cmd)
    log_p4_results( p4, results
                  , log_warnings = log_warnings
                  , log_errors   = log_errors)
    return results


def bytes_to_git_object(byte_array):
    '''
    Decompress a Git object's content, strip header, return content.
    '''
    data = zlib.decompress(byte_array)
    # skip over the type and length
    return data[data.index(b'\x00') + 1:]


def local_path_to_git_object(local_path):
    '''
    Retrieve a decompressed Git object from the Git Fusion object cache.
    The object header is not included in the result.
    '''
    with open(local_path, "rb") as f:
        blob = f.read()
    return bytes_to_git_object(blob)


def depot_path_to_git_object(p4, depot_path):
    '''
    Fetch a Git object from its location in Perforce.
    '''
    byte_array = print_depot_path_raw(p4, depot_path)
    return bytes_to_git_object(byte_array)


def log_p4_request(cmd):
    '''
    Write p4 cmd request to log, depending on log level.
    '''
    assert isinstance(cmd, list)
    logging.getLogger('p4.cmd').debug(' '.join([str(c) for c in cmd]))


def remove_duplicates(collection):
    '''There's probably a library function for this somewhere.'''
    s = set(collection)
    if len(s) == len(collection):
        return collection
    r = []
    for c in collection:
        if c in s:
            r.append(c)
            s.remove(c)
    return r


def pairwise(iterable):
    '''
    Convert a single-element list into a 2-tuple list.

    If odd number of elements, omits final odd element.

    Mostly used to convert lists to dicts:

    l = ['a', 1, 'b', 2]
    d = dict(pairwise(l))
    print(d)  # ==> {'b': 2, 'a': 1}

    Adapted from http://code.activestate.com/recipes/252176-dicts-from-lists/
    '''
    i = iter(iterable)
    while 1:
        yield next(i), next(i)

# pylint:disable=C0301
# line too long? Too bad. Keep tabular code tabular.

# Common Perforce warnings that occur normally as part of how we interact
# with Perforce. Don't clutter the log with WARNING entries unworthy of
# human attention.
_SQUELCHED_P4_RESULTS = [
  p4gf_p4msgid.MsgDm_ExHAVE         # "[%argc% - file(s)|File(s)] not on client." } ;
, p4gf_p4msgid.MsgDm_ExFILE         # "[%argc% - no|No] such file(s)." } ;
, p4gf_p4msgid.MsgDm_ExINTEGPERM    # "[%argc% - all|All] revision(s) already integrated." } ;

]
# pylint:enable=C0301

def log_p4_results( p4, results
                  , log_warnings = logging.WARNING
                  , log_errors   = logging.ERROR ):
    '''
    Write p4 results to log, depending on log level.
    '''
    log_out = logging.getLogger('p4.out')
    log_out.debug('result ct={}'.format(len(results)))
    if log_out.isEnabledFor(logging.DEBUG3):
        pp = pprint.PrettyPrinter()
        log_out.debug3(pp.pformat(results))

    log_err   = logging.getLogger('p4.err')
    log_warn  = logging.getLogger('p4.warn')
    log_msgid = logging.getLogger('p4.msgid')

    # If we're inside a "with p4.at_exception_level(p4.RAISE_NONE), then
    # don't pollute the log with expected errors. Log expected errors
    # at level "debug".
    _log_errors   = log_errors
    _log_warnings = log_warnings
    if p4.exception_level == p4.RAISE_NONE:
        _log_errors   = logging.DEBUG
        _log_warnings = logging.DEBUG

    # Dump in two groups: first the textual stuff, then all the numeric message
    # ID stuff. Too hard to read text + numeric when commingled.

    if (       (p4.errors   and log_err  .isEnabledFor(_log_errors  ))
        or not (p4.warnings and log_warn .isEnabledFor(_log_warnings))):

        for m in p4.messages:
            if m.msgid in _SQUELCHED_P4_RESULTS:
                continue
            if p4gf_p4msgid.E_FAILED <= m.severity:
                log_err.log(_log_errors, str(m))
            elif p4gf_p4msgid.E_WARN == m.severity:
                log_warn.log(_log_warnings, str(m))

    if log_msgid.isEnabledFor(logging.DEBUG2):
        for m in p4.messages:
            log_msgid.debug2(p4gf_p4msg.msg_repr(m))


def git_hash_object(local_path):
    '''
    Return the sha1 that 'git-hash-obect <local_path>' would return,
    but without the process overhead of launching git-hash-object.

    Does not write object to .git store.

    Unlike Git's own git-hash-object, git_hash_object() is smart enough
    to not dereference a symlink file. Returns hash of symlink itself.
    '''
    if os.path.islink(local_path):
        return git_hash_object_symlink(local_path)
    else:
        return git_hash_object_not_symlink(local_path)

                                        # pylint:disable=W1401
                                        # Anomalous backslash in string: '\0'
                                        # Known bug in pylint 0.26.0
                                        #            fixed in 0.27.0
# Header that goes at the top of each blob.
# {} is uncompressed byte count of content.
# Header, uncompressed, along with uncompressed content, is all sha1ed.
# Compress header+content together and store as object under that sha1.
_BLOB_HEADER  = NTR('blob {}\0')
                                        # pylint:enable=W1401

def git_hash_object_not_symlink(local_path):
    '''
    Return the sha1 that 'git-hash-obect <local_path>' would return,
    but without the process overhead of launching git-hash-object.

    Does not write object to .git store.
    '''
    # Push the the "blobNNN\0" header, uncompressed,
    # through the sha1 calculator.
                                        # pylint: disable=E1101
                                        # Module 'hashlib' has no 'sha1' member
                                        # pylint doesn't understand dynamic
                                        # definition of sha1 in hashlib
    sha1                    = hashlib.sha1()
                                        # pylint: enable=E1101
    uncompressed_byte_count = os.lstat(local_path).st_size
    header                  = _BLOB_HEADER.format(uncompressed_byte_count)
    sha1.update(header.encode())

    # Pump file content, uncompressed, through the sha1 calculator.
    with open(local_path, 'rb') as f:
        chunksize = 4096
        while True:
            chunk = f.read(chunksize)
            if chunk:
                sha1.update(chunk)
            else:
                break

    return sha1.hexdigest()


def git_hash_object_symlink(local_path):
    '''
    Return sha1 of a symlink's internal data (the path stored in the link file).

    !!! CAN RETURN INCORRECT RESULTS !!!

    Python's os.readlink() returns a unicode string, not the raw bytes from the
    symlink file. We convert back to bytes using default encoding, which may or
    may not round-trip to the original bytes.

    Use only where incorrect results are annoying but not disastrous.
    '''
    assert os.path.islink(local_path)

    # os.readlink() converts the symlink file's content to a Unicode string,
    # _possibly_ using the special roundtrippable os.fsdecode()/os.fsencode()
    # encode. Use os.fsencode() and hope that restores the string back to the
    # symlink's original bytes.
    symlink_content         = os.readlink(local_path)
    data                    = os.fsencode(symlink_content)

    uncompressed_byte_count = len(data)
    header                  = _BLOB_HEADER.format(uncompressed_byte_count)

                                        # pylint: disable=E1101
                                        # Module 'hashlib' has no 'sha1' member
                                        # pylint doesn't understand dynamic
                                        # definition of sha1 in hashlib
    sha1                    = hashlib.sha1()
                                        # pylint: enable=E1101
    sha1.update(header.encode())
    sha1.update(data)
    return sha1.hexdigest()


def sha1_to_git_objects_path(sha1):
    '''
    Return a path to a loose object: "objects/??/?{38}""

    This is the path that git-hash-objects and other Git tools create when
    writing an object to Git's own object store.

    See git/sha1_file.c/sha1_file_name().

    WARNING: Nathan has seen Git nest objects more deeply after unpacking
             into loose objects. This code knows nothing of that.
    '''
    return 'objects/' + sha1[0:2] + '/' + sha1[2:]


def abbrev(x):
    '''
    To 7 chars.

    Accepts str.
    Accepts None.
    Accepts list.
    '''
    if None == x:
        return 'None'
    elif isinstance(x, list):
        return [abbrev(e) for e in x]
    elif isinstance(x, set):
        return {abbrev(e) for e in x}
    elif isinstance(x, str):
        return x[:7]
    return x


def debug_list(log, lizt, details_at_level=logging.DEBUG2):
    '''
    Conditionally summarize a list for log.

    If debug level at or finer than details_at_level, return list.
    If not, return "ct=N"
    '''
    if log.isEnabledFor(details_at_level):
        return lizt
    return NTR('ct={}').format(len(lizt))


def partition(pred, iterable):
    '''
    Use a predicate to partition true entries before false entries.
    '''
    false = []
    for x in iterable:
        if pred(x):
            yield x
        else:
            false.append(x)
    for f in false:
        yield f


def alpha_numeric_sort(list_):
    """ Sort the given iterable in the way that humans expect."""
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ]
    return sorted(list_, key = alphanum_key)


#pylint: disable=W0612
def dirtree_has_no_files(dirpath):
    """ Test directory tree has no files"""
    for root, dirs, files in os.walk(dirpath, followlinks=True):
        if len(files):
            return False
    return True
#pylint: enable=W0612


GitLsTreeResult = namedtuple( 'GitLsTreeResult'
                            , ['mode', 'type', 'sha1', 'gwt_path'])

def _tuple_from_tree_entry(entry, path=None):
    '''make a tuple from a TreeEntry and path'''
    if not path:
        path = entry.name
    if entry.filemode ==  0o040000:
        otype = 'tree'
    elif entry.filemode == 0o160000:
        otype = 'gitlink'
    else:
        otype = 'blob'

    ### Zig would like to leave mode as int rather than convert to and from str.
    ### Who still relies on it being a str?
    return GitLsTreeResult( mode     = mode_str(entry.filemode)
                          , type     = otype
                          , sha1     = entry.hex
                          , gwt_path = path
                          )


def git_ls_tree_one(repo, commit_sha1, gwt_path):
    '''
    Return a single file's ls-tree 4-tuple:
    ('100644', 'blob', '47cb616851147876ee381cd8aad569530fe25a91', 'a/a3')
    '''
    try:
        tree_entry = repo.get(commit_sha1).tree[gwt_path]
    except: # pylint: disable=W0702
        return None

    return _tuple_from_tree_entry(tree_entry, gwt_path)


def git_ls_tree(repo, treeish_sha1):
    '''
    Return a list of ls-tree 4-tuples direct children of a treeish:
    ('100644', 'blob', '47cb616851147876ee381cd8aad569530fe25a91', 'a/a3')

    DOES NOT RECURSE. See git_ls_tree_r() for that.

    Squelches errors, because this is frequently used to query for existence
    and not-exists is a valid result, not an exception to raise.
    '''
    try:
        obj = repo.get(treeish_sha1)
        if obj.type == pygit2.GIT_OBJ_TREE:
            tree = obj
        elif obj.type == pygit2.GIT_OBJ_COMMIT:
            tree = obj.tree
        else:
            #BLOB or TAG
            raise RuntimeError(_('object is not a commit or tree'))
        return [_tuple_from_tree_entry(entry) for entry in tree]
    except: # pylint: disable=W0702
        return []


def treeish_to_tree(repo, treeish_sha1):
    '''
    Convert a commit or tree sha1 to its pygit2.Tree object.
    '''
    obj = repo.get(treeish_sha1)
    if obj.type == pygit2.GIT_OBJ_TREE:
        return obj
    elif obj.type == pygit2.GIT_OBJ_COMMIT:
        return obj.tree
    else:
        return None

TreeWalk = namedtuple('TreeWalk', ['gwt_path' , 'tree'])

def git_iter_tree(repo, treeish_sha1):
    '''
    Iterate through a tree, yielding
        parent directory path
        child name (single name not full path)
        child mode (integer)
        child sha1

    +++ Do not construct and return GitLsTreeResult instances.
        That increases the cost of a tree walk, and many walks (such as 'find
        all symlinks') discard most nodes. Don't pay construction costs for
        objects you don't need.
    '''

    # Initialize the walk with top-level TreeEntry.
    start_tree = treeish_to_tree(repo, treeish_sha1)
    if not start_tree:
        return
    work_queue = deque([TreeWalk (gwt_path='', tree=start_tree)])

    log = LOG.getChild('ls_tree')
    log.debug2('git_iter_tree / {}'.format(abbrev(treeish_sha1)))
    is_debug3 = log.isEnabledFor(logging.DEBUG3)

    # Walk the tree, yielding rows as we encounter them.
    # Yield directories when encountered, no different than blobs,
    # but also queue them up for later "recursion".

    while work_queue:
        curr_tree_walk = work_queue.pop()

        parent_gwt_path = curr_tree_walk.gwt_path

        for child_te in curr_tree_walk.tree:
            if is_debug3:
                log.debug3('git_iter_tree Y {:06o} {:<40} {:<20} {}'
                           .format( child_te.filemode
                                  , child_te.hex
                                  , child_te.name
                                  , parent_gwt_path))

            yield parent_gwt_path, child_te.name, child_te.filemode, child_te.hex

            # "Recurse" into subdirectory TreeEntry later.
            if child_te.filemode == 0o040000: # dir
                child_gwt_path = p4gf_path.join(parent_gwt_path, child_te.name)
                child_tree     = repo[child_te.oid]
                work_queue.append(TreeWalk( gwt_path = child_gwt_path
                                          , tree     = child_tree     ))
                if is_debug3:
                    log.debug3('git_iter_tree Q {}'.format( child_gwt_path ))


def _filemode_to_type(filemode):
    '''
    It is faster to infer blob/tree type from an integer filemode
    than to instantiate a pygit2 object just to ask it this question.
    '''
    if filemode ==  0o040000:
        return 'tree'
    elif filemode == 0o160000:
        return 'gitlink'
    else:
        return 'blob'


def git_ls_tree_r(repo, treeish_sha1):
    '''
    Return a generator that produces a list of ls-tree 4-tuples, one for
    each directory or blob in the entire tree.

    ('100644', 'blob', '47cb616851147876ee381cd8aad569530fe25a91', 'a/a3')

    Walks entire tree.
    '''
    return ( GitLsTreeResult(
                  mode     = mode_str(mode)  ### Zig dislikes str not int here.
                , type     = _filemode_to_type(mode)
                , sha1     = sha1
                , gwt_path = p4gf_path.join(parent_gwt_path, name) )
            for parent_gwt_path
              , name
              , mode
              , sha1 in git_iter_tree(repo, treeish_sha1) )

def unlink_file_or_dir(path, delete_non_empty=False):
    """handle links, files, and empty/full directory hierarchies"""
    if os.path.lexists(path):
        if os.path.isdir(path) and not os.path.islink(path):
            if delete_non_empty or dirtree_has_no_files(path):
                shutil.rmtree(path)
            else:
                LOG.debug("Cannot remove dir {0} - not empty".format(path))
        else:
            os.unlink(path)  # would fail on real dir


def rm_dir_contents(dir_path):
    """Remove the contents of a dir path"""
    if os.path.isdir(dir_path):
        for file_object in os.listdir(dir_path):
            file_object_path = os.path.join(dir_path, file_object)
            if os.path.isdir(file_object_path) and not os.path.islink(file_object_path):
                shutil.rmtree(file_object_path)
            else:
                os.unlink(file_object_path)  # would fail on real dir


def dequote(path):
    """Strip leading and trailing double-quotes if both present, NOP if not."""
    if (2 <= len(path)) and path.startswith('"') and path.endswith('"'):
        return path[1:-1]
    return path


def enquote(path):
    """Paths with space char require double-quotes, all others pass
    through unchanged.
    """
    if ' ' in path:
        return '"' + path + '"'
    return path


def join_non_empty(grout, a, b):
    '''
    Return a + grout + b, using grout only if both a and b.
    '''
    if a and b:
        return a + grout + b
    if a:
        return a
    return b


def force_case_sensitive_p4api(p4):
    '''
    Attempt to turn on case-sensitivity for all P4Python string operations.

    The P4 C API (and P4Python which wraps it) is already case-sensitive by
    default for linux. But the P4 C API is case-insensitive by default on Mac OS
    X, and Git Fusion developers run a lot of tests on their Macs, including
    some case-sensitive ones. So this is here just to help Zig test.

    This setting is GLOBAL to all P4 C API operations, not just this one p4
    instance.

    This attribute is undocumented, unsupported, and not present in all P4Python
    builds. Okay if absent.
    '''
    try:
        p4.case_folding = 0
    except AttributeError:
        pass


def log_cmd_output(cmd, msg=None):
    """Execute system command and Log command output
    Intended only for temporary logging during development
    """
    if LOG.isEnabledFor(logging.DEBUG):
        out = check_output(cmd, stderr=STDOUT, shell=True)
        if not msg:
            msg = ""
        LOG.debug("log_cmd:{0}: {1}:\n {2}".format(msg, cmd, str(out).replace('\\n',"\n")))
