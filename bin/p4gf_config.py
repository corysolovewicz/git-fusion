#! /usr/bin/env python3.3
'''
Git Fusion configuration files.

A global configuration file is stored in Perforce:
    //P4GF_DEPOT/p4gf_config

Each individual Git Fusion repo has its own config:
    //P4GF_DEPOT/repos/{repo}/p4gf_config

Files are simple INI format.
'''
import configparser
import io
import re
import sys

import P4

import p4gf_env_config    # pylint: disable=W0611
import p4gf_const
import p4gf_create_p4
from   p4gf_l10n import _, NTR
import p4gf_log
import p4gf_util

LOG = p4gf_log.for_module()

# In either global [SECTION_REPO_CREATION] or per-repo [SECTION_REPO] sections:
KEY_CHARSET                 = NTR('charset')
KEY_DESCRIPTION             = NTR('description')
KEY_ENABLE_BRANCH_CREATION  = NTR('enable-git-branch-creation')
KEY_ENABLE_MERGE_COMMITS    = NTR('enable-git-merge-commits')
KEY_ENABLE_SUBMODULES       = NTR('enable-git-submodules')
KEY_CHANGE_OWNER            = NTR('change-owner')

VALUE_AUTHOR                = NTR('author')
VALUE_PUSHER                = NTR('pusher')
VALUE_YES                   = NTR('yes')
VALUE_NO                    = NTR('no')
VALUE_NONE                  = NTR('none')

# In the global config file:
SECTION_REPO_CREATION      = NTR('repo-creation')
SECTION_GIT_TO_PERFORCE    = NTR('git-to-perforce')
SECTION_PERFORCE_TO_GIT    = NTR('perforce-to-git')
KEY_READ_PERMISSION_CHECK  = NTR('read-permission-check')
KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM \
                           = NTR('git-merge-avoidance-after-change-num')

# In the per-repo config files:
SECTION_REPO                        = NTR('@repo')
KEY_GIT_BRANCH_NAME                 = NTR('git-branch-name')
VALUE_GIT_BRANCH_NAME               = NTR('master')
KEY_VIEW                            = NTR('view')
KEY_STREAM                          = NTR('stream')
KEY_ORIGINAL_VIEW                   = NTR('original-view')
KEY_IGNORE_AUTHOR_PERMS             = NTR('ignore-author-permissions')
KEY_ENABLE_MISMATCHED_RHS           = NTR('enable-mismatched-rhs')
KEY_GIT_BRANCH_DELETED              = NTR('deleted')
KEY_DEPOT_BRANCH_ID                 = NTR('depot-branch-id')
KEY_PREFLIGHT_COMMIT                = NTR('preflight-commit')

# In either global or per-repo config files, with per-repo values
# overriding global values:
KEY_HTTP_URL                        = NTR('http_url')  # no default, not propagated to per-repo
KEY_SSH_URL                         = NTR('ssh_url')   # no default, not propagated to per-repo
SECTION_FEATURES                    = NTR('@features')
#FEATURE_TAGS                       = NTR('tags')
FEATURE_MATRIX2                     = NTR('matrix2')
FEATURE_IMPORTS                     = NTR('imports')
FEATURE_KEYS = {
  #FEATURE_TAGS     : "Preserve and reconstitute tags pushed from git to Perforce",
  FEATURE_MATRIX2   : _('git push decision matrix, version 2, with'
                        ' invisible-to-Git P4 changelists')
, FEATURE_IMPORTS   : _('Convert stream imports to git submodules')
}

# When a feature is ready to turn on all the time, add to this list.
#
# Eventually we'll want to completely remove the flag and any code that tests
# for it, but that's an intrusive code change that risks introducing bugs. Only
# do that near the start of a dev cycle.
#
_FEATURE_ENABLE_FORCE_ON = [
]


# Legal @xxx section names. Any other result in
# p4gf_config_validator.is_valid() rejection.
AT_SECTIONS = [
  SECTION_REPO
, SECTION_FEATURES
]


def depot_path_global():
    '''Return path to the global config file.'''
    return p4gf_const.P4GF_CONFIG_GLOBAL.format(P4GF_DEPOT = p4gf_const.P4GF_DEPOT)


def depot_path_repo(repo_name):
    '''Return the path to a repo's config file.'''
    return p4gf_const.P4GF_CONFIG_REPO.format(P4GF_DEPOT = p4gf_const.P4GF_DEPOT
                                             , repo_name = repo_name)


def depot_path_repo2(repo_name):
    '''Return the path to a repo's lightweight branch config file.'''
    return p4gf_const.P4GF_CONFIG_REPO2.format(P4GF_DEPOT = p4gf_const.P4GF_DEPOT
                                             , repo_name = repo_name)


def _rename_config_section(parser, old_sect, new_sect):
    '''
    Change the name of a given section in the config parser, moving
    all settings over to the new section name.
    '''
    parser.add_section(new_sect)
    for option in parser.options(old_sect):
        parser.set(new_sect, option, parser.get(old_sect, option))
    parser.remove_section(old_sect)


# pylint:disable=R0912
_INSTANCE_GLOBAL = None
def get_global(p4):
    '''
    Return a ConfigParser object that contains the current global
    configuration values.

    Returned instance is shared: multiple calls to get_global() return
    the same instance.
    '''
    global _INSTANCE_GLOBAL
    if _INSTANCE_GLOBAL:
        return _INSTANCE_GLOBAL
    _INSTANCE_GLOBAL = read_global(p4)
    if not _INSTANCE_GLOBAL:
        _INSTANCE_GLOBAL = default_config_global(p4)
    else:
        # Guard against malformed configuration files.
        if not _INSTANCE_GLOBAL.has_section(SECTION_REPO_CREATION):
            old_sect = NTR('p4gf-repo-creation')
            if _INSTANCE_GLOBAL.has_section(old_sect):
                _rename_config_section(_INSTANCE_GLOBAL, old_sect, SECTION_REPO_CREATION)
            else:
                _INSTANCE_GLOBAL.add_section(SECTION_REPO_CREATION)
        if not _INSTANCE_GLOBAL.has_option(SECTION_REPO_CREATION, KEY_CHARSET):
            charset = _get_p4_charset()
            _INSTANCE_GLOBAL.set(SECTION_REPO_CREATION, KEY_CHARSET, charset)
        if not _INSTANCE_GLOBAL.has_section(SECTION_GIT_TO_PERFORCE):
            _INSTANCE_GLOBAL.add_section(SECTION_GIT_TO_PERFORCE)
        if not _INSTANCE_GLOBAL.has_option(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_BRANCH_CREATION):
            # Transition old settings to new when reading old config files.
            value = _INSTANCE_GLOBAL.get(SECTION_REPO_CREATION, 'enable-branch-creation',
                                         fallback=VALUE_YES)
            _INSTANCE_GLOBAL.set(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_BRANCH_CREATION, value)
        if not _INSTANCE_GLOBAL.has_option(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_MERGE_COMMITS):
            # Transition old settings to new when reading old config files.
            value = _INSTANCE_GLOBAL.get(SECTION_REPO_CREATION, 'enable-branch-creation',
                                         fallback=VALUE_YES)
            _INSTANCE_GLOBAL.set(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_MERGE_COMMITS, value)
        if not _INSTANCE_GLOBAL.has_option(SECTION_GIT_TO_PERFORCE, KEY_CHANGE_OWNER):
            _INSTANCE_GLOBAL.set(SECTION_GIT_TO_PERFORCE, KEY_CHANGE_OWNER, VALUE_AUTHOR)
        if not _INSTANCE_GLOBAL.has_option(SECTION_GIT_TO_PERFORCE, KEY_PREFLIGHT_COMMIT):
            _INSTANCE_GLOBAL.set(SECTION_GIT_TO_PERFORCE, KEY_PREFLIGHT_COMMIT, VALUE_NONE)
        if not _INSTANCE_GLOBAL.has_option(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_SUBMODULES):
            _INSTANCE_GLOBAL.set(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_SUBMODULES, VALUE_YES)
    return _INSTANCE_GLOBAL
# pylint:enable=R0912


_INSTANCE_REPO = {}
def get_repo(p4, repo_name):
    '''
    Return a ConfigParser object that contains one repo's
    configuration values.

    If configuration is successfully read from p4, the returned instance
    is shared: multiple calls to get_repo() return the same instance for
    the same repo_name.  Otherwise, a new default configuration instance
    is returned each time.  That way when p4gf_init_repo creates a new
    config file it will end up in the cache, rather than being hidden by
    a default config created earlier.
    '''
    if repo_name in _INSTANCE_REPO:
        return _INSTANCE_REPO[repo_name]

    config = read_repo(p4, repo_name)
    if config:
        config = combine_repo_with_global(p4, config)
        _INSTANCE_REPO[repo_name] = config
    else:
        config = default_config_repo(p4, repo_name)
        config = combine_repo_with_global(p4, config)
    return config


def combine_repo_with_global(p4, config):
    """Add global settings for missing repo settings"""
    global_config = get_global(p4)
    if not config.has_section(SECTION_FEATURES):
        config.add_section(SECTION_FEATURES)
    for k in sorted(FEATURE_KEYS.keys()):
        if not config.has_option(SECTION_FEATURES, k):
            config.set(SECTION_FEATURES, k, global_config.get(SECTION_FEATURES, k,
                       fallback="False"))
    if not config.has_section(SECTION_REPO):
        config.add_section(SECTION_REPO)
    if not config.has_option(SECTION_REPO, KEY_ENABLE_BRANCH_CREATION):
        fallback = global_config.get(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_BRANCH_CREATION)
        # Transition old settings to new when reading old config files.
        value = config.get(SECTION_REPO, 'enable-branch-creation', fallback=fallback)
        config.set(SECTION_REPO, KEY_ENABLE_BRANCH_CREATION, value)
    if not config.has_option(SECTION_REPO, KEY_ENABLE_MERGE_COMMITS):
        fallback = global_config.get(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_MERGE_COMMITS)
        # Transition old settings to new when reading old config files.
        value = config.get(SECTION_REPO, 'enable-branch-creation', fallback=fallback)
        config.set(SECTION_REPO, KEY_ENABLE_MERGE_COMMITS, value)
    key_list = [ KEY_ENABLE_SUBMODULES, KEY_CHANGE_OWNER, KEY_PREFLIGHT_COMMIT ]
    for key in key_list:
        if not config.has_option(         SECTION_REPO,            key):
            config.set(                   SECTION_REPO,            key
                      , global_config.get(SECTION_GIT_TO_PERFORCE, key))
    for key in [KEY_HTTP_URL, KEY_SSH_URL]:
        if not config.has_option(         SECTION_REPO,            key):
            config.set(                   SECTION_REPO,            key
                      , global_config.get(SECTION_PERFORCE_TO_GIT, key, fallback=VALUE_NONE))
    if not config.has_option(SECTION_REPO, KEY_CHARSET):
        config.set(                   SECTION_REPO,          KEY_CHARSET
                  , global_config.get(SECTION_REPO_CREATION, KEY_CHARSET))
    return config


_INSTANCE_REPO2 = {}
def get_repo2(p4, repo_name):
    '''
    Return a ConfigParser object that contains one repo's
    lightweight branch configuration values.

    If configuration is successfully read from p4, the returned instance
    is shared: multiple calls to get_repo2() return the same instance for
    the same repo_name.  If read from file fails, returns None.
    '''
    if repo_name in _INSTANCE_REPO2:
        return _INSTANCE_REPO2[repo_name]

    config = read_repo2(p4, repo_name)
    if config:
        _INSTANCE_REPO2[repo_name] = config
    else:
        config = None
    return config


def read_file(file_path):
    '''Read the named file into a config object. Raises a RuntimeError if
    the parsing fails for any reason.
    '''
    with open(file_path, 'r') as f:
        contents = f.read()
    config = configparser.ConfigParser(interpolation  = None,
                                       allow_no_value = True)
    _read_string(config, file_path, contents)
    return config


def _read_string(config, file_path, contents):
    '''
    If unable to parse, convert generic ParseError to one that
    also contains a path to the unparsable file.
    '''
    try:
        config.read_string(contents)
    except configparser.Error as e:
        msg = _("Unable to read config file '{}'.\n{}").format(file_path, e)
        LOG.error(msg)
        raise RuntimeError(msg)


def _print_to_config(p4, file_path):
    '''
    p4 print a config file, parse it into a ConfigParser instance,
    return that ConfigParser instance.
    '''
    contents = _print_config_file(p4, file_path)
    if (   (contents == None)
        or (contents == '') ):
        return None

    config = configparser.ConfigParser(interpolation=None)
    _read_string(config, file_path, contents)
    return config


def read_global(p4):
    '''
    Read the global config file from Perforce into a new ConfigParser
    instance and return that instance.

    Returns None if file does not exist.
    '''
    return _print_to_config(p4, depot_path_global())


def read_repo(p4, repo_name):
    '''
    Read the repo config file from Perforce into a new ConfigParser
    instance and return that instance.

    Returns None if file does not exist.
    '''
    return _print_to_config(p4, depot_path_repo(repo_name))


def read_repo2(p4, repo_name):
    '''
    Read the lightweight branch config file from Perforce into a new ConfigParser
    instance and return that instance.

    Returns None if file does not exist.
    '''
    return _print_to_config(p4, depot_path_repo2(repo_name))


def create_file_global(p4):
    '''
    Write the global config to Perforce. So far, Git Fusion only writes the
    initial default global config, so there's no need to test for changes;
    just write unconditionally.
    '''
    depot_path = depot_path_global()
    file_content = file_content_global(default_config_global(p4))
    return _add_file(p4, depot_path, file_content)


def create_file_repo(ctx, repo_name, charset):
    """Create the config file for this one repo.

    This repo's client must already exist (p4gf_context.create_p4_client() must
    have already succeeded) so that we can read the view from that client and
    record it as the view mapping for git branch 'master'.
    """
    client_view  = ctx.clientmap.as_array()
    client = ctx.p4.fetch_client()
    if 'Stream' in client:
        config = default_config_repo_for_stream(ctx.p4gf, repo_name, client['Stream'])
    else:
        config = default_config_repo_for_view(ctx.p4gf, repo_name, client_view)
    if charset:
        config.set(SECTION_REPO, KEY_CHARSET, charset)
    create_file_repo_from_config(ctx, repo_name, config)


def create_file_repo_from_config(ctx, repo_name, config):
    """Create the config file for a repo from the given config."""
    file_content = file_content_repo(config)
    depot_path = depot_path_repo(repo_name)
    _add_file(ctx.p4gf, depot_path, file_content, ctx.client_spec_gf)


def create_file_repo_with_contents(p4, repo_name, config_file):
    '''
    Store the file contents of local text file <config_file> in Perforce
    as the repo config file for Git Fusion repo <repo_name>, only if no
    config file has already been stored for the repo.
    '''
    with open(config_file, 'r') as f:
        file_content = f.read()
    depot_path   = depot_path_repo(repo_name)
    _add_file(p4, depot_path, file_content)


def _add_file(p4, depot_path, file_content, client_spec=None):
    '''
    add a config file to Perforce using the Git Fusion object client
    '''
    old_client = p4.client
    p4.client = p4gf_util.get_object_client_name()
    added = False
    try:
        added = p4gf_util.add_depot_file(p4, depot_path, file_content, client_spec)
    finally:
        p4.client = old_client
    return added


def _print_config_file(p4, depot_path):
    '''
    Return a config file's content as a string.
    '''
    b = p4gf_util.print_depot_path_raw(p4, depot_path)
    if b:
        return b.decode()    # as UTF-8
    else:
        return None


def write_repo_if(p4, client, repo_name, config):
    '''
    If the config changed, write to Perforce. If it still matches what's
    already in Perforce, do nothing.
    '''
    depot_path = depot_path_repo(repo_name)
    got_file_content  = _print_config_file(p4, depot_path)
    want_file_content = file_content_repo(config)
    if got_file_content == want_file_content:
        return
    p4gf_util.edit_depot_file(p4, depot_path
                             , want_file_content, client)


def file_content_global(config):
    '''Convert a config object to file content that we'd write to Perforce.'''
    return to_text(comment_header_global(), config)


def file_content_repo(config):
    '''Convert a config object to file content that we'd write to Perforce.'''
    return to_text(comment_header_repo(), config)


def comment_header_global():
    '''
    Return the text dump that goes at the top of a newly created global
    config file.
    '''
    header = p4gf_util.read_bin_file(NTR('p4gf_config.global.txt'))
    if header is False:
        sys.stderr.write(_("no 'p4gf_config.global.txt' found\n"))
        header = _("# Missing p4gf_config.global.txt file!")
    return header


def comment_header_repo():
    '''
    Return the text dump that goes at the top of a newly created per-repo
    config file.
    '''
    header = p4gf_util.read_bin_file(NTR('p4gf_config.repo.txt'))
    if header is False:
        sys.stderr.write(_("no 'p4gf_config.repo.txt' found\n"))
        header = _('# Missing p4gf_config.repo.txt file!')
    return header


def _get_p4_charset():
    '''
    Retreive the value for P4CHARSET, or return 'utf8' if not set.
    '''
    p4 = p4gf_create_p4.create_p4(connect=False)
    charset = p4.env('P4CHARSET')
    if (not charset) or (charset == ''):
        charset = 'utf8'
    return charset


def _get_change_counter(p4):
    '''
    Return the current 'p4 counter change' value.
    '''
    r = p4gf_util.p4run_logged(p4, ['counter', 'change'])
    return r[0]['value']


def _get_merge_avoid_seed(p4):
    '''
    Return the current "change" counter, as a string.
    Return "1" if no changelists yet.
    '''
    counter = _get_change_counter(p4)
    if counter == "0":
        return "1"
    return counter


def default_config_global(p4):
    '''
    Return a ConfigParser instance loaded with default values.
    '''
    config = configparser.ConfigParser(interpolation  = None,
                                       allow_no_value = True)

    config.add_section(SECTION_REPO_CREATION)
    charset = _get_p4_charset()
    config.set(SECTION_REPO_CREATION, KEY_CHARSET, charset)
    config.add_section(SECTION_GIT_TO_PERFORCE)
    config.set(SECTION_GIT_TO_PERFORCE, KEY_CHANGE_OWNER, VALUE_AUTHOR)
    config.set(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_BRANCH_CREATION, VALUE_YES)
    config.set(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_MERGE_COMMITS, VALUE_YES)
    config.set(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_SUBMODULES, VALUE_YES)
    config.set(SECTION_GIT_TO_PERFORCE, KEY_PREFLIGHT_COMMIT, VALUE_NONE)
    config.set(SECTION_GIT_TO_PERFORCE, KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM
              , _get_merge_avoid_seed(p4))
    return config


def default_config_repo(p4, name):
    '''
    Return a ConfigParser instance loaded with default values for a
    single repo.

    Default values for a repo include a placeholder description and the
    charset which is copied from the default charset in the global config.
    '''
    global_config = get_global(p4)
    config = configparser.ConfigParser( interpolation  = None
                                      , allow_no_value = True)
    config.add_section(SECTION_REPO)
    config.set(SECTION_REPO, KEY_DESCRIPTION, _("Created from '{}'").format(name))
    config.set(SECTION_REPO, KEY_IGNORE_AUTHOR_PERMS, VALUE_NO)

    # Copy default values from global config file.
    config.set(                   SECTION_REPO,          KEY_CHARSET
              , global_config.get(SECTION_REPO_CREATION, KEY_CHARSET))
    key_list = [ KEY_ENABLE_BRANCH_CREATION
               , KEY_ENABLE_MERGE_COMMITS
               , KEY_ENABLE_SUBMODULES
               , KEY_CHANGE_OWNER
               , KEY_PREFLIGHT_COMMIT
               ]
    for key in key_list:
        config.set(                   SECTION_REPO,            key
                  , global_config.get(SECTION_GIT_TO_PERFORCE, key))

    return config


def view_map_to_client_name(view):
    '''
    Return the "myclient" portion of the first line in a client view
    mapping "//depot/blah/... //myclient/blah/..."
    '''
    p4map = P4.Map(view)
    LOG.debug("view='{}'".format(view))
    LOG.debug("rhs={}".format(p4map.rhs()))
    m = re.search('//([^/]+)/', p4map.rhs()[0])
    if (not m) or (not 1 <= len(m.groups())):
        return None
    return m.group(1)


def convert_view_to_no_client_name(view):
    '''
    Convert a view mapping's right-hand-side from its original client
    name to a new client name:

        //depot/dir/...  //client/dir/...
        //depot/durr/... //client/durr/...

    becomes

        //depot/dir/...  dir/...
        //depot/durr/... durr/...
    '''
    if not view:
        return []
    old_client_name = view_map_to_client_name(view)

    old_map = P4.Map(view)
    lhs = old_map.lhs()
    old_prefix = '//{}/'.format(old_client_name)
    new_prefix = ''
    rhs = [r.replace(old_prefix, new_prefix) for r in old_map.rhs()]
    new_map = P4.Map()
    for (l, r) in zip(lhs, rhs):
        new_map.insert(l, r)
    return '\n'.join(new_map.as_array())


def default_config_repo_for_view(p4, gf_branch_name, view):
    '''
    Return a ConfigParser instance loaded with default values for a
    single repo, using single view as the view for a single for Git
    branch: master.
    '''
    client_less = convert_view_to_no_client_name(view)
    return default_config_repo_for_view_plain(p4, gf_branch_name, client_less)


def default_config_repo_for_view_plain(p4, gf_branch_name, view):
    """Construct a ConfigParser using the client-less view.

    Return a ConfigParser instance loaded with default values for a
    single repo, using single view as the view for a single for Git
    branch: master.

    """
    config = default_config_repo(p4, gf_branch_name)
    sec = p4gf_util.uuid(p4)
    config.add_section(sec)
    config.set(sec, KEY_GIT_BRANCH_NAME, VALUE_GIT_BRANCH_NAME)
    config.set(sec, KEY_VIEW, view)
    return config


def default_config_repo_for_stream(p4, gf_branch_name, stream_name):
    '''
    Return a ConfigParser instance loaded with default values for a
    single repo, using stream to define the view for a single for Git
    branch: master.
    '''
    config = default_config_repo(p4, gf_branch_name)

    sec = p4gf_util.uuid(p4)
    config.add_section(sec)

    config.set(sec, KEY_GIT_BRANCH_NAME, VALUE_GIT_BRANCH_NAME)
    config.set(sec, KEY_STREAM, stream_name)

    return config


def to_text(comment_header, config):
    '''
    Produce a single string with a comment header and a ConfigParser, suitable
    for writing to file.
    '''
    out = io.StringIO()
    out.write(comment_header)
    config.write(out)
    file_content = out.getvalue()
    out.close()
    return file_content


def branch_section_list(config):
    '''
    Return a list of section names, one for each branch mapping section.

    Not every returned section name is guaranteed to be a correct and complete
    branch definition. Use p4gf_branch.Branch.from_config() to figure that out.
    '''
    return [s for s in config.sections() if not s.startswith('@')]


def clean_up_parser(config):
    """
    Break the reference cycles in the ConfigParser instance so the
    object can be garbage collected properly. The config instance
    should not be used after calling this function.
    """
    # pylint:disable=W0212
    # pylint "Access to a protected member"
    # Remove the reference cycle in each section
    sections = config.sections()
    # The default section is a special case
    sections.append(config.default_section)
    for section in sections:
        config._proxies[section]._parser = None
    # pylint:enable=W0212


def create_from_12x_gf_client_name(p4, gf_client_name):
    '''
    Upgrade from Git Fusion 12.x.

    Given the name of an existing Git Fusion 12.x client spec "git-fusion-{view-
    name}", copy its view into a Git Fusion 13.1 p4gf_config file, add and
    submit that file to Perforce.

    NOP if that p4gf_config file already exists.
    '''

    # Extract repo_name (nee view_name) from client spect's name.
    assert gf_client_name.startswith(p4gf_const.P4GF_CLIENT_PREFIX)
    repo_name   = gf_client_name[len(p4gf_const.P4GF_CLIENT_PREFIX):]

    # NOP if repo's p4gf_config already exists and is not deleted at head.
    depot_path = depot_path_repo(repo_name)
    if p4gf_util.depot_file_exists(p4, depot_path):
        return

    # Extract View lines from client spec, use them to create a new config.
    client_spec = p4.fetch_client(gf_client_name)
    view        = client_spec['View']
    config      = default_config_repo_for_view(p4, repo_name, view)

    # Write config to Perforce.
    config_file_content = file_content_repo(config)
    depot_path   = depot_path_repo(repo_name)
    _add_file(p4, depot_path, config_file_content)


def compare_configs_string(text1, text2):
    '''
    Converts the two strings to instances of ConfigParser and calls the
    compare_configs() function, returning the result. The config objects
    are properly cleaned up to avoid leaking memory.
    '''
    config1 = configparser.ConfigParser(interpolation=None)
    config1.read_string(text1)
    config2 = configparser.ConfigParser(interpolation=None)
    config2.read_string(text2)
    eq = compare_configs(config1, config2)
    clean_up_parser(config1)
    clean_up_parser(config2)
    return eq


def compare_configs(ac, bc):
    '''
    Compare two instances of ConfigParser and return True if identical,
    or False if they differ in sections, options, or values. The sections
    and their options are considered in sorted order, so insertion order
    will not affect the result.
    '''
    if ac is None and bc is None:
        return True
    a_sections = ac.sections()
    if ac.has_section(ac.default_section):
        a_sections.append(ac.default_section)
    b_sections = bc.sections()
    if bc.has_section(bc.default_section):
        b_sections.append(bc.default_section)
    if len(a_sections) != len(b_sections):
        return False
    a_sections = sorted(a_sections)
    b_sections = sorted(b_sections)
    for a_section, b_section in zip(a_sections, b_sections):
        if a_section != b_section:
            return False
    for sect_name in a_sections:
        a_options = ac.options(sect_name)
        b_options = bc.options(sect_name)
        if len(a_options) != len(b_options):
            return False
        a_options = sorted(a_options)
        b_options = sorted(b_options)
        for a_option, b_option in zip(a_options, b_options):
            if a_option != b_option:
                return False
            if ac.get(sect_name, a_option) != bc.get(sect_name, a_option):
                return False
    return True


def is_feature_enabled(config, feature):
    '''
    Check if a feature is enabled in a a repo's config
    Default to False if not set.
    '''
    if feature in _FEATURE_ENABLE_FORCE_ON:
        return True
    return config.getboolean(SECTION_FEATURES, feature, fallback=False)


def configurable_features():
    '''
    Return sorted list of configurable features.
    This list does not include any features which are forced on.
    Suitable for producing @features output.
    '''
    return sorted([key for key in FEATURE_KEYS.keys() if not key in _FEATURE_ENABLE_FORCE_ON])


def main():
    '''
    Parse the command-line arguments and print a configuration.
    '''
    p4gf_util.has_server_id_or_exit()
    p4gf_client = p4gf_util.get_object_client_name()
    p4 = p4gf_create_p4.create_p4(client=p4gf_client)
    if not p4:
        sys.exit(1)
    desc = _("""Display the effective global or repository configuration.
All comment lines are elided and formatting is normalized per the
default behavior of the configparser Python module.
The default configuration options will be produced if either of the
configuration files is missing.
""")
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument(NTR('repo'), metavar=NTR('R'), nargs='?', default='',
        help=_('name of the repository, or none to display global.'))
    args = parser.parse_args()
    if args.repo:
        cfg = get_repo(p4, args.repo)
    else:
        cfg = get_global(p4)
    if not cfg:
        print(_('Unable to read configuration file!'))
    cfg.write(sys.stdout)


if __name__ == "__main__":
    main()
