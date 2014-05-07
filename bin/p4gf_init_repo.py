#! /usr/bin/env python3.3
'''
See p4gf_init_repo.help.txt

Create config data to map <view> to a Git Fusion client,
local filesystem location for .git and workspace data.

<view> must be an existing Perforce client spec. Its view mapping is copied
into the git repo's config. After p4gf_init_repo.py completes, Git Fusion no
longer uses or needs this <view> client spec, you can delete it or use it for
your own purposes. We just needed to copy its view mapping once. Later changes
to this view mapping are NOT propagated to Git Fusion. <view> cannot be a
stream client.

p4gf_init_repo.py creates a new Perforce client spec 'git-fusion-<view>'. This
is the client for this view, which Git Fusion uses for all operations
within this repo/view.

p4gf_init_repo.py initializes an empty git repo for this view.

NOP if a view with this name already exists.
'''

import logging
import os
import re
import socket
import sys
import traceback

import P4

import p4gf_env_config    # pylint: disable=W0611
import p4gf_branch
import p4gf_branch_id
import p4gf_config
from   p4gf_config_validator import Validator
import p4gf_const
import p4gf_copy_p2g
import p4gf_copy_to_p4
import p4gf_context   # Intentional mis-sequence avoids pylint Similar lines in 2 files
import p4gf_create_p4
import p4gf_git
import p4gf_gitmirror
import p4gf_group
import p4gf_init
from   p4gf_l10n             import _, NTR, log_l10n
import p4gf_lock
import p4gf_log
import p4gf_object_type
import p4gf_proc
import p4gf_rc
import p4gf_streams
import p4gf_usermap
import p4gf_util
import p4gf_version
import p4gf_view_dirs
import p4gf_translate

LOG = p4gf_log.for_module()
INIT_REPO_EXISTS  = 0            # repo already exists, but may be updated
INIT_REPO_OK      = 1            # repo created successfully
INIT_REPO_NOVIEW  = 2            # missing required template client
INIT_REPO_BADVIEW = 3            # Git Fusion view malformed
INIT_REPO_CONFIG_FILE_MISSING = 4# Git Fusion repo config file specified with
                                 # --config does not exist
INIT_REPO_CONFIG_FILE_BAD = 5    # Git Fusion repo config file malformed, or
                                 # changes config in a way that we can tell
                                 # will break history.
INIT_REPO_START_BAD = 6          # --start=N value not an integer
                                 # or no P4 changelists at or after value.
INIT_REPO_BAD_CHARSET = 7        # invalid charset specified with --charset
CLIENT_OPTIONS = NTR('allwrite clobber nocompress unlocked nomodtime normdir')
CLIENT_LESS_REGEX = re.compile(r'"?//[^/]+/(.*)')


def _print_stderr(msg):
    '''
    Write text to stderr.

    Appends its own trailing newline so that you don't have to.
    '''
    sys.stderr.write(msg + '\n')


# pylint: disable=R0913
def _create_p4_client(p4, view_name, client_name, client_root,
        enable_mismatched_rhs, view_name_p4client, handle_imports=True):
    """Create the p4 client to contain Git meta-data mirror.

    Keyword arguments:
    p4                    -- Perforce client API
    view_name             -- client view of repository to clone - internal (translated) viewname
    client_name           -- client that will be created
    client_root           -- path for client workspace
    enable_mismatched_rhs -- allow branches to have differing RHS?
    view_name_p4client    -- name of actual p4 client on which to base this new repo
                             if None - will be determined from view_name if needed

    Returns one of the INIT_REPO_* constants.
    """
    # Ensure the client root directory has been created.
    if not os.path.exists(client_root):
        os.makedirs(client_root)

    view_name_git = p4gf_translate.TranslateReponame.repo_to_git(view_name)
    LOG.debug("_create_p4_client(): view_name_git {0}   view_name {1}   view_name_p4client {2}".
            format(view_name_git, view_name ,view_name_p4client))

    # If a Git Fusion client for this view already exists, use that,
    # no need to init the repo.
    # Do make sure that the client root is correct.
    if p4gf_util.spec_exists(p4, 'client', client_name):
        LOG.debug("%s client already exists for %s", client_name, view_name)
        p4gf_util.ensure_spec_values(p4, 'client', client_name
            , { 'Root'    : client_root
              , 'Options' : CLIENT_OPTIONS })
        return INIT_REPO_EXISTS

    # Repo config file already checked into Perforce?
    # Use that.
    config_path   = p4gf_config.depot_path_repo(view_name)
    config_exists = p4gf_util.depot_file_exists(p4, config_path)
    if config_exists:
        return repo_from_config( p4, view_name, client_name, client_root
                               , enable_mismatched_rhs)

    # Client exist with the same name as this Git Fusion repo?
    # Build a new config, check it into Perforce, and use that.
    if not view_name_p4client:
        view_name_p4client = p4gf_translate.TranslateReponame.git_to_p4client(view_name_git)
    nop4client = ''
    if p4gf_util.spec_exists(p4, 'client', view_name_p4client):
        return repo_from_template_client( p4, view_name, view_name_p4client, client_name
                                        , client_root
                                        , enable_mismatched_rhs)
    else:
        nop4client = _("p4 client '{0}' does not exist\n").format(view_name_p4client)

    # creating repo from stream?
    # note that we can't pass '//depot/stream' because git would be confused
    # but it's ok to pass 'depot/stream' and add the leading '//' here
    stream_name = '//'+view_name_git
    if p4gf_util.spec_exists(p4, 'stream', stream_name):
        return _repo_from_stream(p4, view_name, stream_name, client_name, client_root,
                                 enable_mismatched_rhs, handle_imports)

    # We don't have, and cannot create, a config for this repo.
    # Say so and give up.
    msg = p4gf_const.NO_REPO_MSG_TEMPLATE.format(view_name=view_name
            ,view_name_p4client=view_name_p4client
            ,nop4client=nop4client)
    LOG.warn(msg)
    _print_stderr(msg)
    return INIT_REPO_NOVIEW


def repo_from_template_client( p4, view_name, view_name_p4client, client_name
                             , client_root, enable_mismatched_rhs):
    '''
    Create a new Perforce client spec <client_name> using existing Perforce
    client spec <view_name> as a template (just use its View).
    '''
    # view_name_p4client is the p4client
    # view_name        is the gfinternal repo name
    # view_name differs from view_name_p4client if latter contains special chars
    #           or was configured with --p4client argument


    if not p4gf_util.spec_exists(p4, 'client', view_name_p4client):
        return INIT_REPO_NOVIEW

    client = p4.run('client', '-o', view_name_p4client)[0]
    if 'Stream' in client:
        return _repo_from_stream(p4, view_name, client.get('Stream'),
                                 client_name, client_root, enable_mismatched_rhs)

    with Validator.from_template_client(view_name, p4, view_name_p4client) as validator:
        if not validator.is_valid(enable_mismatched_rhs):
            return INIT_REPO_CONFIG_FILE_BAD

    # Seed a new client using the view's view as a template.
    LOG.info("Git Fusion client %s does not exist,"
             " creating from existing Perforce client %s"
            , client_name, view_name_p4client)

    view = p4gf_util.first_value_for_key(
            p4.run('client', '-o', '-t', view_name_p4client, client_name),
            'View')

    create_repo_client(p4, view_name, client_name, client_root, view, None)
    return INIT_REPO_OK


def _repo_from_stream(p4, view_name, stream_name, client_name,
                      client_root, enable_mismatched_rhs, handle_imports=True):
    """Create a new client from the named stream.

    Create a new Perforce client spec <client_name> using existing Perforce
    stream spec <stream_name> as a template (just use its View).

    Returns one of the INIT_REPO_* constants.

    """
    # stream_name      is the name of a stream, e.g. '//depot/stream'
    # view_name        is the gfinternal repo name

    if not p4gf_util.spec_exists(p4, 'stream', stream_name):
        return INIT_REPO_NOVIEW

    with Validator.from_stream(view_name, p4, stream_name) as validator:
        if not validator.is_valid(enable_mismatched_rhs):
            return INIT_REPO_CONFIG_FILE_BAD

    # Seed a new client using the stream's view as a template.
    LOG.info("Git Fusion client %s does not exist, creating from existing Perforce stream %s",
             client_name, stream_name)

    # Create virtual stream with excluded paths, use that for client.
    stream = p4.fetch_stream(stream_name)
    if handle_imports:
        config = p4gf_config.get_repo(p4, view_name)
        imports_enabled = p4gf_config.is_feature_enabled(config, p4gf_config.FEATURE_IMPORTS)
    else:
        imports_enabled = False
    if imports_enabled:
        stream_paths = p4gf_streams.stream_import_exclude(stream['Paths'])
    else:
        stream_paths = stream['Paths']
    desc = (_("Created by Perforce Git Fusion for work in '{view}'.")
            .format(view=p4gf_translate.TranslateReponame.repo_to_git(view_name)))
    spec_values = {
        'Owner': p4gf_const.P4GF_USER,
        'Parent': stream_name,
        'Type': 'virtual',
        'Description': desc,
        'Options': 'notoparent nofromparent',
        'Paths': stream_paths,
        'Remapped': ['.gitmodules-{} .gitmodules'.format(view_name)]
    }
    if imports_enabled:
        stream_name += '_p4gfv'
        p4gf_util.set_spec(p4, 'stream', spec_id=stream_name, values=spec_values)
        LOG.debug('virtual stream {} created for {}'.format(stream_name, client_name))
    create_repo_client(p4, view_name, client_name, client_root, None, stream_name)

    return INIT_REPO_OK


def create_repo_client(p4, view_name, client_name, client_root, view, stream):
    '''Create a Git Fusion repo client.'''
    desc = (_("Created by Perforce Git Fusion for work in '{view}'.")
            .format(view=p4gf_translate.TranslateReponame.repo_to_git(view_name)))
    # if creating from a stream, set 'Stream' but not 'View'
    # otherwise, set 'View' but not 'Stream'
    if stream:
        p4gf_util.set_spec(p4, 'client', spec_id=client_name,
                         values={'Owner'         : p4gf_const.P4GF_USER,
                                 'LineEnd'       : NTR('unix'),
                                 'Root'          : client_root,
                                 'Options'       : CLIENT_OPTIONS,
                                 'Host'          : None,
                                 'Stream'        : stream,
                                 'Description'   : desc})
    else:
        p4gf_util.set_spec(p4, 'client', spec_id=client_name,
                         values={'Owner'         : p4gf_const.P4GF_USER,
                                 'LineEnd'       : NTR('unix'),
                                 'View'          : view,
                                 'Root'          : client_root,
                                 'Options'       : CLIENT_OPTIONS,
                                 'Host'          : None,
                                 'Description'   : desc})

    LOG.debug("Successfully created Git Fusion client %s", client_name)
# pylint: enable=R0913


def _submodule_url(ctx):
    """Retrieve the appropriate repo URL for the given context."""
    # Check for a standard HTTP environment variable since I am unaware of
    # any equivalent for SSH (that is also set by our T4 test suite).
    using_http = 'REMOTE_ADDR' in os.environ
    key = p4gf_config.KEY_HTTP_URL if using_http else p4gf_config.KEY_SSH_URL
    config = p4gf_config.get_repo(ctx.p4gf, ctx.config.view_name)
    url = config.get(p4gf_config.SECTION_REPO, key, fallback=None)
    if url is None:
        LOG.error('Git Fusion configuration missing ssh_url/http_url values.')
        url = 'http://please/contact/your/perforce/administrator'
    else:
        args = {}
        args['user'] = 'git'  # fallback value
        for env_key in ['LOGNAME', 'USER', 'USERNAME']:
            if env_key in os.environ:
                args['user'] = os.environ[env_key]
                break
        args['host'] = socket.gethostname()
        args['repo'] = ctx.config.view_name
        url = url.format(**args)
    LOG.debug('_submodule_url([{}]/{}) -> {}'.format(p4gf_config.SECTION_REPO, key, url))
    return url


# pylint:disable=R0913
# Too many arguments
def copy_submodule(ctx, repo_name, subtxt, local_path, change_num, user_3tuple):
    """Copy from Perforce to Git the submodule changes.

    Arguments:
        ctx -- parent repo context.
        repo_name -- name of submodule repo.
        subtxt -- context for submodule repo.
        local_path -- path within parent repo where submodule will go.
        user_3tuple -- (p4user, email, fullname) for Git Fusion user

    Returns the new SHA1 of the parent repo and an error string, or None
    if successful.

    """
    cwd = os.getcwd()
    if subtxt.view_repo is None:
        subtxt.get_view_repo()
    os.chdir(subtxt.view_dirs.GIT_WORK_TREE)
    LOG.debug('copy_submodule() copying changes for {}'.format(repo_name))
    p4gf_copy_p2g.copy_p2g_ctx(subtxt)
    # if available, use the requested change to get the corresponding SHA1 of the submodule
    commit_ot = None
    changes = subtxt.p4run(['changes', '-m1', subtxt.client_view_path(change_num)])
    if changes:
        real_dict = p4gf_util.first_dict_with_key(changes, 'change')
        if real_dict:
            real_change = real_dict['change']
            commit_ot = p4gf_object_type.ObjectType.commit_for_change(subtxt, real_change, None)
    if commit_ot:
        sub_sha1 = commit_ot.sha1
        LOG.debug2('copy_submodule() using commit {}'.format(sub_sha1))
    else:
        # otherwise use the latest commit
        sub_sha1 = subtxt.view_repo.head.hex
        LOG.debug2('copy_submodule() using HEAD: {}'.format(sub_sha1))
    os.chdir(cwd)
    url = _submodule_url(subtxt)
    if local_path.endswith('...'):
        local_path = local_path[:-3]
    local_path = local_path.rstrip('/')
    LOG.debug('adding submodule {} to {} as {}'.format(local_path, repo_name, user_3tuple[0]))
    p4gf_git.add_submodule(ctx.view_repo, repo_name, local_path, sub_sha1, url, user_3tuple)
# pylint:enable=R0913


def deport_submodules(ctx, import_paths, user_3tuple):
    """Find any submodules that Git Fusion controls which should be removed.

    Arguments:
        ctx -- parent repo context.
        import_paths -- current set of import paths in stream.
        user_3tuple -- (p4user, email, fullname) for Git Fusion user

    """
    # parse the .gitmodules file into an instance of ConfigParser
    modules = p4gf_git.parse_gitmodules(ctx.view_repo)
    LOG.debug('deport_submodules() checking for defunct submodules in {}'.format(
        ctx.config.p4client))
    # find those sections whose 'path' no longer matches any of the imports
    for section in modules.sections():
        # we can only consider those submodules under our control
        if modules.has_option(section, p4gf_const.P4GF_MODULE_TAG):
            path = modules.get(section, 'path', raw=True, fallback=None)
            if not path:
                LOG.warn(".gitmodules entry {} has {} but no 'path'".format(
                    section, p4gf_const.P4GF_MODULE_TAG))
                continue
            LOG.debug('deport_submodules() considering import {}'.format(path))
            # append the usual suffix for easier comparison
            view_path = path + '/...'
            present = False
            for impath in import_paths:
                if impath[0] == view_path:
                    present = True
                    break
            if not present:
                LOG.debug('deport_submodules() removing submodule at {}'.format(path))
                # removal happens for each submodule separately because
                # merging the writes into a single tree is tricky
                p4gf_git.remove_submodule(ctx.view_repo, path, user_3tuple)


# pylint:disable=R0914, R0915
# have already split this function several times...
def import_submodules(ctx, view, change_view, import_paths):
    """For stream clients, create a submodule for each import.

    Arguments:
        ctx -- parent repo context.
        view -- the parent stream's 'View'.
        change_view -- the parent stream's 'ChangeView'.
        import_paths -- result from p4gf_streams.match_import_paths() on the
                        virtual stream's paths and the parent stream's paths.

    """
    usermap = p4gf_usermap.UserMap(ctx.p4gf)
    user_3tuple = usermap.lookup_by_p4user(p4gf_const.P4GF_USER)
    if not user_3tuple:
        LOG.error('Missing Perforce user {}'.format(p4gf_const.P4GF_USER))
        return
    client_name = ctx.config.p4client
    LOG.debug('processing imports for {}'.format(client_name))
    LOG.debug3('import_submodules() view={}, change_view={}, import_paths={}'.format(
        view, change_view, import_paths))
    change_views = p4gf_streams.stream_imports_with_changes(view, change_view, import_paths)
    LOG.debug2('import_submodules() change_views={}'.format(change_views))
    if not change_views and LOG.isEnabledFor(logging.DEBUG2):
        LOG.debug2('import_submodules() view={} change_view={} import_paths={}'.format(
            view, change_view, import_paths))
    # initialize and populate the submodules
    old_sha1 = ctx.view_repo.lookup_reference('HEAD').resolve().hex
    for depot_path, change_num, local_path in change_views:
        # avoid double-nesting by excluding the local path from the client path
        client_path = "//{}/...".format(client_name)
        LOG.debug('import_submodules() for {} => {}'.format(depot_path, client_path))
        stream_name = depot_path[:-4]
        if p4gf_util.spec_exists(ctx.p4, 'stream', stream_name):
            # convert stream name to repo name by pruning leading slashes
            repo_name = p4gf_streams.repo_name_from_depot_path(stream_name)
            config = None
            LOG.debug('initializing stream import for {}'.format(depot_path))
        else:
            # create a repo configuration file for this 1-line view
            repo_name = p4gf_streams.repo_name_from_depot_path(depot_path)
            client_less_path = CLIENT_LESS_REGEX.match(client_path).group(1)
            if client_path and client_path[0] == '"':
                client_less_path = '"' + client_less_path
            repo_view = depot_path + " " + client_less_path
            LOG.debug('creating config for {}'.format(repo_name))
            config = p4gf_config.default_config_repo_for_view_plain(ctx.p4, repo_name, repo_view)
        # prepare to initialize the repository
        p4 = p4gf_create_p4.create_p4()
        if not p4:
            LOG.error('unable to create P4 instance for {}'.format(repo_name))
            return
        with p4gf_lock.view_lock(p4, repo_name) as view_lock:
            if config:
                p4gf_config.create_file_repo_from_config(ctx, repo_name, config)
            LOG.debug('initializing repo for {}'.format(repo_name))
            result = init_repo(p4, repo_name, view_lock, handle_imports=False)
            if result > INIT_REPO_OK:
                return result
            with p4gf_context.create_context(repo_name, view_lock) as subtxt:
                # set up gitmirror for child repos
                p4gf_gitmirror.setup_spawn(repo_name)
                # populate the submodule
                shared_in_progress = p4gf_lock.shared_host_view_lock_exists(subtxt.p4, repo_name)
                if not shared_in_progress:
                    copy_submodule(ctx, repo_name, subtxt, local_path, change_num, user_3tuple)
        p4gf_create_p4.p4_disconnect(p4)
    # Remove any submodules controlled by Git Fusion that no longer match
    # any of the current import paths.
    deport_submodules(ctx, import_paths, user_3tuple)
    #
    # Ensure the Git commits we just created are copied back to Perforce by
    # faking a 'push' from the client. Roll the HEAD reference ('master')
    # back to the old SHA1, assign the commits to Perforce branches, then
    # move the reference back to the latest commit and copy everything to
    # the depot as usual.
    #
    new_head = ctx.view_repo.lookup_reference('HEAD').resolve()
    ctx.view_repo.git_reference_create(new_head.name, old_sha1, True)
    prt = p4gf_branch_id.PreReceiveTuple(old_sha1, new_head.hex, new_head.name)
    LOG.debug('Copying modules to depot: {}'.format(prt))
    assigner = p4gf_branch_id.Assigner(ctx.branch_dict(), [prt], ctx)
    assigner.assign()
    ctx.view_repo.git_reference_create(new_head.name, new_head.hex, True)
    err = p4gf_copy_to_p4.copy_git_changes_to_p4(ctx, prt, assigner, None)
    if err:
        LOG.error(err)
# pylint:enable=R0914, R0915


def process_imports(ctx):
    """Ensure stream imports are processed appropriately.

    The parent repository has already been initialized and populated.
    This function only applies to clients with a stream that contains
    import(ed) paths. For all other clients this will be a NOP.

    """
    if not ctx.is_feature_enabled(p4gf_config.FEATURE_IMPORTS):
        return
    # check if this client has a virtual stream
    # (need to force the stream-ish definition of the client, if one exists)
    branch = p4gf_branch.most_equal(ctx.branch_dict())
    LOG.debug2('process_imports() branch {}'.format(branch))
    if branch and branch.stream_name:
        LOG.debug2('process_imports() switching to {}'.format(branch))
        ctx.switch_client_to_stream(branch)
    client = ctx.p4.fetch_client()
    LOG.debug2('process_imports() checking {} for a stream'.format(client['Client']))
    if 'Stream' not in client:
        LOG.debug2('process_imports() {} is not a stream client'.format(client['Client']))
        return
    virt_stream = client['Stream']
    virtual = ctx.p4.fetch_stream(virt_stream)
    if 'Parent' not in virtual or virtual['Parent'] == 'none':
        LOG.debug2('process_imports() {} has no parent'.format(virt_stream))
        return
    if virtual['Type'] != 'virtual':
        LOG.debug2('process_imports() {} created prior to submodules support'.format(virt_stream))
        return
    parent = p4gf_util.first_dict(ctx.p4.run('stream', '-ov', virtual['Parent']))
    LOG.debug3('process_imports() parent stream={}'.format(parent))
    v_paths = virtual['Paths']
    p_paths = parent['Paths']
    import_paths = p4gf_streams.match_import_paths(v_paths, p_paths)
    if not import_paths:
        LOG.debug2('process_imports() {} has no exclude paths'.format(virt_stream))
        return
    if ctx.view_repo is None:
        # ensure the pygit2 repository is ready to go
        ctx.get_view_repo()
    #pylint:disable=W0703
    try:
        import_submodules(ctx, parent['View'], parent.get('ChangeView'), import_paths)
    except Exception:
        LOG.error("submodule imports failed...\n{}".format(traceback.format_exc()))
    #pylint:enable=W0703


def repo_from_config(p4, view_name, client_name, client_root, enable_mismatched_rhs):
    '''
    Create a new Git Fusion repo client spec for this repo.
    The branch_id section should not matter since we now support
    all listed branches, but we need to initially set the client view
    to _something_, so pick one from config.branch_section_list[0].
    '''
    with Validator.from_depot_p4gf_config(view_name, p4) as validator:
        if not validator.is_valid(enable_mismatched_rhs):
            return INIT_REPO_CONFIG_FILE_BAD
        # borrow the validator's config just a little while longer...
        section_name = p4gf_config.branch_section_list(validator.config)[0]
        branch = p4gf_branch.Branch.from_config(validator.config, section_name, p4)
        branch.set_rhs_client(client_name)
    try:
        create_repo_client(p4, view_name, client_name, client_root, branch.view_lines,
                           branch.stream_name)
    except P4.P4Exception as e:
        # Prefix error message with additional context.
        config_path = NTR('{P4GF_DEPOT}/repos/{repo}/p4gf_config')\
                        .format(P4GF_DEPOT=p4gf_const.P4GF_DEPOT, repo=view_name)
        e.value =  (_("\nError while creating Git branch '{branch}' for repo '{repo}'"
                      "\nCheck the branch view specifications in Git Fusion: {config_path}"
                      "\nDetails: {error_details}")
                    .format( branch        = branch.git_branch_name
                           , repo          = view_name
                           , config_path   = config_path
                           , error_details = e.value ))
        raise
    return INIT_REPO_OK


def depot_from_view_lhs(lhs):
    """extract depot name from lhs of view line"""
    return re.search('^\"?[+-]?//([^/]+)/.*', lhs).group(1)


def create_p4_client_root(p4root):
    """Create a directory to hold the p4 client workspace root."""
    if not os.path.exists(p4root):
        LOG.debug("creating directory %s for p4 client workspace root", p4root)
        os.makedirs(p4root)


def create_perm_groups(p4, view_name):
    """Create the pull and push permission groups, initially empty."""
    p4gf_group.create_view_perm(p4, view_name, p4gf_group.PERM_PULL)
    p4gf_group.create_view_perm(p4, view_name, p4gf_group.PERM_PUSH)


# pylint: disable=R0913
def init_repo(p4
             , view_name
             , view_lock
             , charset=None
             , enable_mismatched_rhs=False
             , view_name_p4client=None
             , handle_imports=True):
    '''
    Create view and repo if necessary. Does NOT copy p4 to the repo
    (that's p4gf_copy_p2g's job). Returns one of the INIT_REPO_* constants.

    This is p4gf_auth_server's entry point into init_repo, called in response
    to a 'git clone'.

    view_name is the internal view_name with special chars already translated

    view_lock           CounterLock mutex that prevents other processes from
                        touching this repo, whether on this Git Fusion server
                        or another.
    '''
    LOG.debug("init_repo : view_name {1} view_name_p4client {1}".format(
        view_name, view_name_p4client))
    client_name = p4gf_util.view_to_client_name(view_name)
    p4gf_dir = p4gf_util.p4_to_p4gf_dir(p4)
    view_dirs = p4gf_view_dirs.from_p4gf_dir(p4gf_dir, view_name)

    result = _create_p4_client(p4, view_name, client_name, view_dirs.p4root,
                               enable_mismatched_rhs, view_name_p4client, handle_imports)
    if result > INIT_REPO_OK:
        return result
    create_perm_groups(p4, view_name)
    with p4gf_context.create_context(view_name, view_lock) as ctx:
        p4gf_config.create_file_repo(ctx, view_name, charset)
        if ctx.client_exclusions_added:
            _print_stderr(_("The referenced client view contains implicit exclusions."
                            "\nThe Git Fusion config will contain these as explicit exclusions."))
        p4gf_copy_p2g.create_git_repo(ctx, view_dirs.GIT_DIR)
    create_p4_client_root(view_dirs.p4root)
    p4gf_rc.update_file(view_dirs.rcfile, client_name, view_name)
    if result == INIT_REPO_OK:
        LOG.debug("repository creation for %s complete", view_name)
    # return the result of creating the client, to indicate if the client
    # had already been set up or not
    return result
# pylint: enable=R0913


def copy_p2g_with_start(view_name, start, view_lock, ctx=None):
    """Invoked 'p4gf_init_repo.py --start=NNN': copy changes from @NNN to #head."""
    if ctx is None:
        ctx = p4gf_context.create_context(view_name, view_lock)
    with ctx:
        LOG.debug("connected to P4, p4gf=%s", ctx.p4gf)
        # Check that there are changes to be copied from any branch.
        ctx.switch_client_view_to_union()
        path = ctx.client_view_path()
        changes_result = ctx.p4.run("changes", "-m1", "{}@{},#head".format(path, start))
        if len(changes_result):
            # Copy any recent changes from Perforce to Git.
            print(_("Copying changes from '{}'...").format(start))
            p4gf_copy_p2g.copy_p2g_ctx(ctx, start)
            print(_('Copying completed.'))
        else:
            msg = _("No changes above '{}'.").format(start)
            if int(start) == 1:
                LOG.debug(msg)
            else:
                LOG.info(msg)
                raise IndexError(msg)


def _parse_argv():
    '''Convert argv into a usable dict. Dump usage/help and exit if necessary.'''
    help_txt = p4gf_util.read_bin_file('p4gf_init_repo.help.txt')
    if help_txt is False:
        help_txt = _("Missing '{}' file!").format(NTR('p4gf_init_repo.help.txt'))
    parser = p4gf_util.create_arg_parser(
          desc        = _('Configure and populate Git Fusion repo.')
        , epilog      = None
        , usage       = _('p4gf_init_repo.py [options] <name>')
        , help_custom = help_txt)
    parser.add_argument('--start',   metavar="")
    parser.add_argument('--noclone', action=NTR('store_true'))
    parser.add_argument('--config')
    parser.add_argument('--p4client')
    parser.add_argument(NTR('view'),      metavar=NTR('view'))
    parser.add_argument('--charset')
    parser.add_argument('--enablemismatchedrhs', action=NTR('store_true'))
    args = parser.parse_args()
    if args.noclone and args.start:
        _print_stderr(_('Cannot use both --start and --noclone'))
        sys.exit(1)
    if args.config and args.charset:
        _print_stderr(_('Cannot use both --config and --charset'))
    if args.config and args.p4client:
        _print_stderr(_('Cannot use both --config and --p4client'))
    LOG.debug("args={}".format(args))
    return args


def populate_repo(view_name, view_lock, start):
    '''Populate the repo from Perforce'''
    try:
        start_at = int(start.lstrip('@')) if start else 1
    except ValueError:
        _print_stderr(_('Invalid --start value: {}').format(start))
        return INIT_REPO_START_BAD

    try:
        copy_p2g_with_start(view_name, start_at, view_lock)
    except ValueError:
        _print_stderr(_("Invalid --start value: {}").format(start))
        return INIT_REPO_START_BAD
    except IndexError:
        _print_stderr(_("Could not find changes >= '{}'").format(start_at))
        return INIT_REPO_START_BAD
    return INIT_REPO_EXISTS


def main():
    """set up repo for a view"""
    p4gf_util.has_server_id_or_exit()
    args = _parse_argv()
    p4gf_version.log_version()
    log_l10n()
    # !!! view_name_git    the untranslated repo name
    # !!! view_name        the translated repo name
    view_name_p4client = None
    if args.p4client:
        view_name_p4client = p4gf_util.argv_to_view_name(args.p4client)
    view_name_git = p4gf_util.argv_to_view_name(args.view)
    #strip leading '/' to conform with p4gf_auth_server behavior
    if view_name_git[0] == '/':
        view_name_git = view_name_git[1:]
    view_name = p4gf_translate.TranslateReponame.git_to_repo(view_name_git)
    p4gf_gitmirror.setup_spawn(view_name)
    p4gf_util.reset_git_enviro()

    p4 = p4gf_create_p4.create_p4()
    if not p4:
        return INIT_REPO_NOVIEW

    LOG.debug("connected to P4 at %s", p4.port)
    p4gf_proc.init()
    try:
        with p4gf_create_p4.Closer():
            p4gf_version.version_check()

            with p4gf_lock.view_lock(p4, view_name) as view_lock:
                # Ensure we have a sane environment.
                p4gf_init.init(p4)

                # Now that we can trust that the git-fusion--p4 client exists,
                # switch to that. Change takes effect immediately, don't need to
                # re-run p4.connect().
                p4.client = p4gf_util.get_object_client_name()

                # If local config file specified, validate it and store in
                # Perforce now. Even if client exists (aka repo was already
                # inited), this is one way for an admin to modify an existing
                # repo's config.
                if args.config:
                    if not os.path.exists(args.config):
                        _print_stderr(_("error: missing config file '{}'").format(args.config))
                        return INIT_REPO_CONFIG_FILE_MISSING
                    with Validator.from_local_file(view_name, p4, args.config) as validator:
                        if not validator.is_valid(args.enablemismatchedrhs):
                            return INIT_REPO_CONFIG_FILE_BAD
                    p4gf_config.create_file_repo_with_contents(p4, view_name, args.config)

                elif args.charset and not Validator.valid_charset(args.charset):
                    _print_stderr(_("error: invalid charset: {}").format(args.charset))
                    return INIT_REPO_BAD_CHARSET

                # Initialize the repository if necessary.
                print(_("Initializing '{}'...").format(view_name))
                r = init_repo(p4, view_name, view_lock, args.charset, args.enablemismatchedrhs,
                        view_name_p4client)
                if r > INIT_REPO_OK:
                    return r
                print(_("Initialization complete."))

                # Write --enablemismatchedrhs to config file
                if args.enablemismatchedrhs:
                    config = p4gf_config.read_repo(p4, view_name)
                    config[p4gf_config.SECTION_REPO]\
                          [p4gf_config.KEY_ENABLE_MISMATCHED_RHS] = str(True)
                    p4gf_config.write_repo_if(p4, p4.fetch_client(), view_name, config)

                # Populate the repo from Perforce unless --noclone.
                if not args.noclone:
                    return populate_repo(view_name, view_lock, args.start)
    except P4.P4Exception as e:
        _print_stderr(_('Error occurred: {}').format(e))

    return INIT_REPO_EXISTS

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)
