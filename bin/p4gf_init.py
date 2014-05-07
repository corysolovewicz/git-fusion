#! /usr/bin/env python3.3
'''Create the user and client that Git Fusion uses when communicating with Perforce.

Does NOT set up any git repos yet: see p4gf_init_repo.py for that.

Eventually there will be more options and error reporting. For now:
* current environment's P4PORT + P4USER is used to connect to Perforce.
* current P4USER must have enough privileges to create users, create clients.

Do not require super privileges for current P4USER or
git-fusion-user. Some customers reject that requirement.

'''

import os
import sys
import time
import re

import pygit2
import pytz

import P4
import p4gf_env_config    # pylint: disable=W0611
import p4gf_config
import p4gf_const
import p4gf_create_p4
import p4gf_group
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_log
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_proc
import p4gf_util
from   p4gf_verbosity import Verbosity
import p4gf_version

LOG = p4gf_log.for_module()
OLD_OBJECT_CLIENT = "git-fusion--p4"
OLDER_OBJECT_CLIENT = "git-fusion-p4"


def _write_file(p4, client_name, rootdir, relpath, file_content):
    """Write a template file to the local Git Fusion workspace and then
    add and submit to Perforce. NOP if file already exists in Perforce
    after a 'p4 sync'."""
    localpath = rootdir + relpath
    filename = os.path.basename(localpath)
    old_client = p4.client
    try:
        p4.client = client_name
        with p4.at_exception_level(p4.RAISE_NONE):
            # Sync the file and ensure we really have it.
            p4.run('sync', '-q', localpath)
            results = p4.run('have', localpath)
        if not results:
            LOG.debug("_write_file(): {} does not exist, will create...".format(localpath))
            # Perms are probably read-only, need to remove before writing.
            if os.path.exists(localpath):
                os.remove(localpath)
            else:
                localdir = os.path.dirname(localpath)
                if not os.path.exists(localdir):
                    os.makedirs(localdir)
            with open(localpath, 'w') as mf:
                mf.write(file_content)
            desc = _("Creating initial '{filename}' file via p4gf_init.py")   \
                   .format(filename=filename)
            with p4gf_util.NumberedChangelist(p4=p4, description=desc) as nc:
                nc.p4run(["add", localpath])
                nc.submit()
            LOG.debug("_write_file(): successfully created {}".format(localpath))
            _info(_("File '{}' created.").format(localpath))
        else:
            _info(_("File '{}' already exists.").format(localpath))
    except P4.P4Exception as e:
        LOG.warn('error setting up {file} file: {e}'
                 .format(file=filename, e=str(e)))
    finally:
        p4.client = old_client


def _write_user_map(p4, client_name, rootdir):
    """Writes the template user map file to the Git Fusion workspace and
    submits to Perforce, if such a file does not already exist.
    """
                        # pylint:disable=W9904
                        # double quotes OK: part of file format
    file_content =                                                               \
    _('# Git Fusion user map'                                                    \
    '\n# Format: Perforce-user [whitespace] Email-addr [whitespace] "Full-name"' \
    '\n#joe joe@example.com "Joe User"')
                        # pylint:enable=W9904

    _write_file(p4, client_name, rootdir, '/users/p4gf_usermap', file_content)


def _create_client(p4, client_name, p4gf_dir):
    """Create the host-specific Perforce client to enable working with
    the object cache in the P4GF_DEPOT depot.
    """
    # to prevent the mirrored git commit/tree objects from being retained in the
    # git-fusion workspace, set client option 'rmdir' and sync #none in p4gf_gitmirror
    # Assume the usual P4 client default options are being used so that
    # these options below differ ONLY with normdir -> rmdir
    # if this assumption proves troublesome, then read/write cycle will be needed.
    options = NTR('allwrite clobber nocompress unlocked nomodtime rmdir')

    view = ['//{depot}/... //{client}/...'.format(depot=p4gf_const.P4GF_DEPOT,
                                                  client=client_name)]
    spec_created = False
    if not p4gf_util.spec_exists(p4, 'client', client_name):
        # See if the old object clients exist, in which case we will remove them.
        for old_client_name in [OLD_OBJECT_CLIENT, OLDER_OBJECT_CLIENT]:
            if p4gf_util.spec_exists(p4, 'client', old_client_name):
                p4.run('client', '-df', old_client_name)
                _info(_("Old client '{}' deleted.").format(old_client_name))
        spec_created = p4gf_util.ensure_spec(
                            p4, 'client', spec_id=client_name,
                            values={'Host': None, 'Root': p4gf_dir,
                                    'Description': _('Created by Perforce Git Fusion'),
                                    'Options': options,
                                    'View': view})
    if spec_created:
        _info(_("Client '{}' created.").format(client_name))
    if not spec_created:
        modified = p4gf_util.ensure_spec_values(p4, 'client', client_name,
                {'Root': p4gf_dir, 'View': view, 'Options': options})
        if modified:
            _info(_("Client '{}' updated.").format(client_name))
        else:
            _info(_("Client '{}' already exists.").format(client_name))


def old_counter_present(p4):
    """If a proper upgrade from 2012.2 to 2013.1 is not done, an
    old counter will be present.  Raise an exception if it is.
    """
    old_counter = p4gf_const.P4GF_COUNTER_OLD_UPDATE_AUTH_KEYS.format('*')
    if p4.run('counters', '-u', '-e', old_counter):
        raise RuntimeError(_('error: Git Fusion 2012.2 artifacts detected.'
                             ' Upgrade required for use with 2013.1+.'
                             ' Please contact your administrator.'))
    return False


def _maybe_perform_init(p4, started_counter, complete_counter, func):
    """Check if initialization is required, and if so, kick off the
    initialization process. This is done by checking that both the
    started and completed counters are non-zero, in which case the
    initialization is not performed because it was (presumably) done
    already. If both counters are zero, initialization is performed.
    Otherwise, some amount of waiting and eventual lock stealing takes
    place such that initialization is ultimately completed.

    Arguments:
      p4 -- P4 API object
      started_counter -- name of init started counter
      complete_counter -- name of init completed counter
      func -- initialization function to be called, takes a P4 argument.
              Must be idempotent since it is possible initialization may
              be performed more than once.

    Returns True if initialization performed, False if already completed.
    """
    check_times = 0
    inited = False

    while True:
        r = p4.run('counter', '-u', started_counter)
        if r[0]['value'] == "0":
            # Initialization has not been started, try to do so now.
            r = p4.run('counter', '-u', '-i', started_counter)
            if r[0]['value'] == "1":
                # We got the lock, let's proceed with initialization.
                func(p4)
                # Set a counter so we will not repeat initialization later.
                p4.run('counter', '-u', '-i', complete_counter)
                inited = True
                break
        else:
            # Ensure that initialization has been completed.
            r = p4.run('counter', '-u', complete_counter)
            if r[0]['value'] == "0":
                check_times += 1
                if check_times > 5:
                    # Other process failed to finish perhaps.
                    # Steal the "lock" and do the init ourselves.
                    p4.run('counter', '-u', '-d', started_counter)
                    continue
            else:
                # Initialization has occurred already.
                break
        # Give the other process a chance before retrying.
        time.sleep(1)
    return inited


def _info(msg):
    '''
    CLI output
    '''
    Verbosity.report(Verbosity.INFO, msg)


def _global_init(p4):
    """Check that p4gf_super_init has been run and created the following
    * user git-fusion-user
    * depot //P4GF_DEPOT
    * group git-fusion-pull
    * group git-fusion-push
    * protects entries
    """

    #
    # The global initialization process below must be idempotent in the sense
    # that it is safe to perform more than once. As such, there are checks to
    # determine if work is needed or not, and if that work results in an
    # error, log and carry on with the rest of the steps, with the assumption
    # that a previous attempt had failed in the middle (or possibly that
    # another instance of Git Fusion has started at nearly the same time as
    # this one).
    #

    p4gf_util.has_server_id_or_exit()

    spec_list = {
        'user1':  p4gf_const.P4GF_USER,
        'user2':  p4gf_util.gf_reviews_user_name(),
        'user3':  p4gf_const.P4GF_REVIEWS__NON_GF,
        'group':  p4gf_const.P4GF_GROUP,
        'depot':  p4gf_const.P4GF_DEPOT
    }
    for spec_type, spec_id in spec_list.items():
        spec_type = re.sub(NTR(r'\d$'), '', spec_type)
        if not p4gf_util.spec_exists(p4, spec_type, spec_id):
        # pylint:disable=C0301
            raise RuntimeError(_("error: {spec_type} '{spec_id}' does not exist."
                                 " Please contact your administrator.")
                               .format(spec_type=spec_type, spec_id=spec_id))
        # pylint:enable=C0301

    for group in [p4gf_group.PERM_PULL, p4gf_group.PERM_PUSH]:
        c = p4gf_group.create_global_perm(p4, group)
        if c:
            _info(_("Global permission group '{}' created.").format(group))
        else:
            _info(_("Global permission group '{}' already exists.").format(group))

    c = p4gf_group.create_default_perm(p4)
    if c:
        _info(_("Default permission counter '{}' set to '{}'.")
              .format( p4gf_const.P4GF_COUNTER_PERMISSION_GROUP_DEFAULT
                     , p4gf_group.DEFAULT_PERM ))
    else:
        _info(_("Default permission counter '{}' already exists.")
              .format(p4gf_const.P4GF_COUNTER_PERMISSION_GROUP_DEFAULT))

    # Require that single git-fusion-user have admin privileges
    # over the //P4GF_DEPOT/ depot
    is_protects_empty = False
    try:
        p4.run('protects', '-u', p4gf_const.P4GF_USER, '-m',
               '//{depot}/...'.format(depot=p4gf_const.P4GF_DEPOT))
    except P4.P4Exception:
        # Why MsgDm_ReferClient here? Because p4d 11.1 returns
        # "must refer to client" instead of
        # "Protections table is empty" when given a depot path to
        # 'p4 protects -m -u'. Surprise!
        if p4gf_p4msg.find_all_msgid(p4, [ p4gf_p4msgid.MsgDm_ProtectsEmpty
                                         , p4gf_p4msgid.MsgDm_ReferClient  ]):
            is_protects_empty = True
        # All other errors are fatal, propagated.

    if is_protects_empty:
        # - order the lines in increasing permission
        # - end with at least one user (even a not-yet-created user) with super
        #     write user * * //...
        #     admin user git-fusion-user * //...
        #     super user super * //...
        p4gf_util.set_spec(p4, 'protect', values={
            'Protections': ["super user * * //...",
                            "super user {user} * //...".format(user=p4gf_const.P4GF_USER),
                            "admin user {user} * //{depot}/..."
                            .format(user=p4gf_const.P4GF_USER, depot=p4gf_const.P4GF_DEPOT)]})
        _info(_('Protects table set.'))


def _ensure_git_config_non_empty(key, value):
    """If Git lacks a global config value for a given key, set one.
    Returns value found (if any) or set (if none found).
    """
    # pygit2 does not like the config file to be missing
    fpath = os.path.expanduser(NTR('~/.gitconfig'))
    if not os.path.exists(fpath):
        with open(fpath, 'w') as f:
            f.write(_("# Git Fusion generated"))
    config = pygit2.Config.get_global_config()
    if key in config:
        return config[key]
    config[key] = value
    return value


def _upgrade_p4gf(p4):
    """Perform upgrade from earlier versions of P4GF. This should be invoked
    using _maybe_perform_init() to avoid race conditions across hosts.
    """
    # If updating from 12.2 to 13.1 we need to create global config file
    # (this does nothing if file already exists)
    c = p4gf_config.create_file_global(p4)
    if c:
        _info(_("Global config file '{}' created.")
              .format(p4gf_config.depot_path_global()))
    else:
        _info(_("Global config file '{}' already exists.")
              .format(p4gf_config.depot_path_global()))
    # Ensure the time zone name has been set, else default to something sensible.
    r = p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_TIME_ZONE_NAME)
    tzname = p4gf_util.first_value_for_key(r, 'value')
    if tzname == "0" or tzname is None:
        msg = _("Counter '{}' not set, using UTC as default."          \
                " Change this to your Perforce server's time zone.") \
              .format(p4gf_const.P4GF_COUNTER_TIME_ZONE_NAME)
        LOG.warn(msg)
        sys.stderr.write(NTR('Git Fusion: {}\n').format(msg))
        tzname = None
    else:
        # Sanity check the time zone name.
        try:
            pytz.timezone(tzname)
        except pytz.exceptions.UnknownTimeZoneError:
            LOG.warn("Time zone name '{}' unrecognized, using UTC as default".format(tzname))
            tzname = None
    if tzname is None:
        p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_TIME_ZONE_NAME, 'UTC')


def _delete_old_init_counters(p4, server_id):
    """
    Remove the old host-specific initialization counters, if any.
    """
    names = []
    names.append("git-fusion-{}-init-started".format(server_id))
    names.append("git-fusion-{}-init-complete".format(server_id))
    with p4.at_exception_level(p4.RAISE_NONE):
        for name in names:
            p4.run('counter', '-u', '-d', name)


def init(p4):
    """Ensure both global and host-specific initialization are completed.
    """
    started_counter = p4gf_const.P4GF_COUNTER_INIT_STARTED
    complete_counter = p4gf_const.P4GF_COUNTER_INIT_COMPLETE
    if not old_counter_present(p4):
        Verbosity.report(Verbosity.DEBUG, _('Old 2012.2 counter not present.'))
    if not _maybe_perform_init(p4, started_counter, complete_counter, _global_init):
        Verbosity.report(Verbosity.INFO, _('Permissions already initialized. Not changing.'))
    server_id = p4gf_util.get_server_id()
    started_counter = p4gf_const.P4GF_COUNTER_INIT_STARTED + '-' + server_id
    complete_counter = p4gf_const.P4GF_COUNTER_INIT_COMPLETE + '-' + server_id
    p4gf_dir = p4gf_const.P4GF_HOME
    client_name = p4gf_util.get_object_client_name()

    def client_init(p4):
        '''Perform host-specific initialization (and create sample usermap).'''
        # Set up the host-specific client.
        _create_client(p4, client_name, p4gf_dir)
        # Ensure the default user map and global config files are in place.
        _write_user_map(p4, client_name, p4gf_dir)
        p4gf_config.create_file_global(p4)
        _delete_old_init_counters(p4, server_id)

    if not _maybe_perform_init(p4, started_counter, complete_counter, client_init):
        Verbosity.report(Verbosity.INFO, _('Client and usermap already initialized. Not changing.'))

        # If client already created, make sure it hasn't been tweaked.
        ###: do we really need to handle this case? this is here just to pass the tests
        view = ['//{depot}/... //{client}/...'.format(depot=p4gf_const.P4GF_DEPOT,
                client=client_name)]
        p4gf_util.ensure_spec_values(p4, 'client', client_name,
                {'Root': p4gf_dir, 'View': view})

    # Perform any necessary upgrades within a "lock" to avoid race conditions.
    # For now, the lock is global, but could conceivably loosen to host-only.
    started_counter = p4gf_const.P4GF_COUNTER_UPGRADE_STARTED
    complete_counter = p4gf_const.P4GF_COUNTER_UPGRADE_COMPLETE
    if not _maybe_perform_init(p4, started_counter, complete_counter, _upgrade_p4gf):
        Verbosity.report(Verbosity.INFO, _('Global config file already initialized. Not changing.'))

    # Require non-empty Git config user.name and user.email.
    _ensure_git_config_non_empty('user.name',  _('Git Fusion Machinery'))
    _ensure_git_config_non_empty('user.email', _('nobody@example.com'))


def parse_argv():
    """Only version, help options for now"""
    parser = p4gf_util.create_arg_parser(_('Initializes a Git Fusion server.'))
    Verbosity.add_parse_opts(parser)
    args = parser.parse_args()
    Verbosity.parse_level(args)


def main():
    """create Perforce user and client for Git Fusion"""

    p4gf_version.log_version()
    try:
        log_l10n()
        p4gf_version.version_check()
    # pylint: disable=W0703
    # Catching too general exception
    except Exception as e:
        sys.stderr.write(e.args[0] + '\n')
        sys.exit(1)

    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4()
        if not p4:
            return 2

        Verbosity.report(Verbosity.INFO, "P4PORT : {}".format(p4.port))
        Verbosity.report(Verbosity.INFO, "P4USER : {}".format(p4.user))

        p4gf_util.reset_git_enviro()
        p4gf_proc.init()

        init(p4)

    return 0

if __name__ == "__main__":
    parse_argv()
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)
else:
    Verbosity.VERBOSE_LEVEL = Verbosity.QUIET
