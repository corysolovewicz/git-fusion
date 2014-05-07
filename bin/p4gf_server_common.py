#! /usr/bin/env python3.3
"""
Common functions for all P4GF server implementations.
"""

import os
import shutil
import sys
import time
import traceback

import p4gf_config
import p4gf_const
import p4gf_create_p4
import p4gf_gitmirror
import p4gf_group
from   p4gf_l10n   import _
import p4gf_lock
import p4gf_log
import p4gf_p4msg
from p4gf_repolist import RepoList
import p4gf_translate
import p4gf_util
import p4gf_version
import p4gf_read_permission

LOG = p4gf_log.for_module()

COMMAND_TO_PERM = {'git-upload-pack': p4gf_group.PERM_PULL,
                   'git-receive-pack': p4gf_group.PERM_PUSH}


class CommandError(RuntimeError):
    """
    An error that ExceptionAuditLogger recognizes as one that
    requires a report of exception, but not of stack trace.
    """
    def __init__(self, val, usage=None):
        self.usage = usage # Printed to stdout if set
        RuntimeError.__init__(self, val)


class ExceptionAuditLogger:
    """Write all exceptions to audit log, then propagate."""

    def __init__(self):
        pass

    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_value, _traceback):
        # Skip calls to exit().
        if exc_type == SystemExit:
            return False

        if exc_type == CommandError:
            record_reject("{}".format(exc_value))
            if exc_value.usage:
                print(exc_value.usage)
            return False

        if exc_type:
            str_list = traceback.format_exception(exc_type, exc_value, _traceback)
            s = "".join(str_list)
            record_reject("{}".format(exc_value), s)

        return False # False = do not squelch. Propagate


def check_protects(p4):
    """Check that the protects table is either empty or that the Git
    Fusion user is granted sufficient privileges. Returns False if this
    is not the case.
    """
    return p4gf_version.p4d_supports_protects(p4)


def raise_p4gf_perm():
    '''
    User-visible permission failure.
    '''
    raise CommandError(_('git-fusion-user not granted sufficient privileges.'))


def check_lock_perm(p4):
    '''
    Permission check: can git-fusion-user set our lock counter? If not, you
    know what to do.
    '''
    with p4gf_group.PermErrorOK(p4):
        with p4gf_lock.CounterLock(p4, p4gf_const.P4GF_COUNTER_LOCK_PERM):
            pass
    if p4gf_p4msg.contains_protect_error(p4):
        raise_p4gf_perm()


def check_readiness(p4):
    """
    Check that P4GF is ready for accepting connections from clients.
    """
    # Note the "clever" use of counter names with a shared prefix that
    # just happen to be the two counters we are interested in retrieving.
    # (wanted to avoid another call to p4 counter, but without retrieving
    # _all_ P4GF counters, which could be millions).
    fetch_counters = lambda p4: p4.run('counters', '-u', '-e', 'git-fusion-pre*')
    if p4.connected():
        counters = fetch_counters(p4)
    else:
        with p4gf_create_p4.p4_connect(p4):
            counters = fetch_counters(p4)

    # Check if the "prevent further access" counter has been set, and raise an
    # error if the counter is anything other than zero.
    value = fetch_counter_value(counters, p4gf_const.P4GF_COUNTER_PREVENT_NEW_SESSIONS)
    if value and value != '0':
        raise RuntimeError(_('Git Fusion is shutting down. Please contact your admin.'))

    # Check that GF submit trigger is installed and has a compatible version.
    value = fetch_counter_value(counters, p4gf_const.P4GF_COUNTER_PRE_TRIGGER_VERSION)
    trigger_version_counter = value.split(":")[0].strip() if value else '0'
    if int(trigger_version_counter) != int(p4gf_const.P4GF_TRIGGER_VERSION):
        LOG.error("Incompatible trigger version: {0} should be {1} but got {2}".format(
            p4gf_const.P4GF_COUNTER_PRE_TRIGGER_VERSION,
            p4gf_const.P4GF_TRIGGER_VERSION, trigger_version_counter))
        if int(trigger_version_counter) == 0:
            raise RuntimeError(_('Git Fusion submit triggers are not installed.'
                                 ' Please contact your admin.'))
        else:
            raise RuntimeError(_('Git Fusion submit triggers need updating.'
                                 ' Please contact your admin.'))
    p4gf_util.has_server_id_or_exit(log=LOG)

def user_has_read_permissions(p4, required_perm, view_perm):
    '''
    If this is a pull - check read permissions if enabled.
    '''
    if required_perm != p4gf_group.PERM_PULL:
        return True
    # query the global config for read_permission check
    global_config = p4gf_config.get_global(p4)
    read_perm_check =  global_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
            p4gf_config.KEY_READ_PERMISSION_CHECK, fallback='None')
    if read_perm_check.lower() != 'user': # no user perms enabled? then return True - no check
        return True

    # perform the user read permissions check
    read_permission = p4gf_read_permission.ReadPermission(p4, view_perm)
    read_permission.read_permission_check_for_repo()
    return view_perm.user_perm_view_pull


def check_authorization(p4, view_perm, user, command, view_name):
    '''
    Does view_perm grant permission to run command? If not, raise an exception.
    '''
    required_perm = COMMAND_TO_PERM[command]
    if view_perm.can(required_perm):  # check group permissions
        if required_perm == p4gf_group.PERM_PULL:
            # if group grants permissions - then check for user read perms
            if user_has_read_permissions(p4, required_perm, view_perm):
                return
        else:  # PERM_PUSH
            return
    msg = _("User '{user}' not authorized for '{command}' on '{view}'.") \
          .format(user=user, command=command, view=view_name)
    # if user permissions prevent the pull provide verbose message.
    if view_perm.user_read_permission_checked and view_perm.error_msg:
        msg += view_perm.error_msg

    raise CommandError(msg)


def fetch_counter_value(counters, name):
    """
    Scan the given results looking for a counter whose name matches the
    query, returning the value of 'value', or None if not found.
    """
    ### Someday move this to a new p4gf_counters module, with other cool functions
    for result in counters:
        if isinstance(result, dict) and result['counter'] == name:
            return result['value']
    return None


#pylint:disable=R0912
def run_special_command(view, p4, user):
    """If view is a special command run it and return True; otherwise return False"""

    # @help: dump contents of help.txt, if file exists
    if p4gf_const.P4GF_UNREPO_HELP == view:
        help_text = p4gf_util.read_bin_file('help.txt')
        if help_text == False:
            sys.stderr.write(_("file 'help.txt' not found\n"))
        else:
            sys.stderr.write(help_text)
        return True

    # @info: dump info to stderr
    if p4gf_const.P4GF_UNREPO_INFO == view:
        sys.stderr.write(p4gf_version.as_string())
        sys.stderr.write(_('Server address: {}\n').format(p4.port))
        return True

    # @list: dump list of repos to stderr
    if p4gf_const.P4GF_UNREPO_LIST == view:
        repos = RepoList.list_for_user(p4, user).repos
        if len(repos):
            width = max(len(r[0]) for r in repos)
            sys.stderr.write("\n".join(["{name:<{width}} {perm} {charset:<10} {desc}".format(
                width=width, name=p4gf_translate.TranslateReponame.repo_to_git(r[0]),
                perm=r[1], charset=r[2], desc=r[3])
                                        for r in repos]) + "\n")
        else:
            sys.stderr.write(_('no repositories found\n'))
        return True

    if p4gf_const.P4GF_UNREPO_MIRROR_WAIT == view:
        while p4gf_gitmirror.copying_trees():
            sys.stderr.write(_('waiting for mirror..\n'))
            sys.stderr.flush()
            time.sleep(1)
        sys.stderr.write(_('mirror up-to-date\n'))
        return True

    if p4gf_const.P4GF_UNREPO_MIRROR_STATUS == view:
        if p4gf_gitmirror.copying_trees():
            sys.stderr.write(_('mirroring trees\n'))
        else:
            sys.stderr.write(_('mirror up-to-date\n'))
        return True

        # clone @features just gives list of all available features
    if p4gf_const.P4GF_UNREPO_FEATURES == view:
        sys.stderr.write(_('Available features:\n'))
        for k in p4gf_config.configurable_features():
            sys.stderr.write("{} : {}\n".format(k, p4gf_config.FEATURE_KEYS[k]))
        return True

    # clone @features@repo tells which features are enabled for a repo
    if view.startswith(p4gf_const.P4GF_UNREPO_FEATURES+'@'):
        view = view[len(p4gf_const.P4GF_UNREPO_FEATURES)+1:]
        sys.stderr.write(_("Enabled features for repo '{}':\n").format(view))
        config = p4gf_config.get_repo(p4, view)
        for k in p4gf_config.configurable_features():
            sys.stderr.write("{} : {}\n"
                .format(k, p4gf_config.is_feature_enabled(config, k)))
        return True

    # If no previous match and view starts with '@'
    # then return list of special commands to client - no error
    if view.startswith('@'):
        special_cmds = ' '.join(p4gf_const.P4GF_UNREPO)
        sys.stderr.write(
                _('Git Fusion: unrecognized special command.\nValid commands are: {}') + '\n'.
                format(special_cmds))
        return True

    return False
#pylint:enable=R0912


def record_reject(msg, _traceback=None):
    """
    Write line to both auth audit log and stderr. Will append a
    newline when writing to standard error.

    Separate stack traces to a second optional parameter, so that
    we can still dump those to audit log, but NEVER to stderr, which the
    git user sometimes sees.
    """
    p4gf_log.record_error(msg)
    if _traceback:
        p4gf_log.record_error(_traceback)
    sys.stderr.write(msg + '\n')


def cleanup_client(ctx, view_name):
    """Clean up the failed client and workspace after an error occurs while
    creating the initial clone. If the client does not exist, nothing is done.
    """
    client_name = p4gf_util.view_to_client_name(view_name)
    if not p4gf_util.spec_exists(ctx.p4, 'client', client_name):
        return

    LOG.debug('cleaning up failed view {}'.format(view_name))
    command_path = ctx.client_view_path()

    ctx.p4.run('sync', '-fq', command_path + '#none')
    ctx.p4.run('client', '-df', client_name)
    for vdir in [ctx.view_dirs.view_container]:
        LOG.debug('removing view directory {}'.format(vdir))
        if os.path.isdir(vdir):
            shutil.rmtree(vdir)
        elif os.path.isfile(vdir):
            os.remove(vdir)
