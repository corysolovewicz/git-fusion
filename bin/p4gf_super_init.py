#! /usr/bin/env python3.3
"""
Perform Git Fusion initialization that requires "super" permissions.

* Create group git-fusion-group
* Create user  git-fusion-user
* Create depot P4GF_DEPOT
* Grant admin permission to git-fusion-user
* Configure dm.protects.allow.admin=1

Must be run with current P4USER set to a super user.
"""

import os
import io
import datetime
import getpass
import shutil
from   subprocess import Popen, PIPE
import sys
import textwrap
import configparser

import p4gf_const
import p4gf_create_p4
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_log
import p4gf_util
import p4gf_p4msg
import p4gf_p4msgid
from   p4gf_verbosity import Verbosity
import p4gf_version
from p4gf_missing_config_path import MissingConfigPath

P4D_VERSION_2014_1  = 2014.1
P4_TRIGGER_NAMES = \
    {'change-submit', 'change-content', 'change-commit', 'change-commit-p4gf-config'}
P4_TRIGGER_NAMES_14 = \
    {'change-content', 'change-commit', 'change-failed', 'change-commit-p4gf-config'}
P4_TRIGGER_FILE = 'p4gf_submit_trigger.py'
P4PORT = None
P4USER = None
p4     = None
P4_PASSWD = None
PROMPT_FOR_PASSWD = True
IGNORE_CASE_HANDLING = False
OVERRULE_SERVERID_CONFLICT = False

ID_FROM_ARGV    = None
SHOW_IDS        = False

KEY_PERM_MAX    = NTR('permMax')
KEY_PROTECTIONS = NTR('Protections')
KEY_VALUE       = NTR('Value')
KEY_TRIGGERS    = NTR('Triggers')

CONFIGURABLE_ALLOW_ADMIN = 'dm.protects.allow.admin'

Create_P4GF_CONFIG = False

def check_and_create_default_p4gf_env_config():
    '''If p4gf_env_config threw the MissingConfigPath exception,
    because P4GF_ENV names a non-existing filepath
    then save the required (two) default items
    into the user configured P4GF_ENV environment config file.
    '''
    if not Create_P4GF_CONFIG:
        return
    Verbosity.report(Verbosity.INFO,
            _("Git Fusion environment var P4GF_ENV = {0} names a non-existing file.")
            .format(p4gf_const.P4GF_ENV))
    Verbosity.report(Verbosity.INFO, _("Creating {0} with the default required items.")
            .format(p4gf_const.P4GF_ENV))
    Verbosity.report(Verbosity.INFO, _("Review the file's comments and edit as needed."))
    Verbosity.report(Verbosity.INFO, _("You may unset P4GF_ENV to use no config file.")
            .format(p4gf_const.P4GF_ENV))
    config = configparser.ConfigParser(interpolation  = None,
                                       allow_no_value = True)
    config.optionxform = str
    config.add_section(p4gf_const.SECTION_ENVIRONMENT)
    config.set(p4gf_const.SECTION_ENVIRONMENT, p4gf_const.P4GF_HOME_NAME, p4gf_const.P4GF_HOME)
    Verbosity.report(Verbosity.INFO, _("Setting {0} = {1} in {2}.")
            .format(p4gf_const.P4GF_HOME_NAME, p4gf_const.P4GF_HOME, p4gf_const.P4GF_ENV))
    config.set(p4gf_const.SECTION_ENVIRONMENT, NTR('P4PORT'), P4PORT)
    Verbosity.report(Verbosity.INFO, _("Setting {0} = {1} in {2}.")
        .format(NTR('P4PORT'), P4PORT, p4gf_const.P4GF_ENV))
    header = p4gf_util.read_bin_file(NTR('p4gf_env_config.txt'))
    if header is False:
        sys.stderr.write(_('no p4gf_env_config.txt found\n'))
        header = _('# Missing p4gf_env_config.txt file!')
    out = io.StringIO()
    out.write(header)
    config.write(out)
    file_content = out.getvalue()
    out.close()
    p4gf_util.ensure_dir(p4gf_util.parent_dir(p4gf_const.P4GF_ENV))
    with open(p4gf_const.P4GF_ENV, 'w') as f:
        f.write(file_content)

# p4gf_env_config will apply the config set by P4GF_ENV,
# but throw an exception if the P4GF_ENV is defined but the path does not exist.
# super_init will catch this exception and write the defaults to the P4GF_ENV path.
# pylint: disable=W0703
# Catching too general exception Exception
#
try:
    import p4gf_env_config    # pylint: disable=W0611
except MissingConfigPath:
    Create_P4GF_CONFIG = True
except Exception as exc:
    Verbosity.report(Verbosity.ERROR, str(exc))
    sys.exit(2)


# pylint: enable=W0703

def get_passwd(msg):
    '''Prompt for and confirm password.'''
    print("\n")
    if msg:
        print(msg)
    pw1 = NTR('pw1')
    pw2 = NTR('pw2')
    while pw1 != pw2:
        print(_("To cancel: CTL-C + ENTER."))
        pw1 = getpass.getpass(_('Password: '))
        if '\x03' in pw1:
            raise KeyboardInterrupt()
        pw2 = getpass.getpass(_('Retype password: '))
        if '\x03' in pw2:
            raise KeyboardInterrupt()
        if pw1 != pw2:
            print(_("Passwords do not match. Try again."))
    return pw1


def set_passwd(user, passwd):
    '''Set the P4 passwd for user. Assumes super user priviledge.'''
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('passwd', '-P', passwd, user)
        Verbosity.report(Verbosity.DEBUG, NTR('p4 passwd\n{}').format(r))
    if p4.errors:
        Verbosity.report(Verbosity.ERROR
                        , _("Unable to run 'p4 passwd -P xxx -u {0}'.").format(user))
        for e in p4.errors:
            Verbosity.report(Verbosity.ERROR, e)
        sys.exit(2)


def fetch_protect():
    """Return protect table as a list of protect lines."""

    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('protect','-o')
        Verbosity.report(Verbosity.DEBUG, NTR('p4 protect:\n{}').format(r))
    if p4.errors:
        Verbosity.report(Verbosity.ERROR, _("Unable to run 'p4 protect -o'."))
        for e in p4.errors:
            Verbosity.report(Verbosity.ERROR, e)
        sys.exit(2)

    protections = p4gf_util.first_value_for_key(r, KEY_PROTECTIONS)
    return protections


def fetch_triggers():
    """Return trigger table as a list of lines."""

    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('triggers','-o')
    if p4.errors:
        Verbosity.report(Verbosity.ERROR, _("Unable to run 'p4 triggers -o'."))
        for e in p4.errors:
            Verbosity.report(Verbosity.ERROR, e)
        sys.exit(2)

    triggers = p4gf_util.first_value_for_key(r, KEY_TRIGGERS)
    return triggers


def show_all_server_ids():
    '''List current Git Fusion server ids.'''

    server_ids = p4.run('counters', '-u', '-e', p4gf_const.P4GF_COUNTER_SERVER_ID + '*')
    ids = []
    this_server = p4gf_util.read_server_id_from_file()
    for eyed in server_ids:
        if 'counter' in eyed :
            id_ = eyed['counter'].replace(p4gf_const.P4GF_COUNTER_SERVER_ID,'')
            if this_server == id_:
                id_ = "* " + id_
            ids.append(( id_, eyed['value']))

    if ids:
        Verbosity.report(Verbosity.INFO, _("Git Fusion server IDs: {0}").format(ids))


def server_id_counter_exists(server_id):
    '''Return True if server_id_counter exists.'''
    if server_id:
        r = p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_SERVER_ID + server_id)
        if r[0]['value'] != "0":
            return r[0]['value']
    return False


def set_server_id_counter(server_id):
    '''Set the server_id_counter value to the hostname
    to identify GF hosts.
    '''
    if server_id:
        p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_SERVER_ID + server_id,
                p4gf_util.get_hostname())


def unset_server_id_counter(server_id):
    '''Delete the server_id_counter.'''
    if server_id:
        p4.run('counter', '-u', '-d', p4gf_const.P4GF_COUNTER_SERVER_ID + server_id)


def ensure_server_id():
    """Write this machine's permanent server-id assignment to
    P4GF_HOME/server-id .

    NOP if we already have a server-id stored in that file.
    We'll just keep using it.
    """
    id_from_file = p4gf_util.read_server_id_from_file()

    server_id = ID_FROM_ARGV if ID_FROM_ARGV else p4gf_util.get_hostname()
    # when re-running super_init, do not replace the server-id file when
    # the server_id file exists an no --id parameter is present
    # assume in the case that the existing file is correct
    if id_from_file and not ID_FROM_ARGV:
        server_id = id_from_file

    do_reset = True
    if server_id_counter_exists(server_id):
        do_reset = False
        if id_from_file == server_id:
            Verbosity.report(Verbosity.INFO,
                    _("Git Fusion server ID already set to '{0}'.").format(id_from_file))
        else:
            if not OVERRULE_SERVERID_CONFLICT:
                Verbosity.report(Verbosity.INFO,
                        _("Git Fusion server ID is already assigned: " \
                        "'{0}' set on host on '{1}'.\n" \
                        "Retry with a different --id server_id.") \
                        .format(server_id, server_id_counter_exists(server_id)))
                Verbosity.report(Verbosity.INFO,
                    _("If you are certain no other Git Fusion instance is using this server ID,"
                    "\nyou may overrule this conflict and set the local server-id file to"
                    "\n'{0}' with:"
                    "\n    p4gf_super_init.py --force").format(server_id))
                if id_from_file:
                    Verbosity.report(Verbosity.INFO,
                        _("Git Fusion server ID already set to '{0}'.").format(id_from_file))
                else:
                    Verbosity.report(Verbosity.INFO,
                        _("This Git Fusion's server ID is unset. Stopping."))
                    show_all_server_ids()
                    sys.exit(0)

            else:
                do_reset = True


    if do_reset:
        if id_from_file and id_from_file != server_id:  # delete the previous counter
            if server_id_counter_exists(id_from_file):
                unset_server_id_counter(id_from_file)
        set_server_id_counter(server_id)
        p4gf_util.write_server_id_to_file(server_id)
        Verbosity.report(Verbosity.INFO, _("Git Fusion server ID set to '{0}' in file '{1}'")
                         .format(server_id, p4gf_util.server_id_file_path()))
    show_all_server_ids()

def set_user_passwd_if_created(created, user):
    '''If creating the user, conditionally prompt for and set the passwd.'''
    global P4_PASSWD, PROMPT_FOR_PASSWD
    if created:
        if PROMPT_FOR_PASSWD:
            prompt_msg = _("Set one password for Perforce users 'git-fusion-user'"
                           "\nand 'git-fusion-reviews-*'.")
            # When creating additional Git Fusion instance only the new reviews will be created.
            # Catch this case and avoid a misleading prompt.
            if user == p4gf_util.gf_reviews_user_name():
                prompt_msg = _("Enter a new password for Perforce user '{0}'.").format(user)
            try:
                P4_PASSWD = get_passwd(prompt_msg)
            except KeyboardInterrupt:
                Verbosity.report(Verbosity.INFO,
                    _("\n Stopping. Passwords not set."))
                sys.exit(1)
            # If we prompted, do so once and use for all the service users,
            # even if the user enters no password at all.
            PROMPT_FOR_PASSWD = False
            if not P4_PASSWD:
                Verbosity.report(Verbosity.INFO,
                    _("Empty password. Not setting passwords."))

        # passwd may be suppressed with --nopasswd option, which also suppresses the prompt.
        if P4_PASSWD:
            set_passwd(user, P4_PASSWD)
            Verbosity.report(Verbosity.INFO,
                    _("Password set for Perforce user '{}'.").format(user))


def ensure_users():
    """Create Perforce user git-fusion-user, and reviews users if not already extant."""
    created = p4gf_util.ensure_user_gf(p4)
    log_info(created, p4gf_const.P4GF_USER)
    set_user_passwd_if_created(created, p4gf_const.P4GF_USER)

    created = p4gf_util.ensure_user_reviews(p4)
    log_info(created, p4gf_util.gf_reviews_user_name())
    set_user_passwd_if_created(created, p4gf_util.gf_reviews_user_name())

    created = p4gf_util.ensure_user_reviews_non_gf(p4)
    log_info(created, p4gf_const.P4GF_REVIEWS__NON_GF)
    set_user_passwd_if_created(created, p4gf_const.P4GF_REVIEWS__NON_GF)

    created = p4gf_util.ensure_user_reviews_all_gf(p4)
    log_info(created, p4gf_const.P4GF_REVIEWS__ALL_GF)
    set_user_passwd_if_created(created, p4gf_const.P4GF_REVIEWS__ALL_GF)

    # Report whether 'unknown_git' exists. Do not create.
    e = p4gf_util.service_user_exists(p4, p4gf_const.P4GF_UNKNOWN_USER)
    _exists = ( _("Git Fusion user '{0}' does not exist.")
              , _("Git Fusion user '{0}' exists."))
    Verbosity.report(Verbosity.INFO, _exists[e].format(p4gf_const.P4GF_UNKNOWN_USER))


def log_info(created, user):
    """Create Perforce user git-fusion-user if not already exists."""
    if created:
        Verbosity.report(Verbosity.INFO, _("User '{}' created.").format(user))
    else:
        Verbosity.report(Verbosity.INFO, _("User '{}' already exists. Not creating.")
                     .format(user))
    return created


def ensure_group():
    """Create Perforce group git-fusion-group if not already exists."""
    users = []
    # Keep the order of the users in the same order that P4 insists on
    # (if the order doesn't match then the group is updated repeatedly).
    users.append(p4gf_const.P4GF_REVIEWS__ALL_GF)
    users.append(p4gf_const.P4GF_REVIEWS__NON_GF)
    users.append(p4gf_util.gf_reviews_user_name())
    users.append(p4gf_const.P4GF_USER)
    args = [p4, NTR("group")]
    spec = {'Timeout': NTR('unlimited'), 'Users': users}
    kwargs = {'spec_id': p4gf_const.P4GF_GROUP, 'values': spec}
    if not p4gf_util.ensure_spec(*args, **kwargs):
        # We change the list of users in the group from time to time,
        # so ensure the membership is up to date.
        users = p4gf_util.first_dict(p4.run('group', '-o', p4gf_const.P4GF_GROUP))['Users']
        # Add the gf_reviews_user_name if not already in the group.
        # This avoids removing already existing reviews users from multiple GF instances.
        if not p4gf_util.gf_reviews_user_name() in users:
            users.append(p4gf_util.gf_reviews_user_name())
            spec = {'Timeout': NTR('unlimited'), 'Users': users}
            kwargs = {'spec_id': p4gf_const.P4GF_GROUP, 'values': spec}
            if p4gf_util.ensure_spec_values(*args, **kwargs):
                Verbosity.report(Verbosity.INFO
                                , _("Group '{}' updated.").format(p4gf_const.P4GF_GROUP))
            else:
                Verbosity.report(Verbosity.INFO, _("Group '{}' already up to date.")
                                 .format(p4gf_const.P4GF_GROUP))
        else:
            Verbosity.report(Verbosity.INFO, _("Group '{}' already up to date.")
                             .format(p4gf_const.P4GF_GROUP))
        return False
    else:
        Verbosity.report(Verbosity.INFO, _("Group '{}' created.").format(p4gf_const.P4GF_GROUP))
    return True


def ensure_depot():
    """Create depot P4GF_DEPOT if not already exists."""
    created = p4gf_util.ensure_depot_gf(p4)
    if created:
        Verbosity.report(Verbosity.INFO, _("Depot '{}' created.").format(p4gf_const.P4GF_DEPOT))
    else:
        Verbosity.report(Verbosity.INFO, _("Depot '{}' already exists. Not creating.")
                     .format(p4gf_const.P4GF_DEPOT))
    return created


def ensure_protect(protect_lines):
    """Require that 'p4 protect' table includes grant of admin to git-fusion-user.
    And review to git-fusion-reviews-*
    """
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('protects', '-m', '-u', p4gf_const.P4GF_USER)

    if p4gf_p4msg.find_msgid(p4, p4gf_p4msgid.MsgDm_ProtectsEmpty):
        Verbosity.report(Verbosity.INFO, _("Protect table empty. Setting...."))

    l = None
    Verbosity.report(Verbosity.DEBUG, NTR('p4 protects -mu git-fusion-user\n{}').format(r))
    perm = p4gf_util.first_value_for_key(r, KEY_PERM_MAX)
    if perm and perm in ['admin', 'super']:
        Verbosity.report(Verbosity.INFO,
                         _("Protect table already grants 'admin' to user '{}'. Not changing")
                         .format(p4gf_const.P4GF_USER))
    else:
        l = protect_lines
        l.append('admin user {user} * //...'.format(user=p4gf_const.P4GF_USER))

    review_perm = 'review user git-fusion-reviews-* * //...'
    if review_perm in protect_lines:
                        # Do not insert a newline into this line even
                        # though it is long. Makes it too hard to test
                        # in p4gf_super_init.t
        Verbosity.report(Verbosity.INFO,
            _("Protect table already grants 'review' to users 'git-fusion-reviews-*'."
              "Not changing"))
    else:
        if not l:
            l = protect_lines
        l.append(review_perm)

    if l:
        p4gf_util.set_spec(p4, 'protect', values={KEY_PROTECTIONS : l})
        Verbosity.report(Verbosity.INFO,
                _("Protect table modified. User '{}' granted admin permission.")
                .format(p4gf_const.P4GF_USER))


def ensure_protects_configurable():
    """Grant 'p4 protects -u' permission to admin users."""
    v = p4gf_util.first_value_for_key(
            p4.run('configure', 'show', CONFIGURABLE_ALLOW_ADMIN),
            KEY_VALUE)
    if v == '1':
        Verbosity.report(Verbosity.INFO, _("Configurable '{}' already set to 1. Not setting.")
                     .format(CONFIGURABLE_ALLOW_ADMIN))
        return False

    p4.run('configure', 'set', '{}=1'.format(CONFIGURABLE_ALLOW_ADMIN))
    Verbosity.report(Verbosity.INFO, _("Configurable '{}' set to 1.")
                 .format(CONFIGURABLE_ALLOW_ADMIN))
    return True


def initialize_all_gf_reviews():
    """Execute p4gf_submit_trigger.py as super user to reset the
       git-fusion-reviews--all-gf reviews used by the submit trigger"""
    trigger_path = "{0}/{1}".format(os.path.dirname(os.path.abspath(sys.argv[0])),
        P4_TRIGGER_FILE)
    if not os.path.exists(trigger_path):
        print(_("Unable to find and execute '{0}'").format(trigger_path))
        return

    if P4USER == p4gf_const.P4GF_USER:
        # HACK: p4gf_submit_trigger.py script is hard-coded to ignore most
        # commands run by the git-fusion-user user, so issue a warning.
        print(_("Unable to rebuild the P4GF reviews as user '{}'.").format(P4USER))
        print(_("Please rebuild the P4GF reviews as another user."))
        return

    try:
        cmd = [trigger_path, '--rebuild-all-gf-reviews', P4PORT, P4USER ]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        fd = p.communicate()
        # pylint: disable=E1101
        # Instance of '' has no '' member
        if p.returncode:
            print(_("Error '{ec}' returned from command '{cmd}'")
                   .format(cmd=' '.join(cmd),
                           ec=p.returncode))
            print("{0} {1}".format(str(fd[0]), str(fd[1])))
            return
    # pylint: disable=W0703
    # Catching too general exception
    except Exception:
        print(_("Error rebuilding all GF reviews, unable to locate and/or run '{0}'").
                format(trigger_path))
        return

    if len(fd[0]):
        Verbosity.report(Verbosity.INFO,
            _("Re-setting 'git-fusion-reviews--all-gf' with {0:d} repo views").
            format(len(fd[0].splitlines())))


def check_triggers():
    """ Check all of the GF triggers are installed and the
    trigger version is correct.
    """
    # pylint: disable=R0912
    # Too many branches
    triggers = fetch_triggers()
    if not triggers:
        Verbosity.report(Verbosity.INFO,
            'Git Fusion Triggers are not installed.')
        return

    gf_triggers = set()
    for trig in triggers:
        words = trig.split()
        if P4_TRIGGER_FILE in trig:
            gf_triggers.add(words[5])

    have_all_triggers = 0
    if p4gf_version.p4d_version(p4) <  P4D_VERSION_2014_1:
        trigger_names = P4_TRIGGER_NAMES
    else:
        trigger_names = P4_TRIGGER_NAMES_14

    for trig in trigger_names:
        if trig in gf_triggers:
            have_all_triggers += 1

    if have_all_triggers == 0:
        Verbosity.report(Verbosity.INFO,
            'Git Fusion Triggers are not installed.')
    elif have_all_triggers < 4:
        Verbosity.report(Verbosity.INFO,
            'Git Fusion Triggers are not all installed.')
    else:   # check counter
        counter = p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_PRE_TRIGGER_VERSION)[0]
        version = counter['value']
        if version != '0':
            version = version.split(":")[0].strip()
        version = int(version)
        if version and version != int(p4gf_const.P4GF_TRIGGER_VERSION):
            Verbosity.report(Verbosity.INFO,
                'Git Fusion Triggers are not up to date.')
        elif not version:
            # set the version counter since we detected
            # that all the triggers are installed
            _version = "{0} : {1}".format(p4gf_const.P4GF_TRIGGER_VERSION, datetime.datetime.now())
            p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_PRE_TRIGGER_VERSION, _version)
            p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_POST_TRIGGER_VERSION, _version)
            Verbosity.report(Verbosity.INFO,
                _("Setting '{0}' = '{1}'").format(
                p4gf_const.P4GF_COUNTER_PRE_TRIGGER_VERSION, _version))
            Verbosity.report(Verbosity.INFO,
                _("Setting '{0}' = '{1}'").format(
                p4gf_const.P4GF_COUNTER_POST_TRIGGER_VERSION, _version))
        else:
            Verbosity.report(Verbosity.INFO,
                _('Git Fusion triggers are up to date.'))


def _ensure_case_sensitive():
    """
    Ensure that the Perforce server is case sensitive, but only if we have
    not been instructed to ignore the issue completely, in which case it is
    assumed the administrator knows what they are doing.
    """
    if not IGNORE_CASE_HANDLING:
        info = p4gf_util.first_dict(p4.run('info'))
        if not info.get('caseHandling') == 'sensitive':
            # Yes, the formatting is weird, but otherwise dedent and fill
            # will not yield the desired results (see job job070463).
            msg = _("""\
            The Perforce service's case-handling policy is not set to 'sensitive',
            which means any files introduced via Git whose names differ only by case may
            result in data loss or errors during push. It is strongly advised to set the
            case-handling policy to 'sensitive'. To bypass this check, pass --ignore-case
            when invoking this script.
            """)
            dims = shutil.get_terminal_size()
            msg = textwrap.fill(textwrap.dedent(msg), dims[0])
            Verbosity.report(Verbosity.ERROR, msg)
            sys.exit(1)


def main():
    """Do the thing."""
    try:
        log_l10n()
        parse_argv()
        global P4PORT, P4USER
        needs_exit = False
        if not P4PORT and "P4PORT" not in os.environ:
            Verbosity.report(Verbosity.INFO,
                'P4PORT is neither set in the environment nor passed as an option.')
            needs_exit = True
        if not P4USER and "P4USER" not in os.environ:
            Verbosity.report(Verbosity.INFO,
                'P4USER is neither set in the environment nor passed as an option.')
            needs_exit = True
        # Check that a pre-existing P4GF_ENV config file P4PORT conflicts with the --port option
        if p4gf_const.P4GF_ENV and not Create_P4GF_CONFIG and P4PORT:
            if P4PORT != os.environ['P4PORT']:
                Verbosity.report(Verbosity.INFO,
                        "conflicting P4PORT in args: {0} and P4GF_ENV {1} : P4PORT = {2}. Stopping."
                        .format(P4PORT, p4gf_const.P4GF_ENV, os.environ['P4PORT']))
                needs_exit = True
            else:
                Verbosity.report(Verbosity.INFO,
                    "P4PORT argument is identically configured in {0}. Proceeding.".format(
                        p4gf_const.P4GF_ENV ))
        if needs_exit:
            sys.exit(1)

        p4gf_version.version_check()
        # Connect.
        global p4
        if not P4USER:
            P4USER = os.environ['P4USER']
        p4 = p4gf_create_p4.create_p4(port=P4PORT, user=P4USER)
        if not p4:
            raise RuntimeError(_("Failed to connect to P4."))
        P4PORT = p4.port
        P4USER = p4.user
        check_and_create_default_p4gf_env_config()
        if SHOW_IDS:
            show_all_server_ids()
            sys.exit(0)
        Verbosity.report(Verbosity.INFO, "P4PORT : {}".format(p4.port))
        Verbosity.report(Verbosity.INFO, "P4USER : {}".format(p4.user))
        _ensure_case_sensitive()

        # Require that we have super permission.
        # Might as well keep the result in case we need to write a new protect
        # table later. Saves a 'p4 protect -o' trip to the server
        protect_lines = fetch_protect()

        ensure_server_id()
        ensure_users()
        ensure_group()
        ensure_depot()
        ensure_protect(protect_lines)
        ensure_protects_configurable()
        check_triggers()
        initialize_all_gf_reviews()

    # pylint: disable=W0703
    # Catching too general exception
    except Exception as e:
        sys.stderr.write(str(e) + '\n')
        p4gf_create_p4.close_all()
        sys.exit(1)


# pylint:disable=C0301
# line too long? Too bad. Keep tabular code tabular.
def parse_argv():
    """Copy optional port/user args into global P4PORT/P4USER."""

    parser = p4gf_util.create_arg_parser(
    _("Creates Git Fusion user, depot, and protect entries."))
    parser.add_argument('--port',    '-p', metavar='P4PORT', nargs=1, help=_('P4PORT of server'))
    parser.add_argument('--user',    '-u', metavar='P4USER', nargs=1, help=_('P4USER of user with super permissions.'))
    Verbosity.add_parse_opts(parser)
    parser.add_argument('--id',                              nargs=1, help=_("Set this Git Fusion server's unique id"))
    parser.add_argument('--showids',            action='store_true',  help=_('Display all Git Fusion server ids'))
    parser.add_argument('--ignore-case',        action='store_true',  help=_('Do not check for case-handling policy in server.'))
    parser.add_argument('--force', action='store_true', help=_("Force set local server-id file when server-id already registered in Git Fusion."))
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--passwd',                              nargs=1, help=_("Password for 'git-fusion-user' and 'git-fusion-reviews-*'"))
    group.add_argument('--no-passwd',           action='store_true', help=_("Do not prompt for nor set password for 'git-fusion-user' and 'git-fusion-reviews-*'"))
    args = parser.parse_args()

    Verbosity.parse_level(args)

    # Optional args, None if left unset
    global P4PORT, P4USER, ID_FROM_ARGV, SHOW_IDS, P4_PASSWD, PROMPT_FOR_PASSWD
    global IGNORE_CASE_HANDLING, OVERRULE_SERVERID_CONFLICT
    if args.port:
        P4PORT = args.port[0]
    if args.user:
        P4USER = args.user[0]
    if args.id:
        ID_FROM_ARGV = args.id[0]
    if args.showids:
        SHOW_IDS = True
    if args.passwd:
        P4_PASSWD = args.passwd[0]
        PROMPT_FOR_PASSWD = False
    elif args.no_passwd:
        PROMPT_FOR_PASSWD = False
    if args.ignore_case:
        IGNORE_CASE_HANDLING = True
    if args.force:
        OVERRULE_SERVERID_CONFLICT = True

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)
