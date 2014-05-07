#! /usr/bin/env python3.3
"""Git Fusion package constants."""

import os

from   p4gf_l10n import _, NTR

# pylint:disable=C0301
# Yep, long lines, too annoying to fix...

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

P4GF_CLIENT_PREFIX                  = NTR("git-fusion-")
P4GF_REPO_CLIENT                    = NTR("git-fusion-{server_id}-{repo_name}")
P4GF_REPO_TEMP_CLIENT               = NTR("git-fusion-{server_id}-{repo_name}-temp-{n}")
P4GF_OBJECT_CLIENT_PREFIX           = NTR("git-fusion--")
P4GF_OBJECT_CLIENT_12_2             = NTR("git-fusion--{hostname}")
P4GF_OBJECT_CLIENT                  = NTR("git-fusion--{server_id}")
P4GF_GROUP                          = NTR("git-fusion-group")
P4GF_USER                           = NTR("git-fusion-user")
P4GF_UNKNOWN_USER                   = NTR("unknown_git")

P4GF_GROUP_VIEW_PULL                = NTR("git-fusion-{view}-pull")
P4GF_GROUP_VIEW_PUSH                = NTR("git-fusion-{view}-push")
P4GF_GROUP_PULL                     = NTR("git-fusion-pull")
P4GF_GROUP_PUSH                     = NTR("git-fusion-push")

                                        # Use hyphens, not underscores.
P4GF_COUNTER_INIT_STARTED           = NTR('git-fusion-init-started')
P4GF_COUNTER_INIT_COMPLETE          = NTR('git-fusion-init-complete')
P4GF_COUNTER_UPGRADE_STARTED        = NTR('git-fusion-upgrade-started')
P4GF_COUNTER_UPGRADE_COMPLETE       = NTR('git-fusion-upgrade-complete')
P4GF_COUNTER_PERMISSION_GROUP_DEFAULT \
                                    = NTR('git-fusion-permission-group-default')
P4GF_COUNTER_PUSH_STARTED           = NTR('git-fusion-{repo_name}-push-start')
P4GF_COUNTER_LAST_COPIED_CHANGE     = NTR('git-fusion-{repo_name}-{server_id}-last-copied-changelist-number')
P4GF_COUNTER_LOCK_PERM              = NTR('git-fusion-auth-server-lock')
P4GF_COUNTER_LOCK_VIEW              = NTR('git-fusion-view-{repo_name}-lock')
P4GF_COUNTER_LOCK_HOST_VIEW         = NTR('git-fusion-host-{server_id}-view-{repo_name}-lock')
P4GF_COUNTER_LOCK_HOST_VIEW_SHARED  = NTR('git-fusion-host-{server_id}-view-{repo_name}-shared')
P4GF_COUNTER_SERVER_ID              = NTR('git-fusion-server-id-')
P4GF_COUNTER_LOCK_HEARTBEAT         = NTR('{counter}-heartbeat')

P4GF_COUNTER_UPDATE_AUTH_KEYS       = NTR('git-fusion-auth-keys-last-changenum-{}')

# Needed to check for an un-upgraded 2012.2 install
P4GF_COUNTER_OLD_UPDATE_AUTH_KEYS 	= NTR('p4gf_auth_keys_last_changenum-{}')

P4GF_COUNTER_TIME_ZONE_NAME         = NTR('git-fusion-perforce-time-zone-name')

P4GF_COUNTER_PREVENT_NEW_SESSIONS   = NTR('git-fusion-prevent-new-sessions')
P4GF_COUNTER_LAST_COPIED_TAG        = NTR('git-fusion-{repo_name}-{server_id}-last-copied-tag')
P4GF_COUNTER_READ_PERMISSION_CHECK  = NTR('git-fusion-read-permission-check')
P4GF_COUNTER_DISABLE_ERROR_CLEANUP  = NTR('git-fusion-disable-error-cleanup')


P4GF_BRANCH_EMPTY_REPO              = NTR('p4gf_empty_repo')
P4GF_BRANCH_TEMP_N                  = NTR('git-fusion-temp-branch-{}')

# Environment vars
P4GF_AUTH_P4USER                    = NTR('P4GF_AUTH_P4USER')

# Override the host for use in 'protects -u xxx -h host'.
# It allows admins to require access via a proxy.
# Default is unset.
P4GF_PROTECTS_HOST                  = NTR('P4GF_PROTECTS_HOST')

# Internal debugging keys
# section in rc file for test vars
P4GF_TEST                           = NTR('test')

# Assign sequential UUID numbers so that test scripts
# get the same results every time.
P4GF_TEST_UUID_SEQUENTIAL           = NTR('p4gf_test_uuid_sequential')


# Internal testing environment variables.
# Read config from here, not /etc/git-fusion.log.conf
P4GF_TEST_LOG_CONFIG_PATH           = NTR('P4GF_LOG_CONFIG_FILE')

# Label/tag added to .gitmodules file to indicate a submodule that is
# managed by Git Fusion via the stream-imports-as-submodules feature.
P4GF_MODULE_TAG                     = NTR('p4gf')

# Filenames
P4GF_DIR                            = NTR('.git-fusion')
P4GF_RC_FILE                        = NTR('.git-fusion-rc')
P4GF_ID_FILE                        = NTR('server-id')
P4GF_TEMP_DIR_PREFIX                = NTR('p4gf_')
P4GF_MOTD_FILE                      = NTR('{P4GF_DIR}/motd.txt')
P4GF_FAILURE_LOG                    = NTR('{P4GF_DIR}/logs/{prefix}{date}.log.txt')
P4GF_SWARM_PRT                      = NTR('swarm-pre-receive-list')

# P4GF_HOME
P4GF_HOME = os.path.expanduser(os.path.join("~", P4GF_DIR))
P4GF_HOME_NAME                  = NTR('P4GF_HOME')

# In support of the P4GF_ENV configuration
P4GF_ENV                         = None               # set from the env var P4GF_ENV, if it exists
P4GF_ENV_NAME                    = NTR('P4GF_ENV')
GIT_BIN_DEFAULT                  = 'git'
GIT_BIN_NAME                     = 'GIT_BIN'
GIT_BIN                          = GIT_BIN_DEFAULT

# section definition here avoids circularity issues with p4gf_env_config and p4gf_config
SECTION_ENVIRONMENT       = NTR('environment')

# Perforce copies of Git commit and ls-tree objects live under this root.
P4GF_OBJECTS_ROOT                   = NTR('//{P4GF_DEPOT}/objects')

# Config files (stored in Perforce, not local filesystem)
P4GF_CONFIG_GLOBAL                  = NTR('//{P4GF_DEPOT}/p4gf_config')
P4GF_CONFIG_REPO                    = NTR('//{P4GF_DEPOT}/repos/{repo_name}/p4gf_config')
P4GF_CONFIG_REPO2                   = NTR('//{P4GF_DEPOT}/repos/{repo_name}/p4gf_config2')

P4GF_DEPOT_BRANCH_ROOT              = NTR("//{P4GF_DEPOT}/branches/{repo_name}/{branch_id}")

# branch-info files, separate from the versioned files that the branch stores.
# Nothing but branch-info files can be below this root.
P4GF_DEPOT_BRANCH_INFO_ROOT         = NTR("//{P4GF_DEPOT}/branch-info")

P4GF_CHANGELIST_DATA_FILE           = NTR('//{P4GF_DEPOT}/changelists/{repo_name}/{change_num}')


# Placed in change description when importing from Git to Perforce.
        ### We'll swap these two headers later. Will need to update test
        ### scripts to deal with new header.
P4GF_IMPORT_HEADER                  = NTR('Imported from Git')
P4GF_IMPORT_HEADER_OLD              = NTR('Git Fusion additional data:')
P4GF_DESC_KEY_AUTHOR                = NTR('Author')      # Do not change: required by git-fast-import
P4GF_DESC_KEY_COMMITTER             = NTR('Committer')   # Do not change: required by git-fast-import
P4GF_DESC_KEY_PUSHER                = NTR('Pusher')
P4GF_DESC_KEY_SHA1                  = NTR('sha1')
P4GF_DESC_KEY_PUSH_STATE            = NTR('push-state')
P4GF_DESC_KEY_DEPOT_BRANCH_ID       = NTR('depot-branch-id')
P4GF_DESC_KEY_CONTAINS_P4_EXTRA     = NTR('contains-p4-extra')
P4GF_DESC_KEY_GITLINK               = NTR('gitlink')
P4GF_DESC_KEY_PARENTS               = NTR('parents')
P4GF_DESC_KEY_PARENT_BRANCH         = NTR('parent-branch')
P4GF_DESC_KEY_GHOST_OF_SHA1         = NTR('ghost-of-sha1')
P4GF_DESC_KEY_GHOST_OF_CHANGE_NUM   = NTR('ghost-of-change-num')
P4GF_DESC_KEY_GHOST_PRECEDES_SHA1   = NTR('ghost-precedes-sha1')

# 'git clone' of these views (or pulling or fetching or pushing) runs special commands
P4GF_UNREPO_INFO                    = NTR('@info')           # Returns our version text
P4GF_UNREPO_LIST                    = NTR('@list')           # Returns list of repos visible to user
P4GF_UNREPO_HELP                    = NTR('@help')           # Returns contents of help.txt, if present
P4GF_UNREPO_MIRROR_WAIT             = NTR('@mirror_wait')    # Blocks until git mirror is caught up
P4GF_UNREPO_MIRROR_STATUS           = NTR('@mirror_status')  # Reports if mirror is busy copying or not
P4GF_UNREPO_FEATURES                = NTR('@features')       # Reports enabled state of features
P4GF_UNREPO = [
    P4GF_UNREPO_INFO,
    P4GF_UNREPO_LIST,
    P4GF_UNREPO_HELP,
    P4GF_UNREPO_MIRROR_WAIT,
    P4GF_UNREPO_MIRROR_STATUS,
    P4GF_UNREPO_FEATURES
    ]
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

P4GF_LOCKED_BY_MSG                  = _("Files in the push are locked by '{user}'")

NULL_COMMIT_SHA1                    = '0' * 40
EMPTY_TREE_SHA1                     = NTR('4b825dc642cb6eb9a060e54bf8d69288fbee4904')

# File added in rare case when Git commit's own ls-tree is empty
# and does not differ from first-parent (if any).
P4GF_EMPTY_CHANGELIST_PLACEHOLDER   = NTR('.p4gf_empty_changelist_placeholder')

NO_REPO_MSG_TEMPLATE = _(
'''
Git Fusion repo '{view_name}' does not exist.
{nop4client}To define a Git Fusion repo:
* create a Perforce client spec '{view_name_p4client}', or
* create a Git Fusion repo configuration file
  then check it into //P4GF_DEPOT/repos/{view_name}/p4gf_config, or
* ask your Git Fusion administrator to
  create a Git Fusion repo configuration file
  then specify it on the command line to p4gf_init_repo.py --config <file>,
  run on the Git Fusion server
''')

EMPTY_VIEWS_MSG_TEMPLATE = _(
'''
Git Fusion repo '{view_name}' cannot be created.
The views/exclusions for a branch/client allow no paths.
To define a Git Fusion repo:
* create a Perforce client spec '{view_name_p4client}', or
* create a Git Fusion repo configuration file
  then check it into //P4GF_DEPOT/repos/{view_name}/p4gf_config, or
* ask your Git Fusion administrator to
  create a Git Fusion repo configuration file
  then specify it on the command line to p4gf_init_repo.py --config <file>,
  run on the Git Fusion server
''')
# -- substitution-savvy functions ---------------------------------------------
def objects_root():
    '''Return //P4GF_DEPOT/objects'''
    return P4GF_OBJECTS_ROOT.format(P4GF_DEPOT=P4GF_DEPOT)

# Not officially tested or supported, but quite useful: import any environment
# variables starting P4GF_ as overrides to replace the above constants.
#
# h/t to Ravenbrook for the feature.
# https://github.com/Ravenbrook/perforce-git-fusion/commit/5cace4df621b91ba8b3b20059400af5a3e0837f2
#
# Commented out until we can find all the places in our test machinery that set
# P4GF_ environment variables that break the automated tests.
#
# import os
# locals().update({ key:value
#                   for key, value in os.environ.items()
#                   if key.startswith('P4GF_')})
