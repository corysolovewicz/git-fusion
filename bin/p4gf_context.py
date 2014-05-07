#! /usr/bin/env python3.3
"""Config and Context classes"""
from   collections import deque
import copy
import logging
import os
import pygit2
import tempfile
import time
import traceback

from P4 import Map

from   p4gf_client_pool import ClientPool
from   p4gf_create_p4 import create_p4, p4_connect, p4_disconnect
import p4gf_branch
from   p4gf_branch_files_cache      import BranchFilesCache
import p4gf_config
import p4gf_const
import p4gf_depot_branch
from   p4gf_g2p_preflight_hook      import PreflightHook
from   p4gf_l10n import _, NTR
import p4gf_lock
import p4gf_log
import p4gf_path
import p4gf_path_convert
import p4gf_protect
from   p4gf_temp_p4branch_mapping   import TempP4BranchMapping
import p4gf_util
import p4gf_view_dirs

LOG = logging.getLogger(__name__)

# pylint:disable=R0904
# Too many public methods
# Context is our kitchen sink class, can have as many public data and
# function members as it wants.

# Rate for logging memory usage, in seconds
MEMLOG_HEART_RATE = 5
# How often to ask the P4D server if someone else canceled our heartbeat.
_CANCEL_POLL_PERIOD_SECS = 5.0
# Flag for dealing with differences between Linux and Darwin resources.
IsDarwin = os.uname()[0] == "Darwin"


def client_path_to_local(clientpath, clientname, localrootdir):
    """ return client syntax path converted to local syntax"""
    return localrootdir + clientpath[2 + len(clientname):]


def strip_wild(path):
    """ strip trailing ... from a path

    ... must be present; no check is made
    """
    return path[:len(path) - 3]


def to_lines(x):
    """If x is a single string, perhaps of multiple lines, convert to a list
    of lines.
    """
    if isinstance(x, str):
        return x.splitlines()
    elif x is None:
        return x
    elif not isinstance(x, list):
        return [x]
    else:
        return x


def check_client_view_gf(lhs):
    """ check that the client view fits our strict requirements

    The client used for P4GF data must contain exactly one line in its view,
    mapping from //P4GF_DEPOT/... to //client/somewhere/...

    Multiple wildcards, special characters, any other wacky stuff forbidden.
    """
    if len(lhs) != 1 or lhs[0] != "//{0}/...".format(p4gf_const.P4GF_DEPOT):
        bad_client_view_gf(lhs, _('view not equal to: //{}/...')
                                .format(p4gf_const.P4GF_DEPOT))


def bad_client_view_gf(lhs, why):
    """all purpose exception for invalid p4gf client view"""
    raise RuntimeError(
        _('P4GFContext: Invalid client view for //.git-fusion.'
          '\n{why}\n{lhs}').format(lhs=lhs, why=why))


def calc_last_copied_change_counter_name(view_name, server_id):
    '''
    Return a counter that holds the highest changelist number
    copied to the given repo, on the given Git Fusion server.
    '''
    return p4gf_const.P4GF_COUNTER_LAST_COPIED_CHANGE.format(
                  repo_name = view_name
                , server_id = server_id)


def create_context(view_name, view_lock):
    """Return a Context object that contains the connection details for use
    in communicating with the Perforce server."""
    cfg = Config()
    cfg.p4user = p4gf_const.P4GF_USER
    cfg.p4client = p4gf_util.view_to_client_name(view_name)
    cfg.p4client_gf = p4gf_util.get_object_client_name()
    cfg.view_name = view_name
    ctx = Context(cfg)
    ctx.view_lock = view_lock  # None OK: can run without a lock.
    return ctx


def client_spec_to_root(client_spec):
    '''Return client root, minus any trailing /'''
    root_dir = p4gf_path.strip_trailing_delimiter(client_spec["Root"])
    return root_dir


class Config:
    """perforce config"""

    def __init__(self):
        self.p4port = None
        self.p4user = None
        self.p4client = None     # client for view
        self.p4client_gf = None  # client for gf
        self.view_name = None    # git project name


# pylint: disable=R0902
class Context:
    """a single git-fusion view/repo context"""

    # pylint:disable=R0915
    # too many statements
    def __init__(self, config):
        self.config = config

        # connected by default:
        self.p4                     = None
        self.p4gf                   = None

        # not connected by default:
        self.p4gf_reviews           = None
        self.p4gf_reviews_non_gf    = None

        try:
            from   p4gf_gitmirror import GitMirror
            self.mirror = GitMirror(config.view_name)
        except:
            LOG.error("failed to create GitMirror:\n{}".format(traceback.format_exc()))
            raise

        self.timezone               = None
        self.server_version         = None
        self._user_to_protect       = None
        self.view_dirs              = None
        self.view_lock              = None
        self.view_repo              = None
        self.tempdir                = tempfile.TemporaryDirectory(
                                        prefix=p4gf_const.P4GF_TEMP_DIR_PREFIX)

        # RAII object to operate on a numbered changelist with p4run and p4gfrun
        # set in p4gf_util by NumberedChangelist
        self.numbered_change        = None
        self.numbered_change_gf     = None

        # Environment variable set by p4gf_auth_server.py.
        self.authenticated_p4user   = os.environ.get(p4gf_const.P4GF_AUTH_P4USER)

        # gf_branch_name ==> p4gf_branch.Branch
        # Lazy-loaded by branch_dict()
        self._branch_dict           = None
        self.branch_creation        = None
        self.merge_commits          = None
        self.submodules             = None
        self.owner_is_author        = None

        # DepotBranchInfoIndex of all known depot branches that house
        # files from lightweight branches, even ones we don't own.
        # Lazy-loaded by depot_branch_info_index()
        self._depot_branch_info_index = None

        # paths set up by set_up_paths()
        self.gitdepotroot           = "//{}/".format(p4gf_const.P4GF_DEPOT)
        self.gitlocalroot           = None
        self.client_spec_gf         = None
        self.gitrootdir             = None
        self.contentlocalroot       = None
        self.contentclientroot      = None
        self.clientmap              = None
        self.clientmap_gf           = None
        self.client_exclusions_added = False

        # Avoid unnecessary view switches.
        self.last_view_lines        = None
        self.last_client_spec       = None

        # Seconds since the epoch when we last polled for remote kill switch.
        self._last_heartbeat_cancel_check_time = None
        self._heartbeat_time        = None

            # A set of temporary Perforce client specs, each mapped to one
            # branch's view. Use these to query Perforce rather than switching
            # ctx.p4 back and forth just to run 'p4 files //client/...' for some
            # random branch other than our current branch.
        self._client_pool           = None
            # By default, temp clients in pool will be deleted when Context exits.
            # For 'nested' Contexts, set this to False to avoid deleting clients
            # out from under the surrounding Context.
        self.cleanup_client_pool    = True

        # A single, shared temporary Perforce branch, useful for integrations.
        self._temp_branch           = None

            # Minimize the number of 'p4 files //branch-client/...@n' calls.
        self.branch_files_cache     = BranchFilesCache()

            # Set by G2PMatrix during a 'git push' to remember the most recent
            # changelist integrated from each branch to each other branch.
            # Instance of IntegratedUpTo
        self.integrated_up_to       = None

            # Last N p4run() commands. Reported in _dump_on_failure()
        self.p4run_history          = deque(maxlen=20)

            # Last N p4run() commands. Reported in _dump_on_failure()
        self.p4gfrun_history        = deque(maxlen=20)

            # Admin-configured option to reject unworthy commits.
        self._preflight_hook        = None
    # pylint:enable=R0915

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.disconnect()
        if self.tempdir:
            self.tempdir.cleanup()
        return False  # False == do not squelch any current exception

    def connect(self):
        '''
        Connect the p4 and p4gf connections.

        Note: p4gf_reviews and p4gf_reviews_non_gf are not connected!

        If this is the first time connecting, complete context initialization.
        '''

        # if previously connected, just reconnect
        if self.p4:
            if not self.p4.connected():
                p4_connect(self.p4)
            if not self.p4gf.connected():
                p4_connect(self.p4gf)
            return

        # create connections and use them to complete initialization of context
        self.p4                  = self.__make_p4( client=self.config.p4client )
        self.p4gf                = self.__make_p4( client=self.config.p4client_gf )
        self.p4gf_reviews        = self.__make_p4( user    = p4gf_util.gf_reviews_user_name()
                                                 , connect = False )
        self.p4gf_reviews_non_gf = self.__make_p4( user    = p4gf_const.P4GF_REVIEWS__NON_GF
                                                 , connect = False )
        self._client_pool        = ClientPool(self)
        self.__wrangle_charset()
        self.get_timezone_serverversion()
        self.__set_branch_creation()
        self.__set_merge_commits()
        self.__set_submodules()
        self.__set_change_owner()
        self.__set_up_paths()

    def disconnect(self):
        '''
        disconnect any p4 connections
        '''
        LOG.debug("context disconnecting {} {}"
                  .format( self.p4gf_reviews.connected()
                         , self.p4gf_reviews_non_gf.connected() ))
        # clean up temp clients before disconnecting p4gf
        if self.cleanup_client_pool:
            self._client_pool.cleanup()
        if self.p4.connected():
            p4_disconnect(self.p4)
        if self.p4gf.connected():
            p4_disconnect(self.p4gf)
        if self.p4gf_reviews.connected():
            p4_disconnect(self.p4gf_reviews)
        if self.p4gf_reviews_non_gf.connected():
            p4_disconnect(self.p4gf_reviews_non_gf)

    def last_copied_change_counter_name(self):
        '''
        Return a counter that holds the highest changelist number
        copied to the our repo, on our Git Fusion server.
        '''
        return calc_last_copied_change_counter_name(
                      self.config.view_name
                    , p4gf_util.get_server_id())

    def read_last_copied_change(self):
        '''
        Return the highest changelist number copied to a repo on a server.
        '''
        r = self.p4run(['counter', '-u', self.last_copied_change_counter_name()])
        return r[0]['value']

    def write_last_copied_change(self, change_num):
        '''
        Return the highest changelist number copied to a repo on a server.
        '''
        self.p4run([ 'counter', '-u', self.last_copied_change_counter_name()
                    , change_num])

    def user_to_protect(self, user):
        """Return a p4gf_protect.Protect instance that knows
        the given user's permissions."""
        # Lazy-create the user_to_protect instance since not all
        # Context-using code requires it.
        if not self._user_to_protect:
            self._user_to_protect = p4gf_protect.UserToProtect(self.p4)
        return self._user_to_protect.user_to_protect(user)

    def get_view_repo(self):
        '''
        Lazy-create the pygit2 repo object.

        Ideally we rename this to become a property getter "view_repo" and then
        data member self.view_repo becomes self._view_repo.
        Some future refactor day.
        '''
        if not self.view_repo:
            self.view_repo = pygit2.Repository(self.view_dirs.GIT_DIR)
        return self.view_repo

    def __make_p4(self, client=None, user=None, connect=True):
        """create a connection to the perforce server"""
        if not user:
            user = self.config.p4user
        if not client:
            client = self.config.p4client
        return create_p4(port=self.config.p4port, user=user, client=client, connect=connect)

    def __set_branch_creation(self):
        """Configure branch creation"""
        config = p4gf_config.get_repo(self.p4gf, self.config.view_name)
        self.branch_creation = config.getboolean(p4gf_config.SECTION_REPO,
                                                 p4gf_config.KEY_ENABLE_BRANCH_CREATION)
        LOG.debug('Enable repo branch creation = {0}'.format(self.branch_creation))

    def __set_merge_commits(self):
        """Configure merge commits"""
        config = p4gf_config.get_repo(self.p4gf, self.config.view_name)
        self.merge_commits = config.getboolean(p4gf_config.SECTION_REPO,
                                               p4gf_config.KEY_ENABLE_MERGE_COMMITS)
        LOG.debug('Enable repo merge commits = {0}'.format(self.merge_commits))

    def __set_submodules(self):
        """Configure submodule support"""
        config = p4gf_config.get_repo(self.p4gf, self.config.view_name)
        self.submodules = config.getboolean(p4gf_config.SECTION_REPO,
                                            p4gf_config.KEY_ENABLE_SUBMODULES)
        LOG.debug('Enable repo submodules = {0}'.format(self.submodules))

    def __set_change_owner(self):
        """Configure change ownership setting"""
        config = p4gf_config.get_repo(self.p4gf, self.config.view_name)
        value = config.get(p4gf_config.SECTION_REPO, p4gf_config.KEY_CHANGE_OWNER)
        value = str(value).lower()
        if value not in [p4gf_config.VALUE_AUTHOR, p4gf_config.VALUE_PUSHER]:
            LOG.warn("change-owner config setting has invalid value, defaulting to author")
            value = p4gf_config.VALUE_AUTHOR
        self.owner_is_author = True if value == 'author' else False
        LOG.debug('Set change owner to {0}'.format(value))

    def __wrangle_charset(self):
        """figure out if server is unicode and if it is, set charset"""
        if not self.p4.server_unicode:
            return
        # we have a unicode server
        # first, always use utf8 for the gf connection
        # use that connection to fetch the config for the repo
        self.p4gf.charset = 'utf8'
        config = p4gf_config.get_repo(self.p4gf, self.config.view_name)
        # then set the repo-specific charset for their connection
        self.p4.charset = config.get(p4gf_config.SECTION_REPO, p4gf_config.KEY_CHARSET)
        LOG.debug('repo charset will be: '+self.p4.charset)

    def client_view_path(self, change_num=None):
        '''
        Return "//{client}/..." : the client path for whole view,
        including ... wildcard

        Optional change_num, if supplied, appended as "@N"
        Because I'm sick of constructing that over and over.
        '''
        if change_num:
            return '{}@{}'.format(self.contentclientroot, change_num)
        return self.contentclientroot

    def client_view_union(self):
        """Return the client view to the union of all branches in this repo
        """
        p4map = p4gf_branch.calc_branch_union_client_view( self.config.p4client
                                                         , self.branch_dict())
        return p4map.as_array()

    def switch_client_view_to_union(self):
        """Change the repo's Perforce client view to the union of all branches
        in this repo.

        Warning: other objects retain pointers into OUR self.clientmap object.
        If we swap in a new object, those other objects point to stale data.
        If we change the values in the same object, those other objects point
        to new data. In neither case do these other objects get notification
        of change.
        """
        view_lines = self.client_view_union()
        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2('switch_client_view_to_union:\n{}'.format('\n'.join(view_lines)))
        else:
            LOG.debug('switch_client_view_to_union')

        self.switch_client_view_lines(view_lines)

    def switch_client_view_to_branch(self, branch):
        """Change the repo's Perforce client view to view of
        the given Branch object.

        branch is a p4gf_branch instance. See branch_dict() if you need to
        lookup by name.
        """
        LOG.debug('switch_client_view_to_branch() {}'.format(branch.to_log()))
        if branch.stream_name:
            self.switch_client_to_stream(branch)
        else:
            self.switch_client_view_lines(branch.view_lines)

    def switch_client_view_lines(self, lines):
        """Change this repo's Perforce client view to the given line list.

        Update our clientmap object, with the new lines. Update clientmap in
        place so that other object with pointers to OUR clientmap object will
        see the new data.
        """
        _lines = to_lines(lines)
        if _lines == self.last_view_lines:
            LOG.debug2('switch_client_view_lines() already switched. NOP.')
            return

        LOG.debug2('switch_client_view_lines() client={} {}'
                  .format(self.config.p4client, _lines))
        self.last_client_spec = p4gf_util.set_spec(
                            self.p4, 'client'
                          , spec_id = self.config.p4client
                          , values  = {'View': _lines, 'Stream': None}
                          , cached_vardict = self.last_client_spec)
        self.clientmap.clear()
        for line in _lines:
            self.clientmap.insert(line)
        self.last_view_lines = copy.copy(_lines)

    def switch_client_to_stream(self, branch):
        """Change this repo's Perforce client view to the given line list.

        Update our clientmap object, with the new lines. Update clientmap in
        place so that other object with pointers to OUR clientmap object will
        see the new data.
        """
        LOG.debug2('switch_client_view_stream() client={} stream={}'
                  .format(self.config.p4client, branch.stream_name))
        self.p4run(['client', '-f', '-s', '-S', branch.stream_name, self.config.p4client])

        lines = branch.view_lines
        _lines = to_lines(lines)

        if _lines == self.last_view_lines:
            return
        LOG.debug2('switch_client_view_streams() client={} {}'
                  .format(self.config.p4client, _lines))
        self.clientmap.clear()
        for line in _lines:
            self.clientmap.insert(line)
        self.last_view_lines = copy.copy(_lines)

    def checkout_master_ish(self):
        '''
        Switch Git to the first branch view defined in p4gf_config.
        This is often master, but not always.
        Since P4GF defaults to configuring a 'master' branch, which may
        not exist, try the other configured branches before giving up.

        NOP if no such branch (perhaps an empty p4gf_config?).
        '''
        branches = self.branch_dict()
        br = p4gf_branch.most_equal(branches)
        if br and br.git_branch_name:
            if not p4gf_util.git_checkout(br.git_branch_name):
                # Perhaps the most-equal branch does not exist?
                # Try the others until we achieve success.
                success = False
                for br in branches.values():
                    if br.git_branch_name and p4gf_util.git_checkout(br.git_branch_name):
                        success = True
                        break
                if not success:
                    LOG.warn('Unable to checkout sensible branch for {}'.format(
                        self.config.view_name))

    def git_branch_name_to_branch(self, git_branch_name):
        '''
        If we have an undeleted branch with the requested name, return it.
        If not, return None.

        O(n) scan.
        '''
        assert git_branch_name
        name = git_branch_name
        if name.startswith('refs/heads/'):
            name = name[len('refs/heads/'):]
        for branch in self.branch_dict().values():
            if branch.deleted:
                continue
            if branch.git_branch_name == name:
                return branch
        return None

    def branch_dict(self):
        """Return all known Git<->Perforce branch associations.
        Lazy-loaded from config file.

        Loads DepotBranchInfo dict as a side effect so that we can
        reconnect Branch.depot_branch pointers.
        """
        if not self._branch_dict:
            # Load p4gf_config: the fully populated branches.
            config = p4gf_config.get_repo(self.p4gf, self.config.view_name)
            self._branch_dict = p4gf_branch.dict_from_config(config, self.p4gf)

            # First branch listed in p4gf_config becomes our default HEAD.
            # This is usually 'master', but not required.
            bsl = p4gf_config.branch_section_list(config)
            if bsl:
                self._branch_dict[bsl[0]].more_equal = True

            # Load the lightweight and stream-based branch config data into the
            # branch_dict.  This is stored in p4gf_config2.  For lightweight
            # branches, the full branch def is there.  For stream-based branches
            # all we care about is the original-view, which gets merged with
            # any config stored in p4gf_config.
            config2 = p4gf_config.get_repo2(self.p4gf, self.config.view_name)
            if config2:
                branch_dict2 = p4gf_branch.dict_from_config(config2, self.p4gf)
                lwb_dict = {}
                for branch in branch_dict2.values():
                    if branch.stream_name:
                        if branch.branch_id in self._branch_dict:
                            self._branch_dict[branch.branch_id].original_view_lines = \
                                branch.original_view_lines
                    else:
                        branch.is_lightweight = True
                        if (    branch.depot_branch
                            and isinstance(branch.depot_branch, str)):
                            branch.depot_branch = self.depot_branch_info_index() \
                                      .find_depot_branch_id(branch.depot_branch)
                        lwb_dict[branch.branch_id] = branch
                self._branch_dict.update(lwb_dict)
            for b in self._branch_dict.values():
                b.set_rhs_client(self.config.p4client)

            LOG.debug('branch_dict() lazy-loaded ct={}'
                      .format(len(self._branch_dict)))
            if LOG.isEnabledFor(logging.DEBUG2):
                for b in self._branch_dict.values():
                    LOG.debug2('\n' + b.to_log(LOG))

        return self._branch_dict

    def undeleted_branches(self):
        '''
        An iterator/generator of all Branch values in branch_dict
        that are not deleted.
        '''
        for branch in self.branch_dict().values():
            if branch.deleted:
                continue
            yield branch

    def depot_branch_info_index(self):
        '''
        Return all known depot branches that house files for lightweight
        branches. This includes depot branches that other Git Fusion repos
        created: we must stay lightweight even when sharing across repos.

        Lazy-loaded from all depot branch-info files.
        '''
        if not self._depot_branch_info_index:
            self._depot_branch_info_index = p4gf_depot_branch.DepotBranchInfoIndex()
            root = p4gf_const.P4GF_DEPOT_BRANCH_INFO_ROOT.format(
                                              P4GF_DEPOT=p4gf_const.P4GF_DEPOT)
            root = root + '/...'
            with p4gf_util.RawEncoding(self.p4):
                file_data = self.p4.run('print', root)
            delete = False
            started = False
            file_contents = ''
            for item in file_data:
                if isinstance(item, dict):
                    if started:     # finish with the current branch info
                        dbi = p4gf_depot_branch.depot_branch_info_from_string(file_contents)
                        self._depot_branch_info_index.add(dbi)
                    if item['action'] == 'delete':
                        started = False
                        delete = True
                    else:
                        file_contents = ''
                        delete = False
                        started = True
                else:
                    if delete:
                        continue
                    new_item = item.decode().strip()
                    if len(new_item):
                        file_contents = file_contents + new_item

            if started:
                dbi = p4gf_depot_branch.depot_branch_info_from_string(file_contents)
                self._depot_branch_info_index.add(dbi)

            if LOG.isEnabledFor(logging.DEBUG):
                for dpid in self._depot_branch_info_index.by_id:
                    LOG.debug("DBI index LAZY loaded: {0} {1}".format(dpid
                        , self._depot_branch_info_index.by_id[dpid]))

            ### YAGNI until we have a test that proves it.
            ### p4 print utf8 //P4GF_DEPOT/branches/branch-info/...

        return self._depot_branch_info_index

    def get_timezone_serverversion(self):
        """get server's timezone and server version via p4 info"""
        r = self.p4.run_info()
        server_date = p4gf_util.first_value_for_key(r, 'serverDate')
        self.timezone = server_date.split(" ")[2]
        self.server_version = p4gf_util.first_value_for_key(r, 'serverVersion')

    def __set_up_paths(self):
        """set up depot and local paths for both content and P4GF

        These paths are derived from the client root and client view.
        """
        self.__set_up_content_paths()
        self.__set_up_p4gf_paths()
        self.view_dirs = p4gf_view_dirs.from_p4gf_dir(self.gitrootdir, self.config.view_name)

    def __set_up_content_paths(self):
        """set up depot and local paths for both content and P4GF

        These paths are derived from the client root and client view.
        """

        client = self.p4.fetch_client()
        self.clientmap = Map(client["View"])
        # If the len of the client Views differs from the len of the Map
        # then the P4 disabmbiguator added exclusionary mappings - note this here
        # for reporting a message back to the user.
        self.client_exclusions_added = len(client["View"]) != len(self.clientmap.as_array())

        # local syntax client root, force trailing /
        self.contentlocalroot = client["Root"]
        if not self.contentlocalroot.endswith("/"):
            self.contentlocalroot += '/'

        # client sytax client root with wildcard
        self.contentclientroot = '//' + self.p4.client + '/...'

    def __set_up_p4gf_paths(self):
        """set up depot and local paths for P4GF

        These paths are derived from the client root and client view.
        """

        client = self.p4gf.fetch_client()
        self.client_spec_gf = client
        self.gitrootdir = client_spec_to_root(client)
        self.clientmap_gf = Map(client["View"])

        lhs = self.clientmap_gf.lhs()
        check_client_view_gf(lhs)

        assert len(lhs) == 1, _('view must contain only one line')
        rpath = self.clientmap_gf.translate(lhs[0])
        self.gitlocalroot = strip_wild(client_path_to_local(
            rpath, self.p4gf.client, self.gitrootdir))

    def __str__(self):
        return "\n".join(["Git data in Perforce:   " + self.gitdepotroot + "...",
                          "                        " + self.gitlocalroot + "...",
                          "Exported Perforce tree: " + self.contentlocalroot + "...",
                          "                        " + self.contentclientroot,
                          "timezone: " + self.timezone])

    def __repr__(self):
        return str(self) + "\n" + repr(self.mirror)

    def log_context(self):
        """Dump connection info, client info, directories, all to log category
        'context' as INFO."""

        log = logging.getLogger('context')
        if not log.isEnabledFor(logging.INFO):
            return

        # Dump client spec as raw untagged text.
        self.p4.tagged = 0
        client_lines_raw = self.p4.run('client', '-o')[0].splitlines()
        self.p4.tagged = 1
        # Strip comment header
        client_lines = [l for l in client_lines_raw if not l.startswith('#')]

        # Dump p4 info, tagged, since that includes more pairs than untagged.
        p4info = p4gf_util.first_dict(self.p4.run('info'))
        key_len_max = max(len(k) for k in p4info.keys())
        info_template = NTR('%-{}s : %s').format(key_len_max)

        log.info(info_template, 'P4PORT',     self.p4.port)
        log.info(info_template, 'P4USER',     self.p4.user)
        log.info(info_template, 'P4CLIENT',   self.p4.client)
        log.info(info_template, 'p4gfclient', self.p4gf.client)

        for k in sorted(p4info.keys(), key=str.lower):
            log.info(info_template, k, p4info[k])

        for line in client_lines:
            log.info(line)

    def _check_heartbeat_canceled(self):
        '''
        Ask the server if someone else wants us to stop.
        Remote kill switch: delete our counter and we'll voluntarily stop.

        'p4 counter -u git-fusion-view-{repo}-lock'

        Throttled back to check only once every N seconds.
        '''
        # Check no more than once every N seconds.
        now = time.time()
        if self._last_heartbeat_cancel_check_time:
            since_secs = now - self._last_heartbeat_cancel_check_time
            if since_secs < _CANCEL_POLL_PERIOD_SECS:
                return
            self._last_heartbeat_cancel_check_time += _CANCEL_POLL_PERIOD_SECS
        else:
            self._last_heartbeat_cancel_check_time = now

        if self.view_lock.canceled():
            raise p4gf_lock.LockCanceled(_("Canceling: lock '{}' lost.")
                                        .format(self.view_lock.counter_name()))

    def heartbeat(self):
        '''
        If we have a view lock, update its heartbeat.

        If our lock is cleared, then raise a RuntimeException
        canceling our current task.
        '''
        if not self.view_lock:
            return

        self._check_heartbeat_canceled()
        self.view_lock.update_heartbeat()
        self.log_memory_usage()

    def log_memory_usage(self):
        '''Log our memory usage on a regular basis.'''
        if LOG.isEnabledFor(logging.DEBUG):
            now = time.time()
            if self._heartbeat_time and now - self._heartbeat_time < MEMLOG_HEART_RATE:
                return
            self._heartbeat_time = now
            LOG.debug(p4gf_log.memory_usage())

    def _convert_path(self, clazz, path):
        '''Return a path object that can convert to other formats.'''
        return clazz( self.clientmap
                      , self.config.p4client
                      , self.contentlocalroot[:-1]  # -1 to strip trailing /
                      , path)

    def depot_path(self, path):
        '''Return an object that can convert from depot to other syntax.'''
        return self._convert_path(p4gf_path_convert.DepotPath, path)

    def client_path(self, path):
        '''Return an object that can convert from client to other syntax.'''
        return self._convert_path(p4gf_path_convert.ClientPath, path)

    def gwt_path(self, path):
        '''
        Return an object that can convert from Git work tree to other syntax.
        '''
        return self._convert_path(p4gf_path_convert.GWTPath, path)

    def gwt_to_depot_path(self, gwt_path):
        '''
        Optimized version of ctx.gwt_path(gwt).to_depot().

        Avoid creating the convert and goes straight to P4.Map.translate().

        We call this once for every row in G2PMatrix. This function runs in
        about 60% of ctx.gwt_path(x).to_depot() time. That works out to about 5%
        of total wall clock time for many-file repos such as james.
        '''
        gwt_esc = p4gf_util.escape_path(gwt_path)
        client_path = '//{}/'.format(self.config.p4client) + gwt_esc
        return self.clientmap.translate(client_path, self.clientmap.RIGHT2LEFT)

    def depot_to_gwt_path(self, depot_path):
        '''
        Optimized version of ctx.depot_path(dp).to_gwt().

        Avoid creating the convert and goes straight to P4.Map.translate().

        We call this once for every row in G2PMatrix. This function runs in
        about 60% of ctx.depot_path(x).to_gwt() time. That works out to about 5%
        of total wall clock time for many-file repos such as james.
        '''
        client_path = self.clientmap.translate(depot_path, self.clientmap.LEFT2RIGHT)
        gwt_esc       = client_path[3+len(self.config.p4client):]
        return p4gf_util.unescape_path(gwt_esc)

    def local_path(self, path):
        '''
        Return an object that can convert from absolute local filesystem
        to other syntax.
        '''
        return self._convert_path(p4gf_path_convert.LocalPath, path)

                        # pylint:disable=R0913
                        # Too many arguments
                        # We really should just dial that up to 11
                        # if we're going to keep ignoring its advice.
    @staticmethod
    def _p4run( p4
              , numbered_change
              , run_history
              , cmd
              , log_warnings
              , log_errors ):
        '''
        Record the command in history, then perform it.
        '''
        if numbered_change:
            cmd = numbered_change.add_change_option(cmd)
        run_history.append(cmd)
        return p4gf_util.p4run_logged( p4, cmd
                                     , log_warnings = log_warnings
                                     , log_errors   = log_errors )
                        # pylint:enable=R0913

    def p4run( self, cmd
             , log_warnings = logging.WARNING
             , log_errors   = logging.ERROR):
        '''
        Run a command, with logging.
        '''
        return self._p4run( p4              = self.p4
                          , numbered_change = self.numbered_change
                          , run_history     = self.p4run_history
                          , cmd             = cmd
                          , log_warnings    = log_warnings
                          , log_errors      = log_errors
                          )

    def p4gfrun(self, cmd
               , log_warnings = logging.WARNING
               , log_errors   = logging.ERROR):
        '''
        Run a command, with logging.
        '''
        return self._p4run( p4              = self.p4gf
                          , numbered_change = self.numbered_change_gf
                          , run_history     = self.p4gfrun_history
                          , cmd             = cmd
                          , log_warnings    = log_warnings
                          , log_errors      = log_errors
                          )

    def switched_to_view_lines(self, view_lines):
        '''
        Return an RAII object to switch p4 connection to a different,
        temporary, client spec, with the requested view lines, then
        restore p4 connection to original client on exit.

        Use this ONLY for read-only commands, no opening for edit or submit.
        Ignore any client_path values: they will contain the name of the
        temporary client, not our real client.
        '''
        return View(self, view_lines)

    def switched_to_union(self):
        '''
        Return an RAII object to switch p4 connection to a different,
        temporary, client spec, with the requested view lines, then
        restore p4 connection to original client on exit.

        Use this ONLY for read-only commands, no opening for edit or submit.
        Ignore any client_path values: they will contain the name of the
        temporary client, not our real client.
        '''
        return View(self, self.client_view_union())

    def switched_to_branch(self, branch):
        '''
        Return an RAII object to switch p4 connection to a different,
        temporary, client spec, with the requested view lines, then
        restore p4 connection to original client on exit.

        Use this ONLY for read-only commands, no opening for edit or submit.
        Ignore any client_path values: they will contain the name of the
        temporary client, not our real client.
        '''
        return View(self, branch.view_lines)

    def temp_branch(self, create_if_none=True):
        '''
        Retrieve the shared temporary branch associated with this context.
        If create_if_none is False and there is no temporary branch already,
        None will be returned.
        '''
        if self._temp_branch is None and create_if_none:
            self._temp_branch = TempP4BranchMapping()
        return self._temp_branch

    def is_feature_enabled(self, feature):
        '''
        Return whether feature is enabled for this repo.
        Looks in @features section of config, repo first then global.
        If a feature is not set in config it defaults to not enabled.
        '''
        config = p4gf_config.get_repo(self.p4gf, self.config.view_name)
        return p4gf_config.is_feature_enabled(config, feature)

    @property
    def preflight_hook(self):
        '''
        Return our preflight hook, creating if necessary.
        '''
        if not self._preflight_hook:
            self._preflight_hook = PreflightHook.from_context(self)
        return self._preflight_hook

# -- end class Context --------------------------------------------------------


class View:
    '''
    RAII class for switching client view by temporarily changing current
    P4 connection to use a different client, with the requested view.
    '''
    # pylint:disable=W0212
    # pylint "Access to a protected member"
    def __init__(self, ctx, view_lines):
        self.ctx = ctx
        self.new_lines              = view_lines
        self.save_lines             = ctx.clientmap.as_array()
        self.save_contentclientroot = self.ctx.contentclientroot

    def __enter__(self):
        client_name = self.ctx._client_pool.for_view(self.new_lines)
        self.ctx.p4.client = client_name
        self.ctx.contentclientroot = '//{}/...'.format(client_name)
        # Must set the clientmap (as was done by switch_client_view_lines)
        self.ctx.clientmap.clear()
        for line in self.new_lines:
            self.ctx.clientmap.insert(line)

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.ctx.p4.client = self.ctx.config.p4client
        self.ctx.contentclientroot = self.save_contentclientroot
        # Must set the clientmap (as was done by switch_client_view_lines)
        self.ctx.clientmap.clear()
        for line in self.save_lines:
            self.ctx.clientmap.insert(line)
