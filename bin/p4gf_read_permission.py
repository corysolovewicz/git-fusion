#! /usr/bin/env python3.3

"""Determine whether a user has repo read permissions.
This is an all or nothing test. Either the user may read all
views in all branches or is denied repo read access."""

import os
import logging

import P4
import p4gf_const
from   p4gf_l10n           import _, NTR
import p4gf_log
import p4gf_util
import p4gf_protect
import p4gf_config
import p4gf_context
import p4gf_branch
from p4gf_config_validator import view_lines_define_empty_view


LOG = p4gf_log.for_module()

# simulate enum with new type
def enum(*sequential, **named):
    '''Implement enum as dictionary.'''
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type(NTR('Enum'), (), enums)

# enum used by _compare_paths()
PATHS = enum('EQUAL', 'NO_OVERLAP', 'SUBSET', 'SUPERSET', 'OVERLAP')

# indexes into (view_path, is_marked, is_inclusion) tuple
VIEW  = 0
MARK  = 1
INCLU = 2

READ_DENIED_MSG = _("User '{0}' denied read access to repo '{1}' by Perforce.")
#EXCLUSION_MSG = READ_DENIED_MSG + "\nwith user protects '{2}'."
#NOT_INCLUDED_MSG = READ_DENIED_MSG + "\nA repo view is not granted read access."
NO_PROCTION_LINES = _("No read permissions exist for these views")

OPTIMIZE_WITH_UNION = True


def _views_have_no_exclusions(views):
    """
    Return True if these views contain no exclusions.
    """
    for v in views:
        if v.startswith('-') or v.startswith('"-'):
            return False
    return True

def _remove_view_modifier(line):
    """
    Remove the leading - or + from a view line.
    """
    l = line
    if line.startswith('-') or line.startswith('+'):
        l = line[1:]
    elif line.startswith('"-') or  line.startswith('"+'):
        l = '"' + line[2:]
    return l


def _compare_paths(path_a, path_b):
    """
    Return the relation between two paths as named in the enums below.
    Ignore leading - or +
    """
    a = _remove_view_modifier(path_a)
    b = _remove_view_modifier(path_b)
    if a == b:
        return PATHS.EQUAL
    amap = P4.Map(a, a)
    bmap = P4.Map(b, b)
    jmap = P4.Map.join(amap, bmap)
    if P4.Map.is_empty(jmap):
        return PATHS.NO_OVERLAP
    else:
        c = P4.Map.lhs(jmap)[0]
        if a == c:
            return PATHS.SUBSET
        elif b == c:
            return PATHS.SUPERSET
        else:
            return PATHS.OVERLAP

class ReadPermission:
    """
    Determine whether user's read permissions permit repo access.
    """

    def __init__(self, p4, view_perm):
        self.p4               = p4
        self.p4user           = view_perm.p4user_name
        self.view_name        = view_perm.view_name
        self.view_perm        = view_perm
        self.gf_user          = os.environ.get(p4gf_const.P4GF_AUTH_P4USER)
        self.p4client         = None
        self.p4client_created = False
        self.config           = None
        self.user_to_protect  = p4gf_protect.UserToProtect(self.p4)
        self.current_branch   = None
        self.user_branch_protections = None
        self.branches_without_exclusions = None

    def p4run( self, cmd
             , log_warnings = logging.WARNING
             , log_errors   = logging.ERROR):
        """
        Run a command, with logging.
        """
        return p4gf_util.p4run_logged( self.p4, cmd
                                     , log_warnings = log_warnings
                                     , log_errors   = log_errors)

    def get_branch_dict(self):
        """Get a branch dictionary for this repo.

        If the p4gf_config exists, use that.
        Else if the p4 client exists
        create a branch dict containing a branch from the client views.
        Else return None
        """
        LOG.debug("get_branch_dict for {0}".format(self.view_name))
        # Repo config file already checked into Perforce?
        # Use that.
        config_path   = p4gf_config.depot_path_repo(self.view_name)
        config_exists = p4gf_util.depot_file_exists(self.p4, config_path)
        if config_exists:
            self.config = p4gf_config.get_repo(self.p4, self.view_name)
            if self.config:
                return p4gf_branch.dict_from_config(self.config, self.p4)
            else:
                return None
        else:
            LOG.debug("checking if client {0} exists.".format(self.view_name))
            if not p4gf_util.spec_exists(self.p4, 'client', self.view_name):
                LOG.debug("         client {0} NOT exists.".format(self.view_name))
                return None
            view_lines = p4gf_util.first_value_for_key(
                    self.p4.run('client', '-o', '-t', self.view_name, self.p4client),
                    'View')
            if not view_lines:
                return None
            else:
                # create a Branch object to manage this client view
                if isinstance(view_lines, str):
                    view_lines = view_lines.splitlines()
                LOG.debug("create branch from client views: {0}".format(view_lines))
                branch = p4gf_branch.Branch()
                branch.branch_id = 'master'
                branch.git_branch_name = 'master'
                branch.view_p4map = P4.Map(view_lines)
                branch.view_lines = view_lines
                LOG.debug("create branch from client branch view_p4map: {0}".
                        format(branch.view_p4map))
                LOG.debug("create branch from client branch view_lines: {0}".
                        format(branch.view_lines))
                branch_dict = {}
                branch_dict[branch.branch_id] = branch
                return branch_dict


    def switch_client_to_stream(self, branch):
        """
        Change this repo's Perforce client view to the branch's stream.
        """
        # Lazy create our read-perm client for streams
        if not self.p4client_created:
            p4gf_util.set_spec(
                                self.p4, 'client'
                              , spec_id = self.p4client
                              , cached_vardict = None)
            self.p4client_created = True
        self.p4run(['client', '-f', '-s', '-S', branch.stream_name, self.p4client])

    def switch_client_view_to_branch(self, branch):
        """
        Set the repo's Perforce client to view of the given Branch object.
        The client is used only by this class.
        """
        if branch.stream_name:
            self.switch_client_to_stream(branch)
        else:
            self.switch_client_view_lines(branch.view_lines)

    def switch_client_view_lines(self, lines):
        """
        Change this repo's Perforce client view to the given line list.
        """
        LOG.debug("switch_client_view_lines {0}".format(lines))
        _lines = p4gf_context.to_lines(lines)
        p4gf_util.set_spec(
                            self.p4, 'client'
                          , spec_id = self.p4client
                          , values  = {'View': _lines, 'Stream': None}
                          , cached_vardict = None)
        self.p4client_created = True

    def gf_user_has_list_permissions(self):
        """
        Determine whether git-fusion-user has 'list' permissions as its last protects line.
        Only required when appyling 'user' read-permission-check.
        """
        protects_dict = self.user_to_protect.user_to_protect(
                p4gf_const.P4GF_USER).get_protects_dict()
        last_perm = protects_dict[-1]
        return last_perm['perm'] == 'list' and last_perm['depotFile'] == '//...'

    @classmethod
    def _protect_dict_to_str(cls, pdict):
        """
        Format one protection line as dictionary to string.
        """
        excl = '-' if 'unmap' in pdict else ''
        if NTR('user') in pdict:
            user = NTR('user ') + pdict['user']
        else:
            user = NTR('group ') + pdict['group']
        return "{0} {1} {2} {3}{4}".format(
                pdict['perm'], user, pdict['host'], excl, pdict['depotFile'])

    def log_rejected_not_included(self, view_mark_inclusion):
        """
        Some view paths are not included in the protections.
        Report only repo is protected to git user.
        LOG the unpermitted views.
        """
        msg = READ_DENIED_MSG.format(self.p4user, self.view_name)
        self.view_perm.error_msg = '\n' + msg
        for view_path in [ vmi[VIEW] for vmi in view_mark_inclusion if not vmi[MARK]]:
            msg += _('\n     denied view by missing inclusion: {0}').format(view_path)
        LOG.warn (msg)

    def log_rejected_excluded(self, view_path):
        """
        Report only repo is protected to git user.
        LOG the offending excluded view.
        """
        msg = READ_DENIED_MSG.format(self.p4user, self.view_name)
        self.view_perm.error_msg = '\n' + msg
        msg += _('\n     denied view by exclusion: {0}').format(view_path)
        LOG.warn (msg)

    # pylint: disable=R0912
    # Too many branches
    def check_views_read_permission(self):
        """
        Check a set of view_lines against a user's read permissions.

        Compare each view line (bottom up) against each protect line (bottom up).
        By this strategy, later views once passed by later protections need not
        be rejected by earlier protections.
        Marking a view = True marks it as being granted read permission.

        If all inclusionary views are marked readable return True.
        If a yet unmarked inclusionary view line compares as not NO_OVERLAP
        to an exlusionary protect line return False.
        If any inclusionary view lines remain unmarked after testing
        against all protect lines return False.

        See: doc/p4gf_read_protect.py  and  doc/p4gf_compare_paths.py
        """
        # always get the views from the P4.Map to apply the disambiguator
        view_lines = self.current_branch.view_p4map.lhs()
        # Get the full permissions granted this user by requesting READ
        read_protections = p4gf_protect.create_read_permissions_map(
                self.user_branch_protections.get_protects_dict(),
                p4gf_protect.READ).lhs()

        LOG.debug ("check_views_read_permission: protections {0} ".format(
            self.user_branch_protections.get_protects_dict()))
        LOG.debug ("check_views_read_permission: user {0} : view {1}".
                    format(self.p4user, self.view_name) +
                   "\nview_lines: {0} \nprotects: {1}".format(view_lines, read_protections))

        # A client may be defined such the views resolve to empty
        # In this case PASS the read check - as nothing can be denied
        if view_lines_define_empty_view(view_lines):
            return True

        # initially the number of lines in view which are NOT exclusions
        unmarked_inclusions_count = 0


        # create a list of view tuples (view_line, is_marked, is_inclusion)
        # if is_marked == True, view line is readable
        # count non-exclusion lines
        view_mark_inclusion = []
        for v in view_lines:
            if not v.startswith('-'):
                unmarked_inclusions_count += 1
                view_mark_inclusion.append((v, False, True))
            else:
                view_mark_inclusion.append((v, False, False))
        lastidx = len(view_mark_inclusion) -1
        for p in read_protections[::-1]:               # reverser order slice
            for vix in range(lastidx, -1, -1):
                vmi = view_mark_inclusion[vix]
                result = _compare_paths(vmi[VIEW], p)
                if result ==  PATHS.NO_OVERLAP:
                    continue                           # vmi inner loop
                if p.startswith('-'):                  # p is exclusion
                    if vmi[INCLU] and not vmi[MARK]:   # +view and not marked
                        # in this case reject for all test results and deny read permission
                        self.log_rejected_excluded(vmi[VIEW])
                        return False
                    # case with -view OR +view and marked
                    if result == PATHS.SUBSET or result == PATHS.OVERLAP:
                        continue                        # vmi inner loop
                    else:  # PATHS.EQUAL || PATHS.SUPERSET
                        break    # out of vmi loop into protects loop
                else:  # p not exclusion
                    if result == PATHS.SUBSET or result == PATHS.EQUAL:
                        if vmi[INCLU]:                  # +view
                            # mark this view as granted read permission
                            view_mark_inclusion[vix] = (vmi[VIEW], True, vmi[INCLU])
                            unmarked_inclusions_count -= 1        # decrease unmarked count
                        if unmarked_inclusions_count <= 0:
                            return True
                    else:
                        continue   # next vmi

        self.log_rejected_not_included(view_mark_inclusion)
        return False   # something must have been left unmarked
    # pylint: enable=R0912

    def check_branch_read_permissions(self, branch):
        """
        Check a repo  branch against a user's read permissions
        """
        LOG.debug ("read_permission_check_for_view : switch to branch dict {0}".
                format(branch.to_log(LOG)))
        self.switch_client_view_to_branch(branch)
        self.user_branch_protections = self.user_to_protect.user_view_to_protect(self.p4user,
                self.p4client)
        self.current_branch = branch
        return self.check_views_read_permission()

# pylint: disable = C0301,R0912
# Line too long;  Too many branches
    def read_permission_check_for_repo(self):
        """
        Determine whether the user's p4 protects permit read access to the repo.
        """
        # Indicates this test was invoked by GF global configuration setting
        self.view_perm.user_read_permission_checked = True

        save_client = self.p4.client
        self.p4client = p4gf_util.view_to_client_name(self.view_name) + "-read-perm"
        self.p4.client = self.p4client
        branch_dict = self.get_branch_dict()
        if not branch_dict:
            LOG.debug("no branch_dict for {0}".format(self.view_name))
            # No p4gf_config and no client - so return the same message as does p4gf_init_repo
            nop4client = _("p4 client '{0}' does not exist\n").format(self.view_name)
            self.view_perm.error_msg = '\n' + p4gf_const.NO_REPO_MSG_TEMPLATE.format(view_name=self.view_name
                    ,view_name_p4client=self.view_name
                    ,nop4client=nop4client)
            LOG.warn(self.view_perm.error_msg)
            self.view_perm.user_perm_view_pull = False
            return self.view_perm.user_perm_view_pull

        num_branches = len(branch_dict)
        LOG.debug("read_permission_check_for_repo repo: {0}  num branches {1}".
                format(self.view_name, num_branches))
        branches_with_no_exclusions_list = {}  # all branches views to be collection into a single branch
        branches_with_exclusions_list  = {}    # each branch to be handled independently
        self.view_perm.user_perm_view_pull = True
        # segregate branches into two dictionary lists while setting rhs client name
        for b in branch_dict.values():
            # if these branches came from a p4gf_config we need to set the right hand sides
            if self.config:
                b.set_rhs_client(self.p4client)
            if num_branches > 1 and OPTIMIZE_WITH_UNION:
                # Do not attempt to collect stream views into a single union branch
                # Handle each stream independently by assigning to the list of exlusion branches
                if b.stream_name:
                    branches_with_exclusions_list[b.branch_id] = b
                elif _views_have_no_exclusions(b.view_p4map.lhs()):
                    branches_with_no_exclusions_list[b.branch_id] = b
                else:
                    branches_with_exclusions_list[b.branch_id] = b
        if num_branches > 1 and OPTIMIZE_WITH_UNION :
            if branches_with_no_exclusions_list:
                # copy any element from dict as the union branch base
                base_branch = next (iter (branches_with_no_exclusions_list.values()))
                union_branch = p4gf_branch.Branch.from_branch(base_branch, "gf_read_perm_union_branch")
                #set the views to the union of all the non-exclusionary branches
                union_branch.view_p4map = p4gf_branch.calc_branch_union_client_view(self.p4client
                                                             , branches_with_no_exclusions_list)
                union_branch.view_lines = union_branch.view_p4map.as_array()
                # check the branch with union views
                self.view_perm.user_perm_view_pull = self.check_branch_read_permissions(union_branch)
            # now prepare to test the branches with exclusions
            branch_dict = branches_with_exclusions_list

        if branch_dict and self.view_perm.user_perm_view_pull:
            # check each branch for read permissions
            for branch in branch_dict.values():
                self.view_perm.user_perm_view_pull = self.check_branch_read_permissions(branch)
                LOG.debug("read_permission_check_for_repo branch {0} = {1}".format(branch.branch_id, self.view_perm.user_perm_view_pull))
                if not self.view_perm.user_perm_view_pull:
                    break

        self.p4.client =  save_client
        # delete this temporary read perm only client
        if self.p4client_created:
            self.p4run(['client', '-df', self.p4client])
        return self.view_perm.user_perm_view_pull
# pylint: enable = C0301, R0912
