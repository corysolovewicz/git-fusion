#! /usr/bin/env python3.3
'''
A branch of depot hierarchy in which a lightweight branch stores its files.
'''
import logging
from   collections import namedtuple
import configparser
import copy

import p4gf_config
import p4gf_const
from   p4gf_l10n    import NTR
import p4gf_util

# Section [{depot branch id}]
KEY_ROOT_DEPOT_PATH     = NTR('root-depot-path')
KEY_PARENT_BRANCH_ID    = NTR('parent-branch-id')
KEY_PARENT_CHANGELIST   = NTR('parent-changelist')
KEY_PARENT_BRANCH_ID_N  = NTR('parent-{}-branch-id')
KEY_PARENT_CHANGELIST_N = NTR('parent-{}-changelist')

LOG = logging.getLogger(__name__)

class DepotBranchInfo:
    '''
    Describe a branch of depot hierarchy.

    This is the same data that we store in a depot branch's branch-info file.
    '''

    def __init__(self):
                # Depot branch ID is separate from any lightweight branch
                # mapping that uses this depot branch. Because a single depot
                # branch might be mapped to multiple Git branches, a single
                # Git branch might map to multiple depot branches. Highly
                # unlikely at first, but after a few repo refactors, it could
                # happen.
        self.depot_branch_id        = None

                # A single path without the trailing "/...".
        self.root_depot_path        = None

                # Matching lists of depot branch/changelist values
                # that tell us from where to JIT-branch files later.
                #
                # Usually only a single value, but it is possible to create
                # a Git branch whose first commit has mulitiple parents.
                #
                # Can be empty lists for orphan branches.
        self.parent_depot_branch_id_list = []
        self.parent_changelist_list      = []

                # Is this structure new, not yet written to a branch-info file?
                # Needs to be 'p4 add'ed.
                # Was needs_write
        self.needs_p4add                 = False

                # Is this structure old, but has modifications that need
                # to be 'p4 edit'ed into to its existing branch-info file?
        self.needs_p4edit                = False

                # List of FileDeletion elements.
                #
                # Files that would have been JIT-branched and then deleted in
                # which changelist, if Perforce supported branch-for-delete
                # file actions.
        self._file_deletions             = []

    def __repr__(self):
        s = "depot branch id={} root={}".format(self.depot_branch_id, self.root_depot_path)
        if self.parent_depot_branch_id_list:
            l = ' '.join(["{}@{}".format(br, cl)
                          for br, cl in zip( self.parent_depot_branch_id_list
                                           , self.parent_changelist_list     )])
            s += "; " + l
        return s

    def to_config(self):
        '''
        Return a new ConfigParser object with our data.
        '''
        config = configparser.ConfigParser( interpolation  = None
                                          , allow_no_value = True)
        section = self.depot_branch_id
        config.add_section(section)
        config[section][KEY_ROOT_DEPOT_PATH] = self.root_depot_path
        if not self.parent_depot_branch_id_list:
            return config

        # First parent doesn't need an index. Most depot branches have only
        # one parent depot branch and it's silly to pollute the world with
        # unnecessary ordinals.
        config[section][KEY_PARENT_BRANCH_ID ] = str(self.parent_depot_branch_id_list[0])
        config[section][KEY_PARENT_CHANGELIST] = str(self.parent_changelist_list[0])

        # Second-and-later parents. Rare, but write 'em out with
        # numbers in their keys.
        for i in range(1, len(self.parent_depot_branch_id_list)):
            key_id = KEY_PARENT_BRANCH_ID_N .format(1+i)
            key_cl = KEY_PARENT_CHANGELIST_N.format(1+i)
            val_id = str(self.parent_depot_branch_id_list[i])
            val_cl = str(self.parent_changelist_list     [i])
            config[section][key_id] = val_id
            config[section][key_cl] = val_cl

        return config

    def to_config_depot_path(self):
        '''
        Return the depot path to our branch-info file where we store our data.
        '''
        root = p4gf_const.P4GF_DEPOT_BRANCH_INFO_ROOT.format(
                                              P4GF_DEPOT=p4gf_const.P4GF_DEPOT)
        return root + '/' + p4gf_util.enslash(self.depot_branch_id)

    def contains_depot_file(self, depot_file):
        '''
        Does this Depot branch root hold depot_file?
        '''
        return depot_file.startswith(self.root_depot_path + '/')

    def change_num_to_deleted_depot_path_list(self, change_num):
        '''
        Return a list of depot_path files deleted in this depot branch.

        Must include all files "deleted" by a missing JIT-branch-for-delete
        action.

        May include files actually 'p4 delete'd, but not required to include all
        or omit all.
        '''
        return [fd.depot_path for fd in self._file_deletions
                if fd.change_num == change_num]

# -----------------------------------------------------------------------------

def abbrev(dbi):
    '''
    Return first 7 char of branch ID, or "None" if None.
    '''
    if isinstance(dbi, DepotBranchInfo):
        return p4gf_util.abbrev(dbi.depot_branch_id)
    return p4gf_util.abbrev(dbi)


def new_definition(ctx):
    '''
    Factory method to generate an return a new depot branch definition.
    '''
    dbi = DepotBranchInfo()
    dbi.depot_branch_id = p4gf_util.uuid(ctx.p4)
    dbi.root_depot_path = new_depot_branch_root( ctx
                                               , dbi.depot_branch_id )
    dbi.needs_p4add     = True
    return dbi


def new_depot_branch_root(ctx, depot_branch_id):
    '''
    Return a path to a new root of depot hierarchy where a lightweight
    branch can store future files.

    ctx is used to pull in configurable value(s) for the root template string.
    '''
    template = p4gf_const.P4GF_DEPOT_BRANCH_ROOT

    slashed_id = p4gf_util.enslash(depot_branch_id)

    return template.format( P4GF_DEPOT = p4gf_const.P4GF_DEPOT
                          , repo_name  = ctx.config.view_name
                          , branch_id  = slashed_id
                          )


def depot_branch_info_from_string(dbi_string):
    """ Return DepotBranchInfo from string (from file contents)"""
    config = configparser.ConfigParser( interpolation  = None
                                      , allow_no_value = True )
    config.read_string(dbi_string)
    dbi = depot_branch_info_from_config(config)
    p4gf_config.clean_up_parser(config)
    return dbi

def _dbid_section(config):
    '''
    Return the ConfigParser section that is this Depot Branch's ID.
    '''
    for sec in config.sections():
        if sec:
            return sec
    return None

                        # pylint:disable=W0212
                        # Access to a protected member _file_deletions
                        # We're a factory. We get to poke around
                        # in protected members.

def depot_branch_info_from_config(config):
    """ Return DepotBranchInfo from configparser object"""
    dbi = DepotBranchInfo()
    dbi.depot_branch_id = _dbid_section(config)
    dbi.root_depot_path = config.get(dbi.depot_branch_id, "root-depot-path")
    firstbranch = None
    firstcl     = None
    branch      = []
    cl          = []
    for option in config.options(dbi.depot_branch_id):
        value = config.get(dbi.depot_branch_id, option)
        if option == KEY_PARENT_BRANCH_ID:
            firstbranch = value
        elif option == KEY_PARENT_CHANGELIST:
            firstcl = value
        elif option.endswith(NTR('branch-id')):
            branch.append(option + ':' + value)
        elif option.endswith(NTR('changelist')):
            cl.append(option + ':' + value)

    branch = p4gf_util.alpha_numeric_sort(branch)
    cl     = p4gf_util.alpha_numeric_sort(cl)

    if firstbranch and firstcl:
        dbi.parent_depot_branch_id_list.append(firstbranch)
        dbi.parent_changelist_list.append(firstcl)

    for i in range(len(branch)):
        dbi.parent_depot_branch_id_list.append(branch[i].split(':')[1])
        dbi.parent_changelist_list.append(cl[i].split(':')[1])

    return dbi
                        # pylint:enable=W0212

class DepotBranchInfoIndex:
    '''
    Hash id->info and root->info
    '''

    def __init__(self):
        self.by_id   = {}
        self.by_root = {}

    def add(self, depot_branch_info):
        '''
        Add to our indices.
        '''
        self.by_id  [depot_branch_info.depot_branch_id] = depot_branch_info
        self.by_root[depot_branch_info.root_depot_path] = depot_branch_info

    def find_depot_branch_id(self, depot_branch_id):
        '''Seek.'''
        return self.by_id.get(depot_branch_id)

    def find_root_depot_path(self, depot_root):
        '''Return DepotBranchInfo whose root exactly matches depot_root'''
        return self.by_root.get(depot_root)

    def find_depot_path(self, depot_path):
        '''
        Return DepotBranchInfo whose root prefixes depot_path.

        O(N dbi) scan of entire list of DepotBranchInfo instances across
        all repos ever. Called by code that discovers depot branches
        that come from some other repo, but contribute to this repo.
        '''
        # Passed us just a root with no trailing delimiter? The loop below
        # won't find a match, but our dict lookup will (and quickly).
        r = self.find_root_depot_path(depot_path)
        if r:
            return r

        for dbi in self.by_id.values():
            if depot_path.startswith(dbi.root_depot_path + '/'):
                return dbi
        return None

    def find_ancestor_change_num(self, child_dbi, ancestor_dbid):
        '''
        If either child_dbi or one of its ancestors lists ancestor_dbi
        as a parent, return the changelist associated with ancestor_dbi.

        If no depot branch lists ancestor_dbi as a parent, return None.
        '''
        seen         = {child_dbi.depot_branch_id}    # set
        parent_id_q  = copy.copy(child_dbi.parent_depot_branch_id_list)
        change_num_q = copy.copy(child_dbi.parent_changelist_list     )

        # Special case: None or 'None' acceptable spellings of
        # "fully populated Perforce"
        if ancestor_dbid in [None, 'None']:
            match_list = [None, 'None']
        else:
            match_list = [ancestor_dbid]

        while parent_id_q:
            parent_id  = parent_id_q. pop(0)
            change_num = change_num_q.pop(0)

            # Found a winner.
            if parent_id in match_list:
                return change_num

            # Add this parent's own parents to our list of ancestors to check.
            if parent_id not in seen:
                seen.add(parent_id)
                parent_dbi = self.find_depot_branch_id(parent_id)
                if parent_dbi:
                    parent_id_q .extend(parent_dbi.parent_depot_branch_id_list)
                    change_num_q.extend(parent_dbi.parent_changelist_list     )

        # Ran out of all ancestors without ever finding ancestor_dbid.
        return None

    def find_fully_populated_change_num(self, dbi):
        '''
        Either this depot branch, or one of its ancestors has "None" listed as
        a parent: that depot branch is based on a fully populated Perforce
        hierarchy, at some changelist. Return that changelist number.

        Either succeeds or raises exception.
        '''
        cl_num = self.find_ancestor_change_num(dbi, None)
        if cl_num:
            return cl_num

        # Ran out of all ancestors without ever finding None. This is a rare but
        # possible lightweight depot branch with no fully populated ancestor.
        return None

    def depot_file_list_to_depot_branch_list(self, depot_file_list):
        '''
        Return a list of the few DepotBranchInfo objects whose roots contain
        the many given depot_file paths.
        '''
        dfl = sorted(depot_file_list) # Sorting increases chance that we'll
        last_dbi = None               # re-hit the same depot branch over and over

        dbi_set = set()

        for depot_file in dfl:
            # +++ No need to search if this depot_file is
            # +++ in the same branch as previous depot_file.
            if last_dbi and last_dbi.contains_depot_file(depot_file):
                continue
            dbi = self.find_depot_path(depot_file)
            if dbi:
                dbi_set.add(dbi)
                last_dbi = dbi
        return list(dbi_set)


# "static initializer" time
p4gf_util.test_vars_apply() # Honor sequential UUID option


# self.file_deletion elements.
FileDeletion = namedtuple('FileDeletion', ['change_num', 'depot_path'])

def file_deletion_to_line(fd):
    '''
    Convert a FileDeletion to a single line of text, suitable for storage
    in a branch-info file.
    '''
    return NTR('{change_num} {depot_path}')  \
           .format( change_num = fd.change_num
                  , depot_path = fd.depot_path )

def file_deletion_from_line(line):
    '''
    Convert a single line of text to a FileDeletion.
    '''
    i = line.find(' ')
    return FileDeletion( change_num = line[:i]
                       , depot_path = line[1+i:] )
