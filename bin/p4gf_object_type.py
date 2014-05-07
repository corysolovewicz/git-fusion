#! /usr/bin/env python3.3
'''Return the type and extra info of an object stored in the
//P4GF_DEPOT/objects/... hierarchy.

Checks only the local filesystem for .git-fusion/...
'''

import binascii
import bisect
from collections import namedtuple, MutableSequence, Sequence
import logging
import random
import re

import p4gf_const
from   p4gf_l10n      import _, NTR
import p4gf_log
import p4gf_util

LOG = p4gf_log.for_module()

# Values for ObjectType.type. Must match types for 'git cat-file -t <type>'
COMMIT = "commit"
# Character that is used to delineate branch ID in commit object path
# (allowed in depot paths but not in view names, making it very useful).
BRANCH_SEP = ','

# Details for the commit object stored in the cache.
CommitDetails = namedtuple('CommitDetails', ['changelist', 'viewname', 'branch_id'])
# Regular expression for parsing cached object filepath.
OBJPATH_COMMIT_REGEX = re.compile("/objects/repos/(?P<repo>[^/]+)/commits/(?P<slashed_sha1>[^-]+)"
                                  "-(?P<branch_id>[^,]+),(?P<changelist>\\d+)")
OBJPATH_TREE_REGEX = re.compile("/objects/trees/(?P<slashed_sha1>[^-]+)")
KEY_LAST_REGEX = re.compile("git-fusion-index-last-(?P<repo>[^,]+),(?P<branch_id>(.*))")
VALUE_LAST_REGEX = re.compile("(?P<changelist>\\d+),(?P<sha1>\\w{40})")
KEY_BRANCH_REGEX = re.compile("git-fusion-index-branch-(?P<repo>[^,]+),(?P<changelist>\\d+)," +
                              "(?P<branch_id>(.*))")


def slashify_sha1(sha1):
    '''
    Convert a SHA1 to the path form for use in Perforce.
    For instance, 60eaf72224a34f592636271fa957b6c4acaee5f3
    becomes 60/ea/f72224a34f592636271fa957b6c4acaee5f3
    which can then be used to build a file path.
    '''
    if sha1 == '*':
        return '*/*/*'
    return sha1[:2] + "/" + sha1[2:4] + "/" + sha1[4:]

def _commit_p4_path(commit_sha1, changelist, repo, branch_id):
    '''
    Return depot path to a commit
    '''
    if commit_sha1 == '*':
        assert branch_id == '*' and changelist == '*'
        return (NTR('{objects_root}/repos/{repo}/commits/...')
                .format(objects_root=p4gf_const.objects_root(),
                        repo=repo))
    if branch_id == '*':
        assert changelist == '*'
        return (NTR('{objects_root}/repos/{repo}/commits/{slashed}-*')
                .format(objects_root=p4gf_const.objects_root(),
                        repo=repo,
                        slashed=slashify_sha1(commit_sha1)))
    return (NTR('{objects_root}/repos/{repo}/commits/{slashed}-{branch_id},{change}')
            .format(objects_root=p4gf_const.objects_root(),
                    repo=repo,
                    slashed=slashify_sha1(commit_sha1),
                    branch_id=branch_id,
                    change=changelist))

class TreeCache:
    '''
    Keeps track of which tree objects exist in perforce.
    '''
    MAX_SIZE = 10000

    def __init__(self):
        self._tree_cache = [None]*TreeCache.MAX_SIZE
        self._tree_cache_size = 0

    def clear(self):
        '''
        remove all elements from this cache
        '''
        self._tree_cache = [None]*TreeCache.MAX_SIZE
        self._tree_cache_size = 0

    def _tree_cache_insert(self, j, bsha1):
        '''
        Insert an entry in the cache of tree sha1 values.
        If the cache is already full, remove one entry at random first.
        '''
        # NOP if already in list
        if j < self._tree_cache_size and self._tree_cache[j] == bsha1:
            return

        # just insert if not at capacity
        if self._tree_cache_size < TreeCache.MAX_SIZE:
            self._tree_cache = self._tree_cache[:j] \
                             + [bsha1] \
                             + self._tree_cache[j:TreeCache.MAX_SIZE-1]
            self._tree_cache_size += 1
            return

        # remove a random entry and insert new entry
        i = random.randrange(TreeCache.MAX_SIZE)
        if i == j:
            self._tree_cache[i] = bsha1
        elif i < j:
            self._tree_cache = self._tree_cache[:i] \
                             + self._tree_cache[i+1:j] \
                             + [bsha1] + self._tree_cache[j:]
        else:
            self._tree_cache = self._tree_cache[:j] \
                             + [bsha1] \
                             + self._tree_cache[j:i] \
                             + self._tree_cache[i+1:]

    def tree_exists(self, p4, sha1):
        '''
        Returns true if sha1 identifies a tree object in the //P4GF_DEPOT/objects/... hierarchy.
        '''
        # convert to binary rep for space savings
        bsha1 = binascii.a2b_hex(sha1)
        # test if already in cache
        j = bisect.bisect_left(self._tree_cache, bsha1, hi=self._tree_cache_size)
        if j < self._tree_cache_size and self._tree_cache[j] == bsha1:
            LOG.debug2('tree cache hit for {}'.format(sha1))
            return True
        # not in cache, check server
        LOG.debug2("tree cache miss for {}".format(sha1))
        LOG.debug("fetching tree objects for {}".format(sha1))
        path = ObjectType.tree_p4_path(sha1)
        r = [f for f in _run_p4files(p4, path)]
        if not len(r) == 1:
            return False
        m = OBJPATH_TREE_REGEX.search(r[0])
        if not m:
            return False
        found_sha1 = m.group('slashed_sha1').replace('/', '')
        if not sha1 == found_sha1:
            return False
        self._tree_cache_insert(j, bsha1)
        return True

class ChangeToCommitCache:
    '''
    Maintains a limited size cache of changelist-to-commit mappings
    for different branches.
    '''
    MAX_SIZE = 10000

    def __init__(self):
        self._branches = {}
        self._count = 0

    def clear(self):
        '''
        remove all elements from this cache
        '''
        self._branches = {}
        self._count = 0

    def _remove_one(self):
        '''
        remove a randomly selected element
        '''
        assert self._count == ChangeToCommitCache.MAX_SIZE
        self._count -= 1
        i = random.randrange(ChangeToCommitCache.MAX_SIZE)
        for j in self._branches.values():
            if len(j) <= i:
                i -= len(j)
                continue
            key = list(j.keys())[i]
            del j[key]

    def append(self, changelist, branch_id, sha1):
        '''
        add an entry mapping changelist on branch_id to commit sha1
        '''
        if self._count == ChangeToCommitCache.MAX_SIZE:
            self._remove_one()

        if not branch_id in self._branches:
            self._branches[branch_id] = {}
        self._branches[branch_id][changelist] = sha1
        self._count += 1

    def get(self, changelist, branch_id):
        '''
        return matching (branch_id, commit_sha1) if in cache, else None
        if branch_id is None, returns first matching element, or None
        '''
        if not branch_id:
            return self._get_any_branch(changelist)
        branch_commits = self._branches.get(branch_id)
        if not branch_commits:
            return None
        sha1 = branch_commits.get(changelist)
        if not sha1:
            return None
        return branch_id, sha1

    def _get_any_branch(self, changelist):
        '''
        return first matching (branch_id,commit_sha1) or None
        '''
        for branch_id, changes in self._branches.items():
            if changelist in changes:
                return branch_id, changes[changelist]
        return None

class ObjectTypeCache(MutableSequence):
    """
    Maintains a limited number of sorted ObjectTypeList objects. When more
    objects than MAX_LEN have been appended, a random selection of elements
    will be removed to make room for any new additions. The objects are
    sorted according to their natural order.

    Insertion into the sequence is always done in a sorted manner (i.e.
    index is ignored). Because the list is backed by an array, insertions
    incur a O(N) complexity cost.
    """
    MAX_LEN = 1000

    def __init__(self):
        self.__count = 0
        self.__array = [None] * ObjectTypeCache.MAX_LEN

    def __len__(self):
        return self.__count

    def __contains__(self, value):
        if isinstance(value, str):
            value = ObjectTypeList(value, None)
        i = bisect.bisect_left(self.__array, value, hi=self.__count)
        if i != self.__count and self.__array[i] == value:
            return True
        return False

    def index(self, value):
        i = bisect.bisect_left(self.__array, value, hi=self.__count)
        if i != self.__count and self.__array[i] == value:
            return i
        raise ValueError

    def __getitem__(self, index):
        if index == self.__count or abs(index) > self.__count:
            # pylint disable R0801 (similar lines) does not work?
            raise IndexError(_('index out of bounds'))
        if index < 0:
            # special case for negative indices
            return self.__array[self.__count + index]
        return self.__array[index]

    def get(self, sha1):
        """Retrieve item based on the given SHA1 value. Returns None if not found."""
        value = ObjectTypeList(sha1, None)
        i = bisect.bisect_left(self.__array, value, hi=self.__count)
        if i != self.__count and self.__array[i] == value:
            return self.__array[i]
        return None

    def __setitem__(self, index, value):
        raise IndexError(_('only append operations allowed'))

    def __delitem__(self, index):
        # pylint disable R0801 (similar lines) does not work?
        raise IndexError(_('special list type, no deletions'))

    def __str__(self):
        return str(self.__array[:self.__count])

    def clear(self):
        """Remove all elements from this cache."""
        self.__count = 0
        for i in range(len(self.__array)):
            self.__array[i] = None

    def insert(self, index, value):
        raise IndexError(_('only append operations allowed'))

    def append(self, value):
        """
        Add the given value to the list, maintaining length and order.
        """
        idx = bisect.bisect_left(self.__array, value, hi=self.__count)
        # pylint disable R0801 (similar lines) does not work?
        if self.__count == len(self.__array):
            # reached size limit, remove a random element
            mark = random.randrange(self.__count)
            if idx == self.__count:
                # inserting beyond the end
                idx -= 1
            if mark < idx:
                for i in range(mark, idx - 1):
                    self.__array[i] = self.__array[i + 1]
            else:
                # pylint disable R0801 (similar lines) does not work?
                for i in range(mark, idx, -1):
                    self.__array[i] = self.__array[i - 1]
            self.__array[idx] = value
        else:
            # there is room enough for more
            if idx == self.__count:
                # goes at the end
                self.__array[idx] = value
                # pylint disable R0801 (similar lines) does not work?
            else:
                # goes somewhere other than the end
                for i in range(self.__count, idx, -1):
                    self.__array[i] = self.__array[i - 1]
                # pylint disable R0801 (similar lines) does not work?
                self.__array[idx] = value
            self.__count += 1


# pylint:disable=R0924
# pylint does not realize this is an immutable sequence
class ObjectTypeList(Sequence):
    """
    Immutable list of ObjectType instances. Compares to other instances using sha1.
    Simply wraps an instance of the built-in list type.
    """

    def __init__(self, sha1, ot_list):
        self.sha1 = sha1
        self.ot_list = ot_list

    def __hash__(self):
        return hash(self.sha1)

    def __eq__(self, other):
        return self.sha1 == other.sha1

    def __ne__(self, other):
        return self.sha1 != other.sha1

    def __ge__(self, other):
        return self.sha1 >= other.sha1

    def __gt__(self, other):
        return self.sha1 > other.sha1

    def __le__(self, other):
        return self.sha1 <= other.sha1

    def __lt__(self, other):
        return self.sha1 < other.sha1

    def __getitem__(self, index):
        return self.ot_list[index]

    def __len__(self):
        return len(self.ot_list)

    def __str__(self):
        return "ObjectTypeList[{}]".format(self.sha1[:7])
# pylint:enable=R0924


class ObjectType:
    '''
    A single sha1 maps to a single type: commit, tree, or blob.

    If commit, maps to 1 or more (changlist, view_name) tuples.

    details is an instance of CommitDetails
    '''
    # Cache of object details: keys are SHA1, values are ObjectType lists.
    object_cache = ObjectTypeCache()
    tree_cache = TreeCache()
    last_commits_cache = {}
    last_commits_cache_complete = False
    change_to_commit_cache = ChangeToCommitCache()

    def __init__(self, sha1, otype, details=None):
        self.sha1 = sha1
        self.type = otype
        self.details = details

    def __eq__(self, b):
        return (    self.sha1     == b.sha1
                and self.type    == b.type
                and self.details == b.details)

    def __ne__(self, b):
        return (   self.sha1    != b.sha1
                or self.type    != b.type
                or self.details != b.details)

    def __str__(self):
        return "{} {} {}".format(p4gf_util.abbrev(self.sha1), self.type, self.details)

    def __repr__(self):
        return str(self)

    @staticmethod
    def reset_cache():
        '''
        After gitmirror submits new ObjectCache files to Perforce, our cache
        is no longer correct.
        '''
        ObjectType.object_cache.clear()
        ObjectType.tree_cache.clear()
        ObjectType.last_commits_cache = {}
        ObjectType.last_commits_cache_complete = False
        ObjectType.change_to_commit_cache.clear()
        LOG.debug2("cache cleared")

    @staticmethod
    def commit_from_filepath(filepath):
        '''
        Take a (client or depot) file path and parse off the "xxx-commit-nnn" suffix.

        Return an ObjectType instance.
        '''
        LOG.debug("from_filepath({})".format(filepath))
        m = OBJPATH_COMMIT_REGEX.search(filepath)
        if not m:
            return None
        sha1       = m.group(NTR('slashed_sha1')).replace('/', '')
        repo       = m.group(NTR('repo'))
        changelist = m.group(NTR('changelist'))
        branch_id  = m.group(NTR('branch_id'))
        return ObjectType.create_commit(sha1, repo, changelist, branch_id)

    @staticmethod
    def create_commit(sha1, view_name, change_num, branch_id):
        '''
        Factory to create COMMIT ObjectType.
        '''
        return ObjectType(sha1, COMMIT, CommitDetails( change_num
                                                     , view_name
                                                     , branch_id ))

    @staticmethod
    def tree_p4_path(tree_sha1):
        '''
        Return depot path to a tree
        '''
        return (NTR('{objects_root}/trees/{slashed}')
                .format(objects_root=p4gf_const.objects_root(),
                        slashed=slashify_sha1(tree_sha1)))

    @staticmethod
    def commit_p4_path(ctx, commit):
        '''
        Return depot path to a commit
        '''
        return _commit_p4_path(commit.sha1,
                               commit.details.changelist,
                               ctx.config.view_name,
                               commit.details.branch_id)

    @staticmethod
    def tree_exists_in_p4(p4, sha1):
        '''
        Returns true if sha1 identifies a tree object in the //P4GF_DEPOT/objects/... hierarchy.
        '''
        return ObjectType.tree_cache.tree_exists(p4, sha1)

    @staticmethod
    def _load_last_commits_cache(ctx):
        '''
        If this is the first time called, load the cache of last commits
        '''
        if ObjectType.last_commits_cache_complete:
            return

        r = p4gf_util.p4run_logged(ctx.p4gf, ['counters', '-u', '-e',
                                   'git-fusion-index-last-{repo},*'.format(
                                   repo=ctx.config.view_name)])
        for rr in r:
            mk = KEY_LAST_REGEX.search(rr['counter'])
            if not mk:
                LOG.debug("ignoring unexpected p4 counter: {}".format(rr))
                continue
            mv = VALUE_LAST_REGEX.search(rr['value'])
            if not mv:
                LOG.debug("ignoring invalid p4 counter value: {}".format(rr))
            ObjectType.last_commits_cache[mk.group('branch_id')] = rr['value']
            LOG.debug2('last change,commit for branch {} is {}'
                       .format(mk.group('branch_id'), rr['value']))
        ObjectType.last_commits_cache_complete = True

    # pylint:disable=R0912
    # yes, simply horrible how complex this function is; some other day
    @staticmethod
    def last_change_for_branches(ctx, branch_ids, must_exist_local=False):
        '''
        Returns highest numbered change for all branches which exists in p4.

        Searches //P4GF_DEPOT/objects/... for commits and returns ObjectType
        for commit with highest change_number, or None if no matching commit.

        If must_exist_local is True, only commits which also exist in the
        repo are considered in the search.
        '''
        # if only one branch_id given, don't fetch them all
        if len(branch_ids) == 1:
            branch_id = branch_ids[0]
            if not branch_id in ObjectType.last_commits_cache:
                key = "git-fusion-index-last-{repo},{branch_id}".format(repo=ctx.config.view_name,
                                                                  branch_id=branch_id)
                r = p4gf_util.p4run_logged(ctx.p4gf, ['counters', '-u', '-e', key])
                if r:
                    ObjectType.last_commits_cache[branch_id] = r[0]['value']
            if not branch_id in ObjectType.last_commits_cache:
                return None
            change, sha1 = ObjectType.last_commits_cache[branch_id].split(',')
            if must_exist_local and not p4gf_util.sha1_exists(sha1):
                return None
            return ObjectType.create_commit(sha1,
                                            ctx.config.view_name,
                                            int(change),
                                            branch_id)

        # if more than one branch, load up all branches into the cache
        ObjectType._load_last_commits_cache(ctx)
        highest = {}
        k = None
        for branch_id, v in ObjectType.last_commits_cache.items():
            if not branch_id in branch_ids:
                continue
            change, sha1 = v.split(',')
            if branch_id in highest:
                if int(change) > highest[branch_id][0]:
                    if must_exist_local and not p4gf_util.sha1_exists(sha1):
                        continue
                    highest[branch_id] = (int(change), sha1)
            elif not branch_ids or branch_id in branch_ids:
                if must_exist_local and not p4gf_util.sha1_exists(sha1):
                    continue
                highest[branch_id] = (int(change), sha1)
            else:
                continue
            if not k or int(change) > highest[k][0]:
                k = branch_id
        if not k:
            return None
        return ObjectType.create_commit(highest[k][1],
                                        ctx.config.view_name,
                                        highest[k][0],
                                        k)
    # pylint:enable=R0912

    @staticmethod
    def commits_for_sha1(ctx, sha1, branch_id=None):
        '''
        Returns ObjectTypeList of matching commits.
        If branch_id is specified, result will contain at most one match.
        '''
        assert sha1
        otl = ObjectType.object_cache.get(sha1)
        if otl:
            otl = otl.ot_list
        else:
            path = _commit_p4_path(sha1, '*', ctx.config.view_name, '*')
            otl = _otl_for_p4path(ctx.p4gf, path)
            ObjectType.object_cache.append(ObjectTypeList(sha1, otl))
        if not branch_id:
            return otl
        return [ot for ot in otl if ot.details.branch_id == branch_id]

    @staticmethod
    def change_for_sha1(ctx, sha1, branch_id=None):
        '''
        If a commit exists as specified, returns the change #, else None
        If no branch_id specified, returns highest change number of matching commits.
        '''
        if not sha1:
            return None
        otl = ObjectType.commits_for_sha1(ctx, sha1, branch_id)
        if len(otl):
            return max([int(ot.details.changelist) for ot in otl])
        return None

    @staticmethod
    def commit_for_change(ctx, change, branch_id=None):
        '''
        If a commit exists as specified, returns an ObjectType for the commit, else None
        If no branch_id specified, returns first found matching commit.
        '''
        if not change:
            return None

        # first, try cache
        from_cache = ObjectType.change_to_commit_cache.get(change, branch_id)
        if from_cache:
            return ObjectType.create_commit(from_cache[1],
                                            ctx.config.view_name,
                                            change,
                                            from_cache[0])

        # not in cache, use index to find commit(s)
        if not branch_id:
            branch_id = '*'
        key = "git-fusion-index-branch-{repo},{change},{branch}".format(repo=ctx.config.view_name,
                                                                  change=change,
                                                                  branch=branch_id)
        result_sha1 = None
        result_branch = None
        r = p4gf_util.p4run_logged(ctx.p4gf, ['counters', '-u', '-e', key])
        for rr in r:
            if not 'counter' in rr:
                continue
            m = KEY_BRANCH_REGEX.search(rr['counter'])
            found_branch = m.group('branch_id')
            found_sha1 = rr['value']
            ObjectType.change_to_commit_cache.append(change, found_branch, found_sha1)
            if not branch_id == '*':
                if not found_branch == branch_id:
                    continue
            result_sha1 = found_sha1
            result_branch = found_branch
        if not result_sha1:
            return None
        return ObjectType.create_commit(result_sha1, ctx.config.view_name, change, result_branch)

    @staticmethod
    def update_indexes(ctx, r):
        '''
        Call with result of submit to update indexes in p4 keys
        Ignore trees, but update for any commits.
        '''
        for rr in r:
            if not 'depotFile' in rr:
                continue
            depot_file = rr['depotFile']
            commit = ObjectType.commit_from_filepath(depot_file)
            if commit:
                ObjectType.update_last_change(ctx, commit)

    @staticmethod
    def update_last_change(ctx, commit):
        '''
        Update p4 key that tracks the last change on a branch
        '''
        # unconditionally add a counter mapping change -> commit sha1
        branch_id = commit.details.branch_id
        key = "git-fusion-index-branch-{repo},{change},{branch}".format(
            repo=commit.details.viewname, change=commit.details.changelist, branch=branch_id)
        p4gf_util.p4run_logged(ctx.p4gf, ['counter', '-u', key, commit.sha1])
        # only update last change counter if this commit has a higher change #
        if branch_id in ObjectType.last_commits_cache and\
           (int(ObjectType.last_commits_cache[branch_id].split(',')[0]) >
            int(commit.details.changelist)):
            return
        key = "git-fusion-index-last-{repo},{branch_id}".format(
            repo=commit.details.viewname, branch_id=branch_id )
        value = "{},{}".format(commit.details.changelist, commit.sha1)
        p4gf_util.p4run_logged(ctx.p4gf, ['counter', '-u', key, value])
        ObjectType.last_commits_cache[branch_id] = value

    def is_commit(self):
        '''
        Returns True if this object is a commit object, False otherwise.
        '''
        return COMMIT == self.type

    def applies_to_view(self, view_name):
        '''
        If we're a BLOB or TREE object, we apply to all view names. Yes.
        If we're a COMMIT object, we only apply to view names referenced in our details list.
        '''
        if COMMIT != self.type:
            return True
        match = self.view_name_to_changelist(view_name)
        return None != match

    def view_name_to_changelist(self, view_name):
        '''
        Return the matching Perforce changelist number associated with the given view_name.

        Only works for commit objects.

        Return None if no match.
        '''
        if self.details.viewname == view_name:
            return self.details.changelist
        return None

    def to_p4_client_path(self):
        '''
        Generate relative path to object in Perforce mirror, without the preceding
        depot path (e.g. //P4GF_DEPOT/).
        '''
        if self.type == 'tree':
            return "objects/trees/" + slashify_sha1(self.sha1)
        assert self.type == 'commit'
        return (NTR('objects/repos/{repo}/commits/{sha1}-{branch_id},{changenum}')
                .format(repo=self.details.viewname,
                        sha1=slashify_sha1(self.sha1),
                        branch_id=self.details.branch_id.replace('/', '-'),
                        changenum=self.details.changelist))

    def to_depot_path(self):
        '''
        Return path to this Git object as stored in Perforce.
        '''
        client_path = self.to_p4_client_path()
        if not client_path:
            return None
        return '//{}/{}'.format(p4gf_const.P4GF_DEPOT, client_path)

    @staticmethod
    def log_otl(otl, level=logging.DEBUG3, log=LOG):
        '''
        Debugging dump.
        '''
        if not log.isEnabledFor(level):
            return
        for ot in otl:
            log.log(level, repr(ot))

def _depot_path_to_commit_sha1(depot_path):
    '''
    Return just the sha1 portion of an commit object stored in our depot.
    '''
    m = OBJPATH_COMMIT_REGEX.search(depot_path)
    if not m:
        return None
    return m.group('slashed_sha1').replace('/', '')

def known_commit_sha1_list(ctx):
    '''
    Return a list of every known commit sha1 for the current repo.
    '''
    path = _commit_p4_path('*', '*', ctx.config.view_name, '*')
    return [_depot_path_to_commit_sha1(f) for f in _run_p4files(ctx.p4gf, path)]

def _otl_for_p4path(p4, path):
    '''
    Return list of ObjectType for files reported by p4 files <path>
    '''
    return [ot for ot in [ObjectType.commit_from_filepath(f) for
                          f in _run_p4files(p4, path)] if ot]

def _run_p4files(p4, path):
    '''
    Run p4 files on path and return depot paths of any files reported
    '''
    files = p4gf_util.p4run_logged(p4, ['files', path])
    return [f.get('depotFile') for f in files if isinstance(f, dict) and f.get('depotFile')]
