#! /usr/bin/env python3.3
'''
BranchFilesCache
'''
from collections import deque, namedtuple
import logging
import p4gf_util

LOG = logging.getLogger(__name__)

_MAX = 10

# Does this cache help? Barely.
#
# Pusing android-libhardware's 768 commits:
#
# cache  miss   hit wallclock
#  size count count duration
# ----- ----- ----- -----------
# 10000  1185  1018 84s 83s 82s avg=83.0s
#   100  1208   995 85s 85s 83s avg=84.3s
#    10  1358   845 81s 85s 82s avg=82.7s  tiny 3% savings
#     0  2203     0 85s 86s 86s avg=85.7s
#
# The results are noisy enough that even that 3% savings is suspect.
# But avoiding 845 out of 2203 'p4 files' calls to the server? Yeah,
# measurable or not, that's worth at least a 10-deep cache.

class BranchFilesCache:
    '''
    Generic cache of 'p4 files' in some branch at some changelist.

    G2PMatrix runs enough duplicate 'p4 files //branch-client/...@nn' in close
    temporal proximity that it could benefit from a small bounded cache.
    '''
    def __init__(self):
        self.cache = deque(maxlen=_MAX)    # of CacheLine
        self._hit_ct  = 0
        self._miss_ct = 0

    def files_at(self, ctx, branch, change_num):
        '''
        Fetch files in branch at change and return result list.
        '''
        result_list = self._find(branch, change_num)
        if not result_list:
            self._miss_ct += 1
            LOG.debug2('{branch}@{change} miss {ct}'
                       .format( branch  = p4gf_util.abbrev(branch.branch_id)
                              , change  = change_num
                              , ct      = self._miss_ct ))
            result_list = self._fetch(ctx, branch, change_num)
            self._insert(branch, change_num, result_list)
        else:
            self._hit_ct += 1
            LOG.debug2('{branch}@{change} hit  {ct}'
                       .format( branch  = p4gf_util.abbrev(branch.branch_id)
                              , change  = change_num
                              , ct      = self._hit_ct ))
        return result_list

    @staticmethod
    def _fetch(ctx, branch, change_num):
        '''
        Run 'p4 files' and return results
        '''
        with ctx.switched_to_branch(branch):
            return ctx.p4run([ 'files'
                             , ctx.client_view_path(change_num) ])

    def _find(self, branch, change_num):
        '''
        Find a CacheLine with matching path and return its result_list.
        Or return None if not found.
        '''
        # Never cache results for temp branch views that lack a permanent
        # branch_id: a branch_id of None is used for multiple branch views.
        if not branch.branch_id:
            return None

        for cl in self.cache:
            if (    cl.branch           == branch.branch_id
                and cl.change_num       == change_num):
                return cl.result_list
        return None

    def _insert(self, branch, change_num, result_list):
        '''
        Add a CacheLine for path + result_list.

        Assumes we don't already have such a line.
        '''
        self.cache.appendleft(CacheLine( branch      = branch.branch_id
                                       , change_num  = change_num
                                       , result_list = result_list ))

CacheLine = namedtuple('CacheLine', ['branch', 'change_num', 'result_list'])
