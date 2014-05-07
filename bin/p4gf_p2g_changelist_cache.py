#! /usr/bin/env python3.3
'''ChangelistCache'''

import logging
import sys

LOG = logging.getLogger('p4gf_copy_to_git').getChild('changelist_cache')

class ChangelistCache:
    '''a cache of p4 changes results

    Each item is either a P4Changelist or a string containing the path of
    a P4Changelist.  In the case of complete P4Changelist objects, files are
    not included since: 1) that takes too much space; and 2) they depend on
    the branch.

    The cache size is limited to MAX_SIZE.  When space is short, P4Changelist
    items will first be converted to paths, and eventually paths will be dropped.
    '''

    MAX_SIZE = 1000000

    def __init__(self, p2g):
        self.p2g                = p2g
        self.changes            = {}
        self.paths              = {}
        self.changenums         = set()
        self.sizeof_changes     = 0
        self.sizeof_paths       = 0
        self.hits               = 0
        self.misses             = 0
        self.sizeof_discarded   = 0

    def __del__(self):
        LOG.debug("ChangelistCache hit rate: {} ({}/{}), discarded: {}"
                  .format(self.hits * 100 / (self.hits + self.misses),
                          self.hits,
                          self.hits + self.misses,
                          self.sizeof_discarded))

    #pylint:disable=W0212
    def get(self, changenum):
        '''return P4Changelist for changenum'''
        cl = self.changes.get(changenum)
        if cl:
            self.hits += 1
            return cl
        self.misses += 1
        cl = self.p2g._get_changelist(changenum)
        self._insert(cl)
        return cl

    def get_path(self, changenum):
        '''return only the path of the P4Changelist for changenum'''
        cl = self.changes.get(changenum)
        if cl:
            self.hits += 1
            return self._nonempty_path(cl.path)
        path = self.paths.get(changenum)
        if not path == None:
            self.hits += 1
            return self._nonempty_path(path)
        self.misses += 1
        cl = self.p2g._get_changelist(changenum)
        self._insert(cl)
        return self._nonempty_path(cl.path)
    #pylint:enable=W0212

    def update(self, cl):
        '''If cl is already cached, update it.  Otherwise, insert it.
        If cache is near capacity, this may result in downgrading a cached
        P4Changelist to just a path.'''
        if cl.change in self.changes:
            del self.changes[cl.change]
        elif cl.change in self.paths:
            del self.paths[cl.change]
        self._insert(cl)

    def keys(self):
        '''Return list of change numbers we've seen.  It's possible a change
        number will be returned for which no other info is retained in the
        cache.'''
        return self.changenums

    @staticmethod
    def _nonempty_path(path):
        '''make sure path isn't empty'''
        if path:
            return path
        return '//...'

    def _insert(self, cl):
        '''add the changelist to the collection'''
        # Figure out if we should cache the whole object or just the path.
        # Once we start caching just paths, never cache complete objects again.
        if self.sizeof_paths:
            save_path = True
        else:
            sizeof = self._sizeof_change(cl)
            if sizeof + self.sizeof_changes + self.sizeof_paths > self.MAX_SIZE:
                save_path = True
            else:
                save_path = False
        if save_path:
            sizeof = sys.getsizeof(cl.path)

        # trim until it fits or there's nothing left to trim
        while ((self.sizeof_changes + self.sizeof_paths) and
               (sizeof + self.sizeof_changes + self.sizeof_paths > self.MAX_SIZE)):
            if self.sizeof_changes:
                chosen = self.changes.popitem()
                LOG.debug3("changelist-cache dropping change {}".format(chosen.change))
                sizeof_chosen = self._sizeof_change(chosen)
                self.sizeof_changes -= sizeof_chosen
                self.sizeof_discarded += sizeof_chosen
            elif self.sizeof_paths:
                chosen = self.paths.popitem()
                LOG.debug3("changelist-cache dropping path {}".format(chosen.change))
                self.sizeof_paths -= sys.getsizeof(chosen)
                self.sizeof_discarded += sys.getsizeof(chosen)

        if save_path:
            LOG.debug3("changelist-cache adding path {}".format(cl.change))
            self.paths[cl.change] = cl.path
        else:
            LOG.debug3("changelist-cache adding change {}".format(cl.change))
            self.changes[cl.change] = cl
        self.changenums.add(cl.change)

    @staticmethod
    def _sizeof_change(cl):
        '''calculate the size of a P4Changelist'''
        sizeof = (sys.getsizeof(cl.change) +
                  sys.getsizeof(cl.description) +
                  sys.getsizeof(cl.user) +
                  sys.getsizeof(cl.time) +
                  sys.getsizeof(cl.path))
        return sizeof
