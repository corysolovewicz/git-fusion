#! /usr/bin/env python3.3
'''FilelogCache'''

import logging

LOG = logging.getLogger('p4gf_copy_to_git').getChild('filelog_cache')


class FilelogCache:
    """a cache of p4 filelog results

    Each item in the cache is the result of running p4 filelog @change
    Since the size of these items can vary considerably, it is not sufficient
    to just fix the cache size by the number of such items.  Instead, the
    sizes of the items are summed to determine the total cache size.

    Since the filelog result is frequently empty, such items are tracked
    separately and without any caching limit due to the minimal memory
    requirement.
    """

    MAX_SIZE = 1000000

    def __init__(self, p2g):
        self.p2g        = p2g
        self.empties    = set()
        self.nonempties = {}
        self.sizeof     = 0
        self.hits       = 0
        self.misses     = 0
        self.sizeof_discarded = 0

    def __del__(self):
        LOG.debug("FilelogCache hit rate: {} ({}/{}), discarded: {}"
                  .format(self.hits * 100 / (self.hits + self.misses),
                          self.hits,
                          self.hits + self.misses,
                          self.sizeof_discarded))

    # pylint: disable=W0212
    def get(self, changenum):
        '''return filelog result for changenum
        returns cached value, if possible; else runs filelog and
        adds result to cache
        '''
        if changenum in self.empties:
            self.hits += 1
            return ([], [])
        r = self.nonempties.get(changenum, None)
        if r:
            self.hits += 1
            return (r[0], r[1])

        self.misses += 1
        r = self.p2g._calc_filelog_to_integ_source_list(changenum)
        if len(r[0]):
            while self.sizeof and (self.sizeof + r[2] > self.MAX_SIZE):
                LOG.debug3('_filelog_cache overweight: {}'.format(self.sizeof + r[2]))
                (_, v) = self.nonempties.popitem()
                self.sizeof -= v[2]
                self.sizeof_discarded += v[2]
            self.nonempties[changenum] = r
            self.sizeof += r[2]
        else:
            self.empties.add(changenum)
        return (r[0], r[1])
    # pylint: enable=W0212
