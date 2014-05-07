#! /usr/bin/env python3.3
"""
Garbage collection utilities.
"""

import logging
import gc
import inspect
import operator
import random

import p4gf_log

LOG = p4gf_log.for_module()

# pylint:disable=F0401
# pylint "Unable to import"
_Pympler = False
try:
    # https://pypi.python.org/pypi/Pympler
    # c.f. http://pythonhosted.org/Pympler/
    from pympler import summary
    from pympler.util import stringutils
    _Pympler = True
except ImportError:
    pass
_ObjGraph = False
try:
    # http://mg.pov.lt/objgraph/
    import objgraph
    _ObjGraph = True
except ImportError:
    pass
# pylint:enable=F0401

# pylint:disable=W9903
# non-gettext-ed string
# This is a debugging script, no L10N required.

def init_gc():
    """
    Enable garbage collection debugging, if and only if DEBUG2 is enabled
    for this module.
    """
    if LOG.isEnabledFor(logging.DEBUG2):
        # No need for printing tons of debugging messages, just preserve
        # the unreachable garbage so we can report on it.
        gc.set_debug(gc.DEBUG_SAVEALL)
        th = gc.get_threshold()
        LOG.debug2("gc.get_threshold() => {}, {}, {}".format(th[0], th[1], th[2]))


def _log_summary(objects, limit=15):
    """
    Log (DEBUG3) a summary of the given list of objects based on type and size.
    """
    if not isinstance(limit, int):
        raise RuntimeError("limit must be an integer")
    if not objects:
        return
    # pylint:disable=E0602
    # pylint "Undefined variable"
    rows = summary.summarize(objects)
    try:
        # sort on the total size rather than object count
        keyf = lambda e: e[2]
        rows.sort(key=keyf, reverse=True)
        LOG.debug3("{0: >45}{1: >10}{2: >15}".format('object type', 'count', 'total size'))
        for row in rows[:limit]:
            size = stringutils.pp(row[2])
            LOG.debug3("{0: >45}{1: >10}{2: >15}".format(row[0][-45:], row[1], size))
    finally:
        # clear cyclic references to frame
        del rows
    # pylint:enable=E0602


def process_garbage(label=''):
    """
    This function does nothing if DEBUG2 is not enabled for this module.
    Otherwise, perform garbage collection, then report the number of
    uncollectable garbage objects. If DEBUG3 is enabled, print a summary
    of the uncollectable objects.

    The references in gc.garbage are deleted so as to rebuild the garbage
    list on subsequent calls.

    The label argument is used solely for logging, and may be left undefined
    to print a generic message.
    """
    if LOG.isEnabledFor(logging.DEBUG2):
        ct = gc.get_count()
        LOG.debug2("gc.get_count() => {}, {}, {}".format(ct[0], ct[1], ct[2]))
        LOG.debug2("collecting garbage {}...".format(label))
        # Note that any weak references will likely be purged as a result.
        gc.collect()
        LOG.debug2("{} uncollectable garbage objects".format(len(gc.garbage)))
        try:
            if LOG.isEnabledFor(logging.DEBUG3) and _Pympler:
                _log_summary(gc.garbage)
        finally:
            # Clear references to the garbage
            del gc.garbage[:]
        if LOG.isEnabledFor(logging.DEBUG3) and _ObjGraph:
            # Have to get the objects ourselves and free them, otherwise count() leaks.
            all_objects = gc.get_objects()
            try:
                types = ['Assign', 'Branch', 'CommitChange', 'CommitDetails', 'ObjectType',
                         'ObjectTypeList']
                for o_type in types:
                    count = objgraph.count(o_type, all_objects)
                    LOG.debug3("objgraph.count: {} {} objects".format(count, o_type))
            finally:
                del all_objects


# pylint:disable=W0102
# pylint 'Dangerous default value {} as argument'
def report_growth(label='', limit=20, peak_stats={}):
    """
    Using the objgraph module, report the growth of objects since the last call.
    """
    if LOG.isEnabledFor(logging.DEBUG3) and _ObjGraph:
        # Tried and failed to redirect stdout to get this output in the
        # log, so copying the entire show_growth() function here just to
        # write to the log.
        LOG.debug3("object growth {}".format(label))
        gc.collect()
        # Have to get the objects ourselves and free them, otherwise typestats() leaks.
        all_objects = gc.get_objects()
        try:
            stats = objgraph.typestats(all_objects)
            deltas = {}
            for name, count in stats.items():
                old_count = peak_stats.get(name, 0)
                if count > old_count:
                    deltas[name] = count - old_count
                    peak_stats[name] = count
            deltas = sorted(deltas.items(), key=operator.itemgetter(1), reverse=True)
            if limit:
                deltas = deltas[:limit]
            if deltas:
                width = max(len(name) for name, count in deltas)
                for name, delta in deltas:
                    LOG.debug3('%-*s%9d %+9d' % (width, name, stats[name], delta))
        finally:
            del all_objects
# pylint:enable=W0102


def report_objects(label=''):
    """
    This function does nothing if DEBUG2 is not enabled for this module.
    Otherwise, collect garbage and report the number of remaining objects
    on the heap. If DEBUG3 is enabled, generate and log a summary of the
    objects on the heap.
    """
    if LOG.isEnabledFor(logging.DEBUG2):
        LOG.debug2("Objects remaining on heap {}...".format(label))
        gc.collect()
        all_objects = gc.get_objects()
        try:
            LOG.debug2("{} objects on the heap".format(len(all_objects)))
            if LOG.isEnabledFor(logging.DEBUG3) and _Pympler:
                _log_summary(all_objects, limit=30)
        finally:
            # clear cyclic references to frame
            del all_objects


def is_hashable(o):
    """
    Test if hash() on the object works or not.
    """
    try:
        hash(o)
    except TypeError:
        return False
    return True


def backref_objects_by_type(obj_type):
    """
    Find all of the objects by a given type and randomly graph the
    back-references for a small number of instances.
    """
    if LOG.isEnabledFor(logging.DEBUG3) and _ObjGraph:
        #
        # Any nested function will hold onto some local state and leak.
        # Since gc.get_objects() contains the very frame running this code,
        # you get a circular reference.
        #
        gc.collect()
        all_objects = gc.get_objects()
        try:
            objects = objgraph.by_type(obj_type, all_objects)
        finally:
            del all_objects
        LOG.debug3("{} objects of type {} on heap".format(len(objects), obj_type))
        if objects:
            try:
                obj = random.choice(objects)
            finally:
                del objects
            # prune the unhashables from the garbage list to avoid errors with set()
            garbage = [o for o in gc.garbage if is_hashable(o)]
            try:
                ### this seems to interfere with the measurements...
                chain = objgraph.find_backref_chain(obj, inspect.ismodule, extra_ignore=garbage)
            finally:
                del garbage
                # Delete after calling objgraph, which invokes gc.collect()
                del gc.garbage[:]
            try:
                LOG.debug3("{} chain for {}".format(obj_type, obj))
                for link in chain:
                    LOG.debug3("chain link: {}: {}".format(type(link), str(link)[:240]))
            finally:
                del chain


def sizeof_objects_by_type(obj_type):
    """
    Find objects whose type names match that which is given, printing a summary
    of memory usage.
    """
    if LOG.isEnabledFor(logging.DEBUG3) and _ObjGraph:
        gc.collect()
        try:
            all_objects = gc.get_objects()
            try:
                objects = objgraph.by_type(obj_type, all_objects)
            finally:
                del all_objects
            if objects:
                LOG.debug3("sizeof_objects_by_type for {}".format(obj_type))
                _log_summary(objects, limit=30)
        finally:
            del gc.garbage[:]
