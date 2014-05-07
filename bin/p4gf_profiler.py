#! /usr/bin/env python3.3
"""A simple profiling timer class for timing sections of code.

To use:

with Timer('A'):
    do work
    with Timer('B'):
        do work

with Timer('C'):
    do work
    with Timer('B'):
        do work

At exit, a debug log entry will be produced:

A                    : 0.2000 seconds
  self time          : 0.1000 seconds
  B                  : 0.1000 seconds
C                    : 0.3000 seconds
  self time          : 0.1000 seconds
  B                  : 0.2000 seconds

Restrictions:
  Timer names must not contain '.'.
  Don't try to use class _Timer directly.

"""

import atexit
import time
import logging
import sys

# pylint:disable=W9903
# non-gettext-ed string
# debugging module, no translation required.
LOG = logging.getLogger(__name__)

_ACTIVE_TIMERS = []
_TIMERS = {}
_INDENT = 2
_SEP = '.'


class _Timer:

    """Simple class for timing code."""

    def __init__(self, name, top_level):
        self.name = name
        self.top_level = top_level
        self.time = 0
        self.start = 0
        self.active = False

    def __float__(self):
        return self.time

    def __enter__(self):
        assert not self.active
        assert not _ACTIVE_TIMERS or self.name.startswith(_ACTIVE_TIMERS[-1].name)
        self.active = True
        _ACTIVE_TIMERS.append(self)
        self.start = time.time()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        assert self.active
        assert _ACTIVE_TIMERS[-1] == self
        delta = time.time() - self.start
        _ACTIVE_TIMERS.pop()
        self.active = False
        self.time += delta

    def is_child(self, t):
        '''is t a direct child of this timer?'''
        if t == self:
            return False
        if not t.name.startswith(self.name + _SEP):
            return False
        if _SEP in t.name[len(self.name)+len(_SEP):]:
            return False
        return True

    def children(self):
        '''list of timers nested within this timer'''
        return [t for t in _TIMERS.values() if self.is_child(t)]

    def child_time(self):
        '''sum of times of all nested timers'''
        return sum([t.time for t in self.children()])

    def do_str(self, indent):
        """helper function for str(), recursively format timer values"""
        items = [" " * indent + "{:25}".format(self.name.split(_SEP)[-1]) + " " * (10 - indent) +
                 ": {:8.4f} seconds".format(self.time)]
        ctimers = sorted(self.children(), key=lambda t: t.name)
        if ctimers:
            indent += _INDENT
            self_time = self.time - self.child_time()
            items.append(" " * indent + "{:25}".format("self time") + " " * (10 - indent) +
                         ": {:8.4f} seconds".format(self_time))
            for t in ctimers:
                items.append(t.do_str(indent))
        return "\n".join(items)

    def __str__(self):
        return self.do_str(0)


#pylint:disable=C0103
def Timer(name):
    """Create and return a timer."""
    assert not _SEP in name
    if _ACTIVE_TIMERS:
        assert not name in _ACTIVE_TIMERS[-1].name.split(_SEP)
        full_name = _ACTIVE_TIMERS[-1].name + _SEP + name
    else:
        full_name = name
    if not full_name in _TIMERS:
        _TIMERS[full_name] = _Timer(full_name, full_name == name)
    return _TIMERS[full_name]


@atexit.register
def Report():
    """Log all recorded timer activity."""
    top_timers = sorted([t for t in _TIMERS.values() if t.top_level], key=lambda t: t.name)
    LOG.debug("\n".join(["Profiler report for {}".format(sys.argv)]
                        + [str(t) for t in top_timers]))
