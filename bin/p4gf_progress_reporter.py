#! /usr/bin/env python3.3
'''
Tools for telling the user what is going on.
'''

import math
import sys
import time

from   p4gf_l10n      import _, NTR

                        # pylint:disable=W9903
                        # non-gettext-ed string
                        # I don't want to pollute __all__ with NTR noise.
__all__ = ['increment', 'write', 'Determinate', 'Indeterminate']
                        # pylint:enable=W9903
                        #
                        # All of our human-visible strings from here on down are
                        # just "Perforce:" prefixes, and we don't translate
                        # our company name, so we could leave W9903 disabled.
                        # But just in case one day we add something that
                        # requires translation, reenable W9903 to catch it.


def increment(message):
    '''
    Update progress message.
    '''
    _instance().increment(message)

def write(message):
    '''
    Write a progress message without incrementing progress.
    '''
    _instance().write(NTR('Perforce: {}').format(message))

_INSTANCE_GLOBAL = None
_FLUSH_MINIMUM = 10

def _instance():
    '''
    Return a shared Reporter instance
    '''
    global _INSTANCE_GLOBAL
    if _INSTANCE_GLOBAL == None:
        _INSTANCE_GLOBAL = Single()
    return _INSTANCE_GLOBAL

class Reporter:
    '''base reporter'''
    def __init__(self):
        self.enabled                = True
        self.debug                  = False
        self.running_interval       = 0
        self.flush_interval         = 0

    def write(self, message):
        '''show a message
        If debugging, pause briefly after showing the message
        '''
        if not self.enabled:
            return

        _write(message)

        if self.running_interval >= self.flush_interval:
            _flush()
            self.running_interval = 0

        if self.debug:
            time.sleep(1)

class Single(Reporter):
    '''Spew status messages, one per line.'''
    def __init__(self):
        Reporter.__init__(self)

    def increment(self, message):
        '''show message on its own line'''
        self.write(_('Perforce: {}\n').format(message))

class Multi(Reporter):
    '''Show a sequence of messages on one line, each new one replacing the
    previous one.
    '''
    def __init__(self):
        Reporter.__init__(self)
        self.last_len = 0

    def __enter__(self):
        global _INSTANCE_GLOBAL
        _INSTANCE_GLOBAL = self
        return None

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.write('\n')
        try:
            sys.stderr.flush()
        except IOError:
            pass
        global _INSTANCE_GLOBAL
        _INSTANCE_GLOBAL = Single()
        return False  # False == do not squelch any current exception

    def write_over(self, message):
        '''write message on top of last written message
        message must begin with \r'''
        this_len = len(message)
        if this_len < self.last_len:
            self.write(message + ' '*(self.last_len - this_len))
        else:
            self.write(message)
        self.last_len = this_len

class Determinate(Multi):
    '''Write a sequence of related messages of known length with count
    and percent complete on each line.

    Each call to increment overwrites the message shown by the previous
    call.

    If incremented too many times, percent complete will remain at 100%
    '''
    def __init__(self, count):
        Multi.__init__(self)
        self.nominator     = 0
        self.denominator   = count
        self.flush_interval = _FLUSH_MINIMUM


    def increment(self, message):
        '''show message with count, percent complete'''
        self.nominator += 1
        self.running_interval += 1
        fmt = (_('\rPerforce: %3d%% (%{ct}d/%{ct}d) %s')
               .format(ct=_digit_count(self.denominator)))
        self.write_over(fmt % (self.percentage(),
                          self.nominator,
                          self.denominator,
                          message))

    def percentage(self):
        '''
        Return an integer 0..100

        Does range-check. n/0 = 0, 26/25 = 100.
        '''
        if not self.denominator:
            return 0

        if self.denominator <= self.nominator:
            return 100

        return int(  float(self.nominator) * 100.0
                   / float(self.denominator) )

class Indeterminate(Multi):
    '''write a sequence of messages of unknown length with count.'''
    def __init__(self):
        Multi.__init__(self)
        self.count = 0
        self.flush_interval = _FLUSH_MINIMUM

    def increment(self, message):
        '''show message with count'''
        self.count += 1
        self.running_interval += 1
        self.write_over(_('\rPerforce: %s: %d') % (message, self.count))

def _digit_count(n):
    '''
    How many digits?
    '''
    if n == 0:
        return 1
    return 1 + int(math.log10(n))


_IOERROR_SEEN = False   # Have we seen an IOError when writing to stderr?

def _write(txt):
    '''
    Write to stderr, which relays to the Git client's stderr.

    Once we see any IOError while writing to stderr, stop writing to stderr,
    but don't propagate the error and kill the rest of the process. OK to
    continue on in the background. We might actually finish.
    '''
    global _IOERROR_SEEN
    if _IOERROR_SEEN:
        return

    try:
        sys.stderr.write(txt)
    except IOError:
        _IOERROR_SEEN = True

def _flush():
    ''' Flush stderr.'''
    global _IOERROR_SEEN
    if _IOERROR_SEEN:
        return

    try:
        sys.stderr.flush()
    except IOError:
        _IOERROR_SEEN = True
