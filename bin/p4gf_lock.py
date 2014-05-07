#! /usr/bin/env python3.3

"""Acquire and release a lock using p4 counters."""

import logging
import math
import multiprocessing
import os
import sys
import time
import threading

from P4 import P4, P4Exception

import p4gf_create_p4
import p4gf_const
from   p4gf_l10n      import _, NTR
import p4gf_proc
import p4gf_util

LOG = logging.getLogger(__name__)

# time.sleep() accepts a float, which is how you get sub-second sleep durations.
MS = 1.0 / 1000.0

# How often we retry to acquire the lock.
_RETRY_PERIOD = 500 * MS

# Number of seconds after which we assume the process holding the lock has
# exited without deleting the counter. That is, if the heartbeat counter
# is not observed to have changed during this time, it must be stale.
HEARTBEAT_TIMEOUT_SECS = 60

# Rate for updating heartbeat counter, in seconds
HEART_RATE = 10


class LockCanceled(Exception):
    '''
    LockCanceled is used to signal that a lock was canceled, presumably
    by an administrator. Whatever operation is in progress should be
    cleanly abandoned.
    '''
    pass


class CounterLock:
    """An object that acquires a lock when created, releases when destroyed.

    with p4gf_lock.CounterLock(p4, "mylock") as lock:
        ... do stuff ...
        # Call periodically to ward off any future watchdog timer,
        # unless autobeat() was called earlier.
        lock.update_heartbeat()
    """

    def __init__( self
                , p4
                , counter_name
                , timeout_secs   = None
                , heartbeat_only = False):
        self.__p4__              = p4
        self.__counter_name__    = counter_name
        self.__has__             = False
        self.__timeout_secs__    = timeout_secs
        self.__log_timer         = None
        self.__acquisition_time  = None
        self.__heartbeat_time    = None
        self.__heartbeat_content = None
        self.__heartbeat_only    = heartbeat_only
        self.__auto_beat         = False
        self.__event             = None
        self.__stolen__          = False

    def __enter__(self):
        self.acquire(self.__timeout_secs__)
        return self

    def __exit__(self, exc_type, exc_value, _traceback):
        """If we own the lock, release it."""
        self.release()
        return False    # False = do not squelch exception

    def counter_name(self):
        """The lock."""
        return self.__counter_name__

    def heartbeat_counter_name(self):
        """Who owns the lock."""
        return p4gf_const.P4GF_COUNTER_LOCK_HEARTBEAT.format(counter=self.counter_name())

    def _acquire_attempt(self):
        """Attempt an atomic increment. If the result is 1, then we now
        own the lock. Any other value means somebody else owns the
        lock.
        """
        value = p4gf_util.first_value_for_key(
                self.__p4__.run('counter', '-u', '-i', self.counter_name()),
                'value')
        acquired = value == "1"  # Compare as strings, "1" != int(1)
        LOG.debug("_acquire_attempt {name} pid={pid} acquired={a} value={value}"
                  .format(pid=os.getpid(),
                          a=acquired,
                          name=self.counter_name(),
                          value=value))
        return acquired

    def acquire(self, timeout_secs=None):
        """Block until we acquire the lock or run out of time.

        timeout_secs:
            None or 0 means forever.
            negative means try once then give up.
        """
        if self.__heartbeat_only:
            self.update_heartbeat()
            self._start_pacemaker()
            return

        start_time = time.time()
        start_heart = self.get_heartbeat()
        counter_name = self.counter_name()
        alerted_user = False
        while True:
            self.__has__ = self._acquire_attempt()
            if self.__has__:
                self.__acquisition_time = time.time()
                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug("acquire {name} pid={pid}".format(
                        pid=os.getpid(), name=counter_name))
                self.update_heartbeat()
                self._start_pacemaker()
                self._create_log_timer()
                return

            # Let the client know we're waiting for a lock.
            if not alerted_user:
                sys.stderr.write(_('Waiting for access to repository...\n'))
                sys.stderr.flush()
                alerted_user = True

            # Check on the lock holder's status, maybe clear the lock.
            heartbeat = self.get_heartbeat()
            elapsed = time.time() - start_time
            if heartbeat == start_heart and elapsed > HEARTBEAT_TIMEOUT_SECS:
                LOG.debug("releasing the abandoned lock {}".format(counter_name))
                # Pretend we have the lock so we can release it.
                self.__has__ = True
                self.release()
                self.__stolen__ = True
                # Skip the timeout logic and loop around immediately.
                continue

            # Stop waiting if run out of time. Tell the git user
            # who's hogging the lock.
            if timeout_secs and timeout_secs <= elapsed:
                msg = _('Unable to acquire lock: {}').format(counter_name)
                if heartbeat:
                    msg += _('\nLock holder: {}').format(heartbeat)
                timeout = int(math.ceil(HEARTBEAT_TIMEOUT_SECS / 60.0))
                msg += _('\nPlease try again after {0:d} minute(s).').format(timeout)
                raise RuntimeError(msg)

            time.sleep(_RETRY_PERIOD)

    def _start_pacemaker(self):
        """If the lock has been acquired and it is configured to have
        automatic updates of the heartbeat counter, then set up a child
        process to regularly update the heartbeat.
        """
        if self.__auto_beat and self.__event is None:
            LOG.debug('launching pacemaker process for {}, pid={}'.format(
                self.__counter_name__, os.getpid()))
            # Set up event flag for signaling subprocess to exit.
            self.__event = multiprocessing.Event()
            # Start the pacemaker to beat the heart automatically.
            p = multiprocessing.Process(target=pacemaker,
                    args=[self.__counter_name__, self.__event])
            p.daemon = True
            p.start()

    def _stop_pacemaker(self):
        """If the pacemaker has been set up, signal it to stop.
        """
        if self.__event is not None:
            LOG.debug('signaling pacemaker to terminate for {}, pid={}'.format(
                self.__counter_name__, os.getpid()))
            # Signal the pacemaker to exit normally.
            self.__event.set()
            self.__event = None

    def _held_duration_seconds(self):
        '''
        How long since we acquired this lock?
        '''
        if not self.__acquisition_time:
            return 0
        return time.time() - self.__acquisition_time

    def release(self):
        """If we have the lock, release it. If not, NOP."""
        if self.has() or self.__heartbeat_only:
            self._stop_pacemaker()
        if not self.has():
            return False

        counter_name = self.counter_name()
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("release {name} pid={pid}".format(pid=os.getpid(), name=counter_name))

        self.__log_timer = None

        if _log_timer_duration_seconds() <= self._held_duration_seconds():
            self._report_long_lock()
            LOG.warning("Released lock {}".format(counter_name))
        self._clear_heartbeat()
        try:
            self.__p4__.run('counter', '-u', '-d', counter_name)
        except P4Exception:
            # Unusual, probably a "stale" lock that wasn't really stale
            # and cleaned up just as we were attempting to remove it.
            LOG.warn("lock counter deletion failed: {}".format(counter_name))
        self.__has__ = False

        return True

    def has(self):
        """Do we have the lock? False if we timed out or error."""
        return self.__has__

    def was_stolen(self):
        """Did we steal this lock after detecting a stale heartbeat?"""
        return self.__stolen__

    def autobeat(self):
        """Enable keeping the heartbeat counter updated automatically via
        a subprocess. This applies to the next acquisition of the lock.
        """
        self.__auto_beat = True

    def heartbeat_content(self):
        '''
        What should we write to our heartbeat counter?
        Enough data that an admin could figure out who's hogging the lock.

        If enough time has elapsed since the last call to this function,
        updates the content string.

        Returns a tuple containing the current and previous content strings.
        '''
        now = time.time()
        if self.__heartbeat_time and now - self.__heartbeat_time < HEART_RATE:
            return self.__heartbeat_content, self.__heartbeat_content

        self.__heartbeat_time = now

        # Produce a heartbeat value that is unique for each host and
        # process, and changes each time it is updated. Values are not so
        # important, but the value must continue to change so that others
        # can determine if the lock holder has died.
        val = NTR('{host} -- {process} -- {time}') \
              .format( host     = p4gf_util.get_server_id()
                     , process  = os.getpid()
                     , time     = int(self.__heartbeat_time)
                     )
        last = self.__heartbeat_content
        self.__heartbeat_content = val
        return self.__heartbeat_content, last

    def update_heartbeat(self):
        '''
        Update the timestamp written to our heartbeat counter, if we own the
        lock, or if this is a heartbeat-only lock.
        '''
        if not (self.has() or self.__heartbeat_only):
            return

        # don't update counter or log if nothing has changed
        current, last = self.heartbeat_content()
        if current != last:
            self.__p4__.run('counter', '-u', self.heartbeat_counter_name(), current)
            logger = LOG.getChild("heartbeat")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("update_heartbeat {name} {val}".format(
                    name=self.heartbeat_counter_name(), val=current))

    def get_heartbeat(self):
        '''
        Return the current heartbeat value, if any.

        Might provide clue to what holds the lock.
        '''
        result = self.__p4__.run('counter', '-u', self.heartbeat_counter_name())
        value = p4gf_util.first_value_for_key(result, 'value')
        if not value or value == '0':
            return None
        return value

    def _clear_heartbeat(self):
        '''
        Clear the heartbeat counter associated with this lock.
        '''
        # Clearing the heartbeat means we start all over with heartbeat tracking.
        self.__heartbeat_time = None
        self.__heartbeat_content = None
        # Suppress P4 exceptions here.
        # Don't care if fail, only lock counter matters.
        with self.__p4__.at_exception_level(P4.RAISE_NONE):
            counter_name = self.heartbeat_counter_name()
            result = self.__p4__.run('counter', '-u', '-d', counter_name)
            if p4gf_util.first_value_for_key(result, 'counter'):
                LOG.getChild("heartbeat").debug("deleted counter {}".format(counter_name))

    def _log_timer_expired(self, timer_id):
        '''
        We've held our lock for a long time. Tell the log.
        '''

        LOG.debug("_log_timer_expired id={}, pid={}".format(timer_id, os.getpid()))
        self._report_long_lock()

        # Restart timer: We'll log the same message again in N seconds.
        self._create_log_timer()

    def _report_long_lock(self):
        '''
        Unconditionally record our lock duration to log at level WARNING.
        '''
        LOG.warning("Lock {lock_name} held for {duration_seconds} seconds by {holder}"
                .format( lock_name        = self.counter_name()
                       , duration_seconds = int(self._held_duration_seconds())
                       , holder           = self.heartbeat_content()[0]))

    def _create_log_timer(self):
        '''
        Return a one-shot timer, already started, that will call
        _log_timer_expired() once in N seconds.
        '''
        duration_seconds = _log_timer_duration_seconds()
        timer_id = _next_timer_id()
        LOG.debug("Long-held locks reported every {} seconds. Timer id={}, pid={}"
                  .format(duration_seconds, timer_id, os.getpid()))
        t = threading.Timer( int(duration_seconds)
                           , CounterLock._log_timer_expired
                           , args=[self, timer_id])
        t.daemon = True
        t.start()
        self.__log_timer = t

    def canceled(self):
        '''
        Has our lock counter been cleared?

        This is one way to remote-kill a long-running Git Fusion task.
        '''
        value = p4gf_util.first_value_for_key(
                        self.__p4__.run('counter', '-u', self.counter_name()),
                        'value')

        if value != "0":  # Compare as strings, "0" != int(0)
            return False

        LOG.error("Lock canceled: {name}={value}"
                  .format(name=self.counter_name(), value=value))
        return True


def pacemaker(view_name, event):
    """
    As long as event flag is clear, update heartbeat of named lock.
    """
    # Running in a separate process, need to establish our own P4 connection
    # and set up a heartbeat-only lock to update the heartbeat of the lock
    # associated with the view.
    p4gf_proc.install_stack_dumper()
    LOG.getChild("pacemaker").debug("starting for lock {}".format(view_name))
    p4 = None
    try:
        p4 = p4gf_create_p4.create_p4(client=p4gf_util.get_object_client_name(), connect=False)
        lock = CounterLock(p4, view_name, heartbeat_only=True)
        while not event.is_set():
            with p4gf_create_p4.Connector(p4):
                lock.update_heartbeat()
            event.wait(HEART_RATE)
    # pylint: disable=W0703
    # Catching too general exception
    except Exception as e:
        LOG.getChild("pacemaker").error("error occurred: {}".format(str(e)))
    finally:
        LOG.getChild("pacemaker").debug("stopping for view {}".format(view_name))
        if p4:
            p4gf_create_p4.destroy(p4)
    # pylint: enable=W0703


def check_process_alive(pid, args):
    '''
    Check if the given process is still alive, comparing the arguments
    given to those of the running process. If a match is found, return
    True, otherwise False.
    '''
    # Check if there is a running process with that ID, and compare the args
    # with those in the heartbeat counter.
    result = p4gf_proc.popen(["ps", "-o", "pid,command"])
    # pylint: disable=E1101
    # Instance of '' has no '' member
    if result['ec'] != 0:
        LOG.error("ps failed, exit status {}".format(result['ec']))
        # Had an error, fall back on the default behavior.
        return True
    for line in result['out'].splitlines()[1:]:
        (fid, fargs) = line.strip().split(' ', 1)
        if fid == pid:
            # Ignore any leading 'python' cruft in the command name since the
            # interpreter strips that from our own command name.
            return not args or fargs.endswith(args)
    # Lock holder appears to be gone, go ahead and steal it.
    return False


_timer_id = 0


def _next_timer_id():
    '''
    Give each timer a unique identifier.
    '''
    global _timer_id
    _timer_id += 1
    return _timer_id


def _log_timer_duration_seconds():
    '''
    How long should we wait before logging reports about long-held locks?
    '''
    return float(5 * 60)


def user_spec_lock(p4, user_name):
    '''
    Return a lock on a p4 user name.
    '''
    lock = CounterLock(p4, user_name)
    lock.autobeat()
    return lock


def view_lock_name(view_name):
    '''
    Return a name for a counter that we use to lock a view.
    '''
    return p4gf_const.P4GF_COUNTER_LOCK_VIEW.format(repo_name=view_name)


def view_lock(p4, view_name, timeout_secs=None):
    '''
    Return a lock for a single view.
    '''
    lock = CounterLock(p4, view_lock_name(view_name), timeout_secs)
    lock.autobeat()
    return lock


def view_lock_heartbeat_only(p4, view_name):
    '''
    Return a lock that only updates an existing heartbeat.
    Does not acquire or release the lock.
    Assumes someone else holds the lock. Does not check for this.
    '''
    return CounterLock( p4
                      , view_lock_name(view_name)
                      , heartbeat_only=True)


def host_view_lock_name(host_name, view_name):
    '''
    Return a name for a counter that we use to coordinate access to
    shared resources for a particular host and view combination.
    '''
    return p4gf_const.P4GF_COUNTER_LOCK_HOST_VIEW.format( server_id=host_name
                                                        , repo_name=view_name)


def shared_host_view_name(host_name, view_name):
    '''Return the name for the shared host/view counter.
    '''
    return p4gf_const.P4GF_COUNTER_LOCK_HOST_VIEW_SHARED.format(
                                                          server_id=host_name
                                                        , repo_name=view_name)


def shared_host_view_lock(p4, view_name
                         , first_acquire_func=None
                         , last_release_func=None):
    '''Return a shared lock for the desired view, specific to the current host.
    '''
    host_name = p4gf_util.get_server_id()
    return SharedViewLock(p4, host_name, view_name
                          , first_acquire_func=first_acquire_func
                          , last_release_func=last_release_func)


def shared_host_view_lock_exists(p4, view_name):
    '''
    Are one or more processes already cloning through this repo?
    '''
    host_name = p4gf_util.get_server_id()
    result = get_shared_host_view_lock_state(p4, host_name, view_name)
    return result == SHARED


SHARED    = NTR('shared')
EXCLUSIVE = NTR('exclusive')

def get_shared_host_view_lock_state(p4, host_name, view_name):
    '''Does any process already hold this shared host+view lock?

    Returns
        None        if none do
        EXCLUSIVE   if one process holds an exclusive lock
        SHARED      if one or more processes hold shared locks

    Returns None if there is a stale lock suitable for stealing,
    but does not actually steal the lock.
    '''
    shared_name = shared_host_view_name(host_name, view_name)
    # Get the current value for the shared lock.
    current = p4gf_util.first_value_for_key(
            p4.run('counter', '-u', shared_name), 'value')
    if (not current) or (current == '0'):
        return None

    if current[0] == '*':
        # Exclusive lock in place, check if process still alive.
        (pid, args) = current.split('\t', 1)
        pid = pid.strip('*')
        if not check_process_alive(pid, args):
            # Seems the lock holder has died.
            return None
        else:
            return EXCLUSIVE

    # Shared lock(s) in place, check if process(es) still alive.
    okay_to_steal = True
    for row in current.splitlines():
        (pid, args) = row.split('\t', 1)
        if check_process_alive(pid, args):
            okay_to_steal = False
            break
    if okay_to_steal:
        # Seems the lock holders have all died.
        return None
    return SHARED


def exclusive_host_view_lock(p4, view_name):
    '''Return an exclusive lock for the desired view, specific to the current host.
    '''
    host_name = p4gf_util.get_server_id()
    return ExclusiveViewLock(p4, host_name, view_name)


class SharedViewLock:
    """Context manager for controlling host/view-specific resource access.
    The shared view lock allows for multiple holders, with the expectation
    that no modifications will be made to the shared resource. If an
    exclusive lock is currently being held, no shared locks will be granted.
    """

    # pylint:disable=R0913
    # Too many arguments (7/5). Converting all these default
    # options to a dict or struct is even worse.
    def __init__(self
                , p4
                , host_name
                , view_name
                , timeout=None
                , first_acquire_func=None
                , last_release_func=None):
        self.__p4 = p4
        self.__host_name = host_name
        self.__view_name = view_name
        self.__has = False
        self.__timeout_secs = timeout
        self.__first_acquire_func = first_acquire_func
        self.__last_release_func = last_release_func

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, _traceback):
        self.release()
        return False    # False = do not squelch exception

    def acquire(self):
        """Attempt to acquire a shared lock.
        """
        counter_name = host_view_lock_name(self.__host_name, self.__view_name)
        counter_lock = CounterLock(self.__p4, counter_name, self.__timeout_secs)
        shared_name = shared_host_view_name(self.__host_name, self.__view_name)
        start_time = time.time()
        while not self.__has:
            with counter_lock:
                # Get the current value for the shared lock.
                current = p4gf_util.first_value_for_key(
                        self.__p4.run('counter', '-u', shared_name), 'value')
                if current and current[0] == '*':
                    # Exclusive lock in place, check if process still alive
                    (pid, args) = current.split('\t', 1)
                    pid = pid.strip('*')
                    if not check_process_alive(pid, args):
                        # Seems the lock holder has died
                        try:
                            self.__p4.run('counter', '-u', '-d', shared_name)
                        except P4Exception:
                            LOG.warn("lock counter deletion failed: {}".format(shared_name))
                        continue
                if current is None or current == '0' or current[0] != '*':
                    # Add our PID to the list of shared lock holders.
                    value = self._lock_value()
                    if current and current != '0':
                        value = current + '\n' + value
                    self.__p4.run('counter', '-u', shared_name, value)
                    self.__has = True
                    # If current was empty, then we're the first acquisition.
                    if      (current is None or current == '0') \
                        and  self.__first_acquire_func:
                        self.__first_acquire_func()
            if not self.__has:
                elapsed = time.time() - start_time
                if self.__timeout_secs and self.__timeout_secs <= elapsed:
                    msg = _('Unable to acquire lock: {}').format(shared_name)
                    if current:
                        msg += _('\nLock holder: {}').format(current)
                    raise RuntimeError(msg)
                # Having released the lock and not acquired the shared lock,
                # pause briefly before trying again.
                time.sleep(_RETRY_PERIOD)

    def release(self):
        """Release a previously acquired hold on the shared lock. Does nothing
        if the lock is not currently held by this instance.
        """
        if not self.__has:
            return
        counter_name = host_view_lock_name(self.__host_name, self.__view_name)
        shared_name = shared_host_view_name(self.__host_name, self.__view_name)
        with CounterLock(self.__p4, counter_name):
            current = p4gf_util.first_value_for_key(
                    self.__p4.run('counter', '-u', shared_name), 'value')
            if current and current != '0':
                # Remove our PID from the list of shared lock holders.
                value = self._lock_value()
                lines = current.splitlines()
                lines = [line for line in lines if line != value]
                value = '\n'.join(lines)
                if value:
                    self.__p4.run('counter', '-u', shared_name, value)
                else:
                    if self.__last_release_func:
                        self.__last_release_func()
                    try:
                        self.__p4.run('counter', '-u', '-d', shared_name)
                    except P4Exception:
                        LOG.warn("lock counter deletion failed: {}".format(shared_name))
        self.__has = False

    def has(self):
        """Returns True if the lock is currently held by this instance,
        False otherwise.
        """
        return self.__has

    @staticmethod
    def _lock_value():
        """Generate the value for the shared lock.
        """
        return "{0}\t{1}".format(os.getpid(), ' '.join(sys.argv))


class ExclusiveViewLock:
    """Context manager for controlling host/view-specific resource access.
    The exclusive view lock allows for a single holder. The intended purpose
    is to allow the lock holder to modify the shared resource.
    """

    def __init__(self, p4, host_name, view_name, timeout=None):
        self.__p4 = p4
        self.__host_name = host_name
        self.__view_name = view_name
        self.__has = False
        self.__timeout_secs = timeout

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, _traceback):
        self.release()
        return False    # False = do not squelch exception

    def acquire(self):
        """Attempt to acquire a shared lock.
        """
        counter_name = host_view_lock_name(self.__host_name, self.__view_name)
        counter_lock = CounterLock(self.__p4, counter_name, self.__timeout_secs)
        shared_name = shared_host_view_name(self.__host_name, self.__view_name)
        start_time = time.time()
        while not self.__has:
            with counter_lock:
                # Get the current value for the lock.
                current = p4gf_util.first_value_for_key(
                        self.__p4.run('counter', '-u', shared_name), 'value')
                if current is None or current == '0':
                    # Set the value to our PID wrapped in asterisks to signal
                    # that it is an exclusive lock.
                    value = self._lock_value()
                    self.__p4.run('counter', '-u', shared_name, value)
                    self.__has = True
                else:
                    # Shared locks in place, check if processes still alive
                    okay_to_steal = True
                    for row in current.splitlines():
                        (pid, args) = row.split('\t', 1)
                        if check_process_alive(pid, args):
                            okay_to_steal = False
                            break
                    if okay_to_steal:
                        # Seems the lock holders have all died
                        try:
                            self.__p4.run('counter', '-u', '-d', shared_name)
                        except P4Exception:
                            LOG.warn("lock counter deletion failed: {}".format(shared_name))
                        continue
            if not self.__has:
                elapsed = time.time() - start_time
                if self.__timeout_secs and self.__timeout_secs <= elapsed:
                    msg = _('Unable to acquire lock: {}').format(shared_name)
                    if current:
                        msg += _('\nLock holder: {}').format(current)
                    raise RuntimeError(msg)
                # Having released the lock and not acquired the shared lock,
                # pause briefly before trying again.
                time.sleep(_RETRY_PERIOD)

    def release(self):
        """Release a previously acquired hold on the shared lock. Does nothing
        if the lock is not currently held by this instance.
        """
        if not self.__has:
            return
        counter_name = host_view_lock_name(self.__host_name, self.__view_name)
        shared_name = shared_host_view_name(self.__host_name, self.__view_name)
        with CounterLock(self.__p4, counter_name):
            current = p4gf_util.first_value_for_key(
                self.__p4.run('counter', '-u', shared_name), 'value')
            expected = self._lock_value()
            if current == expected:
                # Delete the exclusive lock
                try:
                    self.__p4.run('counter', '-u', '-d', shared_name)
                except P4Exception:
                    LOG.warn("lock counter deletion failed: {}".format(shared_name))
            else:
                LOG.warn("Lock {0} does not belong to us, not releasing".format(shared_name))
        self.__has = False

    def has(self):
        """Returns True if the lock is currently held by this instance,
        False otherwise.
        """
        return self.__has

    @staticmethod
    def _lock_value():
        """Generate the value for the exclusive lock.
        """
        return "*{0}*\t{1}".format(os.getpid(), ' '.join(sys.argv))
