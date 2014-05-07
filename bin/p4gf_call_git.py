#! /usr/bin/env python3.3
'''
Invoke the intended git command (i.e. git-receive-pack or git-upload-pack)
via a child process. If this process receives a terminating signal, set the
output streams to os.devnull so that it may continue writing indefinitely.
'''

from contextlib import contextmanager
import logging
import os
import signal
import sys

import p4gf_atomic_lock
import p4gf_const
import p4gf_git
from   p4gf_l10n import _, NTR
import p4gf_lock
import p4gf_log
import p4gf_util

LOG = p4gf_log.for_module()


#pylint:disable=E0602
#pylint does not know about BrokenPipeError
def prohibit_interrupt(view_name, pid):
    '''
    Signal the parent process that the child is now in a state where
    interruption would be problematic. As such, terminating signals will
    be ignored, and a message will be printed to standard output.
    '''
    link = _link_for_view(view_name)
    if not os.path.lexists(link):
        os.symlink(NTR('process-{}').format(pid), link)
        # This is expected to make its way back to the client.
        try:
            sys.stderr.write(_('Processing will continue even if connection is closed.\n'))
            sys.stderr.flush()
        except BrokenPipeError:
            # Too late, the client connection has already been terminated and
            # we're the last ones to know about it.
            os.unlink(link)
            _install_signal_handler(signal.SIG_DFL)
            LOG.warn('Processing of {} terminated by closed connection'.format(view_name))
            sys.exit(os.EX_PROTOCOL)
#pylint:enable=E0602


def _link_for_view(view_name):
    '''
    Return the path to the file used to signal that the child process
    is in a state such that interruption should be prohibited.
    '''
    link_name = NTR('processing-{}').format(view_name)
    return os.path.join(p4gf_const.P4GF_HOME, link_name)


def _install_signal_handler(handler):
    '''
    Install the given signal handler (either a function or one of the
    signal module constants) for all of the terminating signals.
    It is probably a good idea to use _signal_restorer to preserve and
    later restore any existing signal handlers.
    '''
    if LOG.isEnabledFor(logging.DEBUG):
        if callable(handler):
            label = handler.__qualname__
        elif isinstance(handler, int):
            if handler == signal.SIG_DFL:
                label = NTR('default')
            elif handler == signal.SIG_IGN:
                label = NTR('ignore')
            else:
                label = str(handler)
        else:
            label = str(handler)
        LOG.debug("_install_signal_handler({}) for pid={}".format(label, os.getpid()))
    signal.signal(signal.SIGHUP, handler)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGQUIT, handler)
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGTSTP, handler)


@contextmanager
def _signal_restorer():
    '''
    Context manager to preserve and subsequently restore the installed
    signal handlers.
    '''
    handlers = dict()
    for num in range(1, signal.NSIG):
        han = signal.getsignal(num)
        if han:
            handlers[num] = han
    try:
        yield
    finally:
        for num, han in handlers.items():
            signal.signal(num, han)


def _shared_lock_first_acquire():
    '''
    Callback function from SharedViewLock when this is the first
    shared lock on this host+view.

    Switches the git repo to --bare so that multiple parallel
    clones can all run simultaneously without battling over index.lock.
    '''
    LOG.debug("bare: First git-upload-pack starting, switching to bare...")
    p4gf_git.set_bare(True)


def _shared_lock_last_release():
    '''
    Callback function from SharedViewLock when this is the last
    shared lock on this host+view.

    Switches the git repo to non-bare so that future processes
    can copy Perforce-to-Git or receive a git push.
    '''
    LOG.debug("bare: Last git-upload-pack finished, switching to non-bare...")
    p4gf_git.set_bare(False)


def _call_original_git(cmdf, ctx, view_name, view_lock, exclusive=True):
    '''
    Manage the reader/writer locking to allow for multiple pull
    operations, or a single push operation, then delegate to the
    appropriate Git command (e.g. git-upload-pack, git-receive-pack).

    Arguments:
        cmdf -- function to be invoked within the lock; proxy for git command.
        ctx -- Git Fusion Context.
        view_name -- name of view being operated on.
        view_lock -- lock on the view, may be released early for pull.
        exclusive -- True to get an exclusive 'write' lock vs a shared 'read' lock
                     (currently hard-coded to always be True).
        env -- mapping of environment variables (default is None).

    Returns the exit code of the Git command.
    '''

    # Shared reader locks still cause conflicts and corrupt Git data.
    # Is Git running GC or packing or otherwise modifying its data even
    # during parallel pulls/clones?
    #
    # Until we can figure this out, no shared locks for you.
    #
    # Leave 'exclusive' correctly calculated to True only for push so that the
    # expensive Atomic Push locks kick in only during push, not pull.
    #
    if True:  # exclusive:
        # We have the view lock, why are we also getting this other lock?
        # Because in the pull case the view lock was released by the pull
        # operation may still be running. We need to wait for that to complete
        # by attempting to get the host/view lock.
        hvlock = p4gf_lock.exclusive_host_view_lock(ctx.p4gf, view_name)
    else:
        # While we have the view lock, get a shared host/view lock to prevent
        # any push operations from occurring at the same time and clobbering
        # the Git repository.
        hvlock = p4gf_lock.shared_host_view_lock(
            ctx.p4gf, view_name, first_acquire_func=_shared_lock_first_acquire,
            last_release_func=_shared_lock_last_release)

    with hvlock:
        if not exclusive:
            # In the pull case, we are done with the view lock and need only
            # the host/view lock.
            view_lock.release()
        else:
            # In the push case, engage the Atomic View Lock If a previous
            # lock on this view was stale and was stolen to obtain the
            # viewlock then remove the 'stale' client views from the
            # Reviews users.
            if view_lock.was_stolen():
                LOG.debug("lock was stolen for {0}".format(view_name))
                p4gf_atomic_lock.lock_update_repo_reviews(
                    ctx, view_name, ctx.clientmap, action=p4gf_atomic_lock.REMOVE)
            p4gf_atomic_lock.lock_update_repo_reviews(
                ctx, view_name, ctx.clientmap, action=p4gf_atomic_lock.ADD)

        # Detach git repo's HEAD before calling original git,
        # otherwise we won't be able to push the current branch (if any).
        if not p4gf_util.is_bare_git_repo():
            p4gf_util.checkout_detached_head()

        # Flush stderr before returning control to Git. Otherwise Git's own
        # output might interrupt ours.
        sys.stderr.flush()

        # call to git may take a while; no need to keep idle open connections
        ctx.disconnect()
        retval = cmdf()
        ctx.connect()

        if exclusive:
            p4gf_atomic_lock.lock_update_repo_reviews(
                ctx, view_name, ctx.clientmap, action=p4gf_atomic_lock.REMOVE)
        return retval


def call_git(cmdf, ctx, view_name, view_lock, exclusive=True):
    '''
    Call the provided fucntion while ignoring all terminating signals, which
    avoids any interruption caused by the client connection being terminated.
    Default signal handling is restored prior to return.

    Arguments:
        cmdf -- function to be invoked within the lock; proxy for git command.
        ctx -- P4GF context object.
        view_name -- name of the view to operate on.
        view_lock -- the lock for which to release ownership.
        exclusive -- True to get an exclusive 'write' lock vs a shared 'read' lock
                     (currently hard-coded to always be True).

    Returns the return value of the proxy function.
    '''

    def _signal_handler(signum, _frame):
        '''
        If we are to prohibit termination of the child process, redirect
        the stdout and stderr streams to os.devnull so the process can
        continue running and printing (to nowhere) without blocking.
        Otherwise, restore default signal handlers and terminate by
        replaying the signal.
        '''
        link = _link_for_view(view_name)
        if os.path.lexists(link):
            devnull = open(os.devnull, 'w')
            sys.stdout = devnull
            sys.stderr = devnull
            LOG.info('received signal {}, redirecting output to devnull, pid={}'.format(
                signum, os.getpid()))
            # ignore any further signals now that we've redirected our output
            _install_signal_handler(signal.SIG_IGN)
        else:
            _install_signal_handler(signal.SIG_DFL)
            os.kill(os.getpid(), signum)

    retval = None
    with _signal_restorer():
        _install_signal_handler(_signal_handler)
        try:
            retval = _call_original_git(cmdf, ctx, view_name, view_lock, exclusive)
        finally:
            link = _link_for_view(view_name)
            if os.path.lexists(link):
                os.unlink(link)
    return retval
