#! /usr/bin/env python3.3
"""
Run subprocesses via a separate Python process to avoid blowing out memory,
which happens when fork() is called (e.g. on Linux).

When fork() is called on Linux, the entire memory space of the parent
process is copied, and then the subprocess is run. If the parent process
were using 800MB of memory, the child will as well, even though all it may
be doing is running `ps`.

Note that on Darwin this is not a problem.
"""

import io
import logging
import multiprocessing
import os
import queue
import signal
import subprocess
import sys
import time
import traceback

from p4gf_const import GIT_BIN as git_bin
from p4gf_const import GIT_BIN_DEFAULT as git_bin_default

import p4gf_char
from   p4gf_l10n      import _, NTR

LOG = logging.getLogger(__name__)
# The child process; call init() to initialize this.
ChildProc = None
ParentProc = None


def translate_git_cmd(cmd):
    '''Translate git commands from 'git' to value in GIT_BIN, which defaults to 'git' '''
    if cmd[0] != git_bin_default or git_bin == git_bin_default:      # no translation required
        return cmd
    new_cmd = list(cmd)
    new_cmd[0] = git_bin
    return new_cmd


def install_stack_dumper():
    """
    To debug a seemingly hung process, send the process the USR1 signal
    and it will dump the stacks of all threads to the log. To set up such
    behavior, call this function within each new Python process.
    """

    # pylint:disable=W0212
    def _dumper(signum, _frame):
        """
        Signal handler that dumps all stacks to the log.
        """
        LOG.info('Received signal {} in process {}'.format(signum, os.getpid()))
        LOG.info('Thread stack dump follows:')
        for thread_id, stack in sys._current_frames().items():
            LOG.info('ThreadID: {}'.format(thread_id))
            for filename, lineno, name, line in traceback.extract_stack(stack):
                LOG.info('  File: "{}", line {}, in {}'.format(filename, lineno, name))
                if line:
                    LOG.info('    ' + line.strip())
            LOG.info('----------')
        LOG.info('Thread stack dump complete')
    # pylint:enable=W0212

    # Try to use a signal that we're not using anywhere else.
    signal.signal(signal.SIGUSR1, _dumper)


def init():
    """
    Launch the separate Python process for running commands. This should
    be invoked early in the process, before gobs of memory are allocated,
    otherwise the child will consume gobs of memory as well.
    """
    global ChildProc, ParentProc
    if ChildProc and not ParentProc == os.getpid():
        ChildProc = None
    if not ChildProc:
        ParentProc = os.getpid()
        ChildProc = ProcessRunner()
        ChildProc.start()
        return True
    return False


def stop():
    """
    Stop the child process.
    """
    global ChildProc
    if not ChildProc:
        return
    ChildProc.stop()
    ChildProc = None


def _log_cmd_result(result, expect_error):
    """
    Record the command results in the log.

    If command completed successfully, record output at DEBUG level so that
    folks can suppress it with cmd:INFO. But if command completed with error
    (non-zero return code), then record its output at ERROR level so that
    cmd:INFO users still see it.
    """
    ec = result['ec']
    out = result['out']
    err = result['err']
    if (not ec) or expect_error:
        # Things going well? Don't care if not?
        # Then log only if caller is REALLY interested.
        log_level = logging.DEBUG
    else:
        # Things going unexpectedly poorly? Log almost all of the time.
        log_level = logging.ERROR
        log = logging.getLogger('cmd.cmd')
        if not log.isEnabledFor(logging.DEBUG):
            # We did not log the command. Do so now.
            log.log(log_level, result['cmd'])
    logging.getLogger('cmd.exit').log(log_level, NTR("exit: {0}").format(ec))
    out_log = logging.getLogger('cmd.out')
    out_log.debug(NTR("out : ct={0}").format(len(out)))
    if len(out) and out_log.isEnabledFor(logging.DEBUG3):
        out_log.debug3(NTR("out :\n{0}").format(out))
    if len(err):
        logging.getLogger('cmd.err').log(log_level, NTR("err :\n{0}").format(err))


def _validate_popen(cmd):
    """
    Checks that cmd is a list, reporting an error and returning None if it's not.
    Otherwise returns a boolean indicating if the ProcessRunner was initialized
    or not (False if already initialized).
    """
    if not isinstance(cmd, list):
        LOG.error("popen_no_throw() cmd not of list type: {}".format(cmd))
        return None
    logging.getLogger("cmd.cmd").debug(' '.join(cmd))
    if not ChildProc:
        LOG.warn("ProcessRunner launched at time of popen()")
        return init()
    return False


def _popen_no_throw_internal(cmd_, expect_error, stdin=None, env=None):
    """
    Internal Popen() wrapper that records command and result to log.
    The standard output and error results are converted to text using
    the p4gf_char.decode() function.
    """
    if _validate_popen(cmd_) is None:
        return None
    cmd = translate_git_cmd(cmd_)
    result = ChildProc.popen(cmd, stdin, env)
    result['cmd'] = ' '.join(cmd_)   # use the untranslated cmd_ for logging
    if 'out' in result:
        result['out'] = p4gf_char.decode(result['out'])
    if 'err' in result:
        result['err'] = p4gf_char.decode(result['err'])
    _log_cmd_result(result, expect_error)
    return result


def popen_binary(cmd_, expect_error=False, stdin=None, env=None):
    """
    Internal Popen() wrapper that records command and result to log.
    The stdin argument is the input text for the child process.
    The standard output and error results are in binary form.
    """
    if _validate_popen(cmd_) is None:
        return None
    cmd = translate_git_cmd(cmd_)
    result = ChildProc.popen(cmd, stdin, env)
    result['cmd'] = ' '.join(cmd_)
    _log_cmd_result(result, expect_error)
    return result


def popen_no_throw(cmd, stdin=None, env=None):
    """
    Call popen() and return, even if popen() returns a non-zero returncode.
    The stdin argument is the input text for the child process.

    Prefer popen() to popen_no_throw(): popen() will automatically fail fast
    and report errors. popen_no_throw() will silently fail continue on,
    probably making things worse. Use popen_no_throw() only when you expect,
    and recover from, errors.
    """
    return _popen_no_throw_internal(cmd, True, stdin, env)


def popen(cmd, stdin=None, env=None):
    """
    Wrapper for subprocess.Popen() that logs command and output to debug log.
    The stdin argument is the input text for the child process.

    Returns three-way dict: (out, err, Popen)
    """
    result = _popen_no_throw_internal(cmd, False, stdin, env)
    if result['ec'] == 0:
        return result
    raise RuntimeError(NTR('Command failed: {cmd}'
                         '\nexit code: {ec}.'
                         '\nstdout:\n{out}'
                         '\nstderr:\n{err}')
                       .format(ec=result['ec'],
                               cmd=result['cmd'],
                               out=result['out'],
                               err=result['err']))


def wait(cmd_, stdin=None, env=None):
    """
    Invokes subprocess.wait() with the given command list and the name
    of a file whose content provides the standard input to the command.
    The return code of the process is returned.
    """
    if _validate_popen(cmd_) is None:
        return None
    cmd = translate_git_cmd(cmd_)
    result = ChildProc.wait(cmd, stdin, env)
    result['cmd'] = ' '.join(cmd_)
    _log_cmd_result(result, False)
    return result['ec']


def call(cmd_, stdin=None, env=None):
    """
    Invokes subprocess.call() with the given command and the name of
    a file whose content provides the standard input to the command.
    The return code of the process is returned.
    """
    if _validate_popen(cmd_) is None:
        return None
    cmd = translate_git_cmd(cmd_)
    result = ChildProc.call(cmd, stdin, env)
    result['cmd'] = ' '.join(cmd_)
    _log_cmd_result(result, False)
    return result['ec']


def _cmd_runner(event, incoming, outgoing):
    """
    Running in a separate process, this function invokes subprocess.Popen()
    to perform the actual task of running a subprocess. This should never
    be called directly, but instead launched via multiprocessing.Process().
    """
    LOG.debug("_cmd_runner() running, pid={}".format(os.getpid()))
    install_stack_dumper()
    try:
        while not event.is_set():
            try:
                # Use timeout so we loop around and check the event.
                (cmd, stdin, cwd, wait_, call_, env) = incoming.get(timeout=1)
                # By taking a command list vs a string, we implicitly avoid
                # shell quoting. Also note that we are intentionally _not_
                # using the shell, to avoid security vulnerabilities.
                result = {"out": b'', "err": b''}
                # pylint: disable=E1101
                # Instance of '' has no '' member
                try:
                    stdin_file = None
                    if (wait_ or call_) and stdin:
                        # Special-case: stdin names a file to feed to process.
                        stdin_file = open(stdin)
                    if wait_:
                        p = subprocess.Popen(cmd, cwd=cwd, stdin=stdin_file,
                                             restore_signals=False, env=env)
                        result["ec"] = p.wait()
                    elif call_:
                        result["ec"] = subprocess.call(cmd, stdin=stdin_file,
                                                       restore_signals=False, env=env)
                    else:
                        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                                             restore_signals=False, env=env)
                        fd = p.communicate(stdin)
                        # return the raw binary, let higher level funcs decode it
                        result["out"] = fd[0]
                        result["err"] = fd[1]
                        result["ec"] = p.returncode
                except IOError as e:
                    LOG.warn("IOError in subprocess: {}".format(e))
                    result["ec"] = os.EX_IOERR
                    result["err"] = bytes(str(e), 'UTF-8')
                finally:
                    if stdin_file:
                        stdin_file.close()
                # pylint: enable=E1101
                outgoing.put(result)
            except queue.Empty:
                pass
        LOG.debug("_cmd_runner() process exiting, pid={}".format(os.getpid()))
    # pylint: disable=W0703
    # Catching too general exception
    except Exception as e:
        LOG.error("_cmd_runner() died unexpectedly, pid={}: {}".format(os.getpid(), e))
        event.set()
    # pylint: enable=W0703


class ProcessRunner():
    """
    Manages a child process which receives commands to be run via the
    subprocess module, returning the output to the caller.
    """

    def __init__(self):
        self.__event = None
        self.__input = None
        self.__output = None
        self.__stats = {}

    def log_stats(self):
        """
        log statistics for git commands run
        """
        LOG.debug("\nProcessRunner statistics:\n" +
                  "\n".join(["\t{:10.10}: {:6} {:6.3f}".format(k, v[0], v[1])
                  for (k, v) in self.__stats.items()]))

    def start(self):
        """
        Start the child process and prepare to run commands.
        """
        self.__event = multiprocessing.Event()
        self.__input = multiprocessing.Queue()
        self.__output = multiprocessing.Queue()
        pargs = [self.__event, self.__input, self.__output]
        p = multiprocessing.Process(target=_cmd_runner, args=pargs, daemon=True)
        p.start()
        LOG.debug("ProcessRunner started child {}, pid={}".format(p.pid, os.getpid()))
        if LOG.isEnabledFor(logging.DEBUG3):
            sink = io.StringIO()
            traceback.print_stack(file=sink)
            LOG.debug3("Calling stack trace:\n" + sink.getvalue())
            sink.close()

    def stop(self):
        """
        Signal the child process to terminate. Does not wait.
        """
        if self.__event:
            self.__event.set()
            self.__event = None
            self.__input = None
            self.__output = None
        self.log_stats()

    def run_cmd(self, cmd_, stdin, _wait, _call, env):
        """
        Invoke the given command via subprocess.Popen() and return the
        exit code, standard output, and standard error in a dict.
        """
        if not self.__input:
            LOG.warn("ProcessRunner.run_cmd() called before start()")
            self.start()

        # Make the child process use whatever happens to be our current
        # working directory, which seems to matter with Git.
        cwd = os.getcwd()
        start_time = time.time()
        cmd = translate_git_cmd(cmd_)  # translate the 'git' command if needed
        self.__input.put((cmd, stdin, cwd, _wait, _call, env))
        result = None
        while not self.__event.is_set():
            try:
                result = self.__output.get(timeout=1)
                break
            except queue.Empty:
                pass
        if not result:
            raise RuntimeError(_('Error running: {}').format(cmd))
        if cmd_[0] == "git":
            git_cmd = cmd_[1]
            if git_cmd.startswith("--git-dir") or git_cmd.startswith("--work-tree"):
                git_cmd = cmd_[2]
            elapsed_time = time.time() - start_time
            current = self.__stats.get(git_cmd, (0, 0))
            self.__stats[git_cmd] = (current[0] + 1, current[1] + elapsed_time)
        return result

    def popen(self, cmd, stdin, env=None):
        """
        Invoke the given command via subprocess.Popen() and return the
        exit code, standard output, and standard error in a dict.
        """
        return self.run_cmd(cmd, stdin, _wait=False, _call=False, env=env)

    def wait(self, cmd, stdin, env=None):
        """
        Invoke the given command via subprocess.Popen() and return the
        exit code, standard output, and standard error in a dict.
        """
        return self.run_cmd(cmd, stdin, _wait=True, _call=False, env=env)

    def call(self, cmd, stdin, env=None):
        """
        Invoke the given command via subprocess.Popen() and return the
        exit code, standard output, and standard error in a dict.
        """
        return self.run_cmd(cmd, stdin, _wait=False, _call=True, env=env)
