#! /usr/bin/env python3.3
"""
Utilities for configuring Git Fusion's debug/error/audit log.
"""

import argparse
import configparser
import datetime
import inspect
import gzip
import io
import logging
import logging.handlers
import os
import resource
import socket
import sys
import syslog
import tempfile
import time
import traceback

import p4gf_bootstrap  # pylint: disable=W0611
import p4gf_const
from   p4gf_ensure_dir import ensure_parent_dir
from   p4gf_l10n      import _, NTR
import p4gf_protect
import p4gf_util

_config_filename_default    = '/etc/git-fusion.log.conf'
_configured                 = False
_general_section            = NTR('general')
_audit_section              = NTR('audit')
_syslog_ident               = NTR('git-fusion')
_syslog_audit_ident         = NTR('git-fusion-auth')
_memory_usage               = False
_audit_logger_name          = NTR('audit')


def _find_config_file():
    """
    Return path to existing log config file, None if no config file found.

    Returns "/etc/git-fusion.log.conf" unless a test hook has overridden
    with environment variable P4GF_LOG_CONFIG_FILE.
    """
    # Check test-imposed environment var P4GF_LOG_CONFIG_FILE.
    if p4gf_const.P4GF_TEST_LOG_CONFIG_PATH in os.environ:
        path = os.environ[p4gf_const.P4GF_TEST_LOG_CONFIG_PATH]
        if os.path.exists(path):
            return path

    # Check /etc/git-fusion.log.conf .
    if os.path.exists(_config_filename_default):
        return _config_filename_default

    return None


class P4GFSysLogFormatter(logging.Formatter):
    """
    A formatter for SysLogHandler that inserts category and level into
    the message.
    """

    def __init__(self, fmt=None, datefmt=None):
        logging.Formatter.__init__(self, fmt, datefmt)

    def format(self, record):
        """
        Prepend category and level.
        """
        msg = record.getMessage()
        return (NTR("{name} {level} {message}").format(
            name=record.name, level=record.levelname, message=msg))


class P4GFSysLogHandler(logging.handlers.SysLogHandler):
    """
    A SysLogHandler that knows to include an ident string properly.
    The implementation in Python (as recent as 3.3.2) does not use
    the correct syslog API and as such is formatted incorrectly.
    """

    def __init__(self,
                 address=(NTR('localhost'), logging.handlers.SYSLOG_UDP_PORT),
                 facility=syslog.LOG_USER,
                 socktype=socket.SOCK_DGRAM,
                 ident=None):
        logging.handlers.SysLogHandler.__init__(self, address, facility, socktype)
        self.ident = ident if ident else _syslog_ident

    def emit(self, record):
        msg = self.format(record)
        syspri = self.mapPriority(record.levelname)
        # encodePriority() expects 1 for "user", shifts it to 8. but
        # syslog.LOG_USER is ALREADY shifted to 8, passing it to
        # encodePriority shifts it again to 64. No. Pass 0 for facility, then
        # do our own bitwise or.
        pri = self.encodePriority(0, syspri) | self.facility

        # Point syslog at our file. Syslog module remains pointed at our
        # log file until any other call to syslog.openlog(), such as those
        # in p4gf_audit_log.py.
        syslog.openlog(self.ident, syslog.LOG_PID)
        syslog.syslog(pri, msg)


def _effective_config(parser, section, defaults):
    """
    Build the effective configuration for a logger using a combination
    of the configparser instance and default options. Returns a dict
    with only the relevant settings for configuring a Logger instance.

    It is here the 'handler' over 'filename' and other such precedence
    rules are enforced.

    Arguments:
        parser -- instance of ConfigParser providing configuration.
        section -- section name from which to take logging configuration.
        defaults -- dict of default settings.
    """
    assert 'file' not in defaults
    config = defaults.copy()
    fallback = parser.defaults()
    if parser.has_section(section):
        fallback = parser[section]
    config.update(fallback)
    # Allow configuration 'file' setting to take precedence over 'filename'
    # since it is not one of our defaults.
    if 'file' in config:
        config['filename'] = config.pop('file')
    if 'handler' in config:
        val = config['handler']
        if val.startswith('syslog'):
            # Logging to syslog means no format support.
            config.pop('format', None)
            config.pop('datefmt', None)
        # Logging to a handler means no filename
        config.pop('filename', None)
    elif 'filename' in config:
        # perform variable substitution on file path
        fnargs = {}
        fnargs['user'] = os.path.expanduser('~')
        fnargs['tmp'] = tempfile.gettempdir()
        config['filename'] %= fnargs
    config.setdefault(NTR('format'), logging.BASIC_FORMAT)
    config.setdefault(NTR('datefmt'), None)
    return config


def _configure_logger(config, name=None, ident=None):
    """
    Configure the named logger (or the root logger if name is None) using
    provided settings, which likely came from _effective_config().

    Arguments:
        config -- dict of Logger settings (will be modified).
        name -- name of the logger to configure (defaults to root logger).
        ident -- syslog identity, if handler is 'syslog'.
    """
    formatter = None
    if 'handler' in config:
        val = config.pop('handler')
        if val.startswith('syslog'):
            words = val.split(maxsplit=1)
            if len(words) > 1:
                handler = P4GFSysLogHandler(address=words[1], ident=ident)
            else:
                handler = P4GFSysLogHandler(ident=ident)
            formatter = P4GFSysLogFormatter()
        elif val == 'console':
            handler = logging.StreamHandler()
        else:
            sys.stderr.write(_('Git Fusion: unrecognized log handler: {}\n').format(val))
            handler = logging.StreamHandler()
    elif 'filename' in config:
        fpath = config.pop('filename')
        handler = logging.FileHandler(fpath, 'a', 'utf-8')
    else:
        handler = logging.StreamHandler()
    # Always remove these fake logging levels
    fs = config.pop('format', None)
    dfs = config.pop('datefmt', None)
    if formatter is None:
        # Build the formatter if one has not already been.
        formatter = logging.Formatter(fs, dfs)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.addHandler(handler)
    logger.setLevel(config.pop('root').upper())

    # Set the logging levels based on the remaining settings
    for key, val in config.items():
        logging.getLogger(key).setLevel(val.upper())


def _read_configuration(filename):
    """
    Attempt to read the log configuration using the "new" format,
    massaging from the old format into the new as needed (basically
    prepend a section header). Returns an instance of ConfigParser.
    """
    # Note that we do not make our 'general' section the default since
    # that requires an entirely different API to work with.
    parser = configparser.ConfigParser(interpolation=None)
    with open(filename, 'r') as f:
        text = f.read()
    try:
        try:
            parser.read_string(text, source=filename)
        except configparser.MissingSectionHeaderError:
            text = '[{}]\n{}'.format(_general_section, text)
            parser.read_string(text, source=filename)
    except configparser.Error as e:
        sys.stderr.write(_('Git Fusion: log configuration error, using defaults: {}\n').format(e))
        parser = configparser.ConfigParser()
    return parser


def _apply_default_config(parser):
    """
    Given a ConfigParser instance, merge with the default logging settings
    to produce the effective logging configuration, returned as a tuple of
    the general and audit settings.
    """
    # Configure the general logging
    general_config = NTR({
        'filename': os.environ['HOME'] + '/p4gf_log.txt',
        'format'  : '%(asctime)s %(name)-10s %(levelname)-8s %(message)s',
        'datefmt' : '%m-%d %H:%M:%S',
        'root'    : 'WARNING',
    })
    general_config = _effective_config(parser, _general_section, general_config)

    # Configure the audit logging (defaults to standard syslog)
    audit_config = {'root': NTR('warning')}
    audit_config = _effective_config(parser, _audit_section, audit_config)
    if not ('filename' in audit_config or 'handler' in audit_config):
        audit_config['handler'] = NTR('syslog')
    return (general_config, audit_config)


def _script_name():
    """
    Return the 'p4gf_xxx' portion of argv[0] suitable for use as a log category.
    """
    return sys.argv[0].split('/')[-1]


class ExceptionLogger:
    """
    A handler that records all exceptions to log instead of to console.

    with p4gf_log.ExceptionLogger() as dont_care:
        ... your code that can raise exceptions...
    """

    def __init__(self, exit_code_array=None, category=_script_name(),
                 squelch=True, write_to_stderr_=False):
        """
        category, if specified, controls where exceptions go if caught.
        squelch controls the return value of __exit__, which in turn
        controls what happens after reporting a caught exception:

        squelch = True: squelch the exception.
            This is what we want if we don't want this exception
            propagating to console. Unfortunately this also makes it
            harder for main() to know if we *did* throw+report+squelch
            an exception.

        squelch = False: propagate the exception.
            This usually results in dump to console, followed by the
            death of your program.

        """
        self.__category__ = category
        self.__squelch__ = squelch
        self.__write_to_stderr__ = write_to_stderr_
        if exit_code_array:
            self.__exit_code_array__ = exit_code_array
        else:
            self.__exit_code_array__ = [1]
        _lazy_init()

    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_value, _traceback):
        """
        Record any exception to log. NOP if no exception.
        """
        # Someone called sys.exit(x). Retain the exit code.
        if isinstance(exc_value, SystemExit):
            self.__exit_code_array__[0] = exc_value.code
            return self.__squelch__

        if exc_type:
            log = logging.getLogger(self.__category__)
            log.error("Caught exception", exc_info=True)
            val = exc_value.args[0] if exc_value.args else exc_value
            if self.__write_to_stderr__:
                sys.stderr.write('{}\n'.format(val))

        return self.__squelch__


def _caller(depth=1):
    """
    Return a dict for the caller N frames up the stack.
    """
    stack = inspect.stack()
    if len(stack) <= depth:
        if len(stack) == 0:
            return
        depth = 1
    frame = stack[depth]
    fname = os.path.basename(frame[1])
    frame_dict = { 'file'     : fname,
                   'filepath' : frame[1],
                   'filebase' : os.path.splitext(fname)[0],
                   'line'     : frame[2],
                   'func'     : frame[3],
                 }
    # Internally sever link to Traceback frame in an attempt to avoid
    # module 'inspect' and its refcount cycles.
    del frame
    return frame_dict


def for_module(depth=2):
    """
    Return the logger for the calling module.

    Returns "p4gf_foo" for module /Users/bob/p4gf_foo.py. This simple
    punctuation-less name works better as a log category.

    Typically this is called at the top of your Python script:

        LOG = p4gf_log.for_module()

    and then used later as a logger:

        LOG.debug("hello")

    The depth parameter tells for_module() how many stack frames up to
    search for the caller's filename. The default of two is usually what you
    want.
    """
    c = _caller(depth)
    return logging.getLogger(c['filebase'])


def run_with_exception_logger(func, write_to_stderr=False):
    """
    Wrapper for most 'main' callers, route all exceptions to log.
    """
    exit_code = [1]
    c = None
    log = None
    with ExceptionLogger(exit_code, write_to_stderr_=write_to_stderr):
        c = _caller(2)
        log = logging.getLogger(c['filebase'])
        log.debug("{file}:{line} start --".format(file=c['file'],
                                                  line=c['line']))
        prof_log = logging.getLogger('p4gf_profiling')
        run_with_profiling = prof_log.isEnabledFor(logging.DEBUG3)
        if run_with_profiling:
            # Run the function using the Python profiler and dump the
            # profiling statistics to the log.
            try:
                import cProfile
                prof = cProfile.Profile()
                prof.enable()
            except ImportError:
                log.warn('cProfile not available on this system, profiling disabled')
                run_with_profiling = False
        exit_code[0] = func()
        if run_with_profiling:
            prof.disable()
            buff = io.StringIO()
            import pstats
            ps = pstats.Stats(prof, stream=buff)
            ps.sort_stats(NTR('cumulative'))
            ps.print_stats(100)
            ps.print_callees(100)
            prof_log.debug3("Profile stats for {}:\n{}".format(c['file'], buff.getvalue()))
            buff.close()

    if log and c:
        log.debug("{file}:{line} exit={code} --".format(code=exit_code[0],
                                                        file=c['file'],
                                                        line=c['line']))

    if log and _memory_usage:
        log.warn(memory_usage())

    sys.exit(exit_code[0])


def memory_usage():
    """
    Format a string that indicates the memory usage of the current process.
    """
    r = resource.getrusage(resource.RUSAGE_SELF)
    # Linux seems to report in KB while Mac uses bytes.
    factor = 20 if os.uname()[0] == "Darwin" else 10
    mem = r.ru_maxrss / (2 ** factor)
    return NTR('memory usage (maxrss): {: >8.2f} MB').format(mem)


def _print_config(label, config):
    """
    Print the sorted entries of the config, preceded with the label
    (printed in square brackets ([]) as if a section header).
    """
    options = sorted(config.keys())
    print("[{0}]".format(label))
    for opt in options:
        print("{0} = {1}".format(opt, config[opt]))


def _lazy_init(debug=False):
    """
    If we have not yet configured the logging system, do so now, using a
    default set of configuration settings.
    """
    global _configured
    if not _configured:
        try:
            config_file_path = _find_config_file()
            if config_file_path:
                parser = _read_configuration(config_file_path)
            else:
                parser = configparser.ConfigParser()
            general, audit = _apply_default_config(parser)
            if debug:
                _print_config(_general_section, general)
                _print_config(_audit_section, audit)
            _configure_logger(general, ident=_syslog_ident)
            _configure_logger(audit, _audit_logger_name, _syslog_audit_ident)
            _configured = True
        # pylint:disable=W0703
        except Exception:
        # pylint:enable=W0703
            # Unable to open log file for write? Some other random error?
            # Printf and squelch.
            sys.stderr.write(_('Git Fusion: Unable to configure log.\n'))
            sys.stderr.write(traceback.format_exc())


def create_failure_file(prefix=''):
    """
    Create file P4GF_HOME/logs/2013-07-31T164804.log.txt and attach it
    to logging category 'failures'.

    Causes all future logs to 'failures' to tee into this file as well as
    wherever the usual debug log goes.

    Each failure deserves its own timestamped file (unless you manage to fail
    twice in the same second, in which case, good on ya, you can have both
    failures in a single file).

    NOP if there's already a handler attached to 'failure': we've probably
    already called create_failure_file().
    """
    logger = logging.getLogger('failures')
    if logger.handlers:
        return

    p4gf_dir = p4gf_const.P4GF_HOME

    date = datetime.datetime.now()
    date_str = date.isoformat().replace(':', '').split('.')[0]
    file_path = p4gf_const.P4GF_FAILURE_LOG.format(
                                                 P4GF_DIR = p4gf_dir
                                               , prefix   = prefix
                                               , date     = date_str )
    ensure_parent_dir(file_path)
    logger.addHandler(logging.FileHandler(file_path, encoding='utf8'))
    logger.setLevel(logging.ERROR)
    logger.error('Recording Git Fusion failure log to {}'.format(file_path))


def close_failure_file():
    """
    If we have a failure log attached to category 'failures', remove it,
    close the file, compress it.
    """
    logger = logging.getLogger('failures')
    if not logger.handlers:
        return
    handler = logger.handlers[0]
    logger.removeHandler(handler)
    handler.close()

    file_path = handler.baseFilename
    gz_path   = handler.baseFilename + '.gz'
    logger.error('Compressing log report to {}'.format(gz_path))

    with open(file_path, 'rb') as fin \
    ,    gzip.open(gz_path, 'wb') as fout:

        while True:
            b = fin.read(100 * 1024)
            if not len(b):
                break
            fout.write(b)
        fout.flush()

    os.remove(file_path)


def _prepare_logging_args(environ=None):
    """Generate a dict containing extra logging arguments."""
    if environ is None:
        environ = os.environ
    args = dict()
    args['epoch'] = int(time.time())
    args['clientIp'] = p4gf_protect.get_remote_client_addr()
    server_id = p4gf_util.read_server_id_from_file()
    args['serverId'] = server_id if server_id else 'no-server-id'
    pusher = environ.get(p4gf_const.P4GF_AUTH_P4USER, None)
    if not pusher:
        pusher = environ.get('REMOTE_USER', 'unknown')
    args['userName'] = pusher
    return args


def record_error(line):
    """
    Write a line of text to audit log, at priority level 'error'.
    """
    log = logging.getLogger(_audit_logger_name)
    args = _prepare_logging_args()
    log.error(line, extra=args)


def record_argv():
    """
    Write entire argv and SSH* environment variables to audit log.
    """
    line = " ".join(sys.argv)
    ssh_env = ["{}={}".format(k, v) for k, v in os.environ.items() if k.startswith('SSH_')]
    if ssh_env:
        line = line + " " + " ".join(ssh_env)
    log = logging.getLogger(_audit_logger_name)
    args = _prepare_logging_args()
    log.warn(line, extra=args)


_http_params = ('PATH_INFO', 'QUERY_STRING', 'REMOTE_ADDR', 'REMOTE_USER', 'REQUEST_METHOD')
def record_http(environ):
    """
    Write HTTP-related environment variables to audit log.
    """
    line = " ".join("{}={}".format(k, environ.get(k)) for k in _http_params)
    log = logging.getLogger(_audit_logger_name)
    args = _prepare_logging_args(environ)
    log.warn(line, extra=args)

                        # pylint:disable=W9903
                        # non-gettext-ed string
                        # This is all debug/test code from here on down.
                        # No translation required.
def main():
    """
    Parse the command-line arguments and perform the requested operation.
    """
    desc = """Test wrapper and debugging facility for Git Fusion logging.
    By default, does not read the global log configuration unless the
    --default option is given. Set the P4GF_LOG_CONFIG_FILE environment
    variable to provide the path to a log configuration file.
    """
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--default', action='store_true',
                        help="allow reading default log configuration")
    parser.add_argument('--debug', action='store_true',
                        help="print the effective logging configuration (implies --default)")
    parser.add_argument('--http', action='store_true',
                        help="audit log as if HTTP request")
    parser.add_argument('--ssh', action='store_true',
                        help="audit log as if SSH request")
    parser.add_argument('--level', default='INFO',
                        help="log level name (default is 'INFO')")
    parser.add_argument('--name', default='test',
                        help="logger name (default is 'test')")
    parser.add_argument('--msg', default='test message',
                        help="text to write to log")
    args = parser.parse_args()

    if not args.default and not args.debug:
        # Disable loading the default logging configuration since that
        # makes testing log configurations rather difficult.
        global _config_filename_default
        _config_filename_default = '/foo/bar/baz'

    # Perform usual logging initialization.
    _lazy_init(args.debug)

    if args.debug:
        # We're already done.
        return
    elif args.http:
        record_http(os.environ)
    elif args.ssh:
        record_argv()
    else:
        log = logging.getLogger(args.name)
        # pylint:disable=W0212
        if args.level not in logging._levelNames:
            sys.stderr.write("No such logging level: {}\n".format(args.level))
            sys.exit(1)
        lvl = logging._levelNames[args.level]
        # pylint:enable=W0212
        log.log(lvl, args.msg)

if __name__ == "__main__":
    main()
