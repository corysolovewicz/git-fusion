#! /usr/bin/env python3.3
"""
WSGI/CGI script in Python for interfacing with Git HTTP backend.
"""
# pylint:disable=W0201

from contextlib import closing, contextmanager
import functools
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
import wsgiref.handlers
import wsgiref.simple_server
import wsgiref.util

# Ensure the system path includes our modules.
try:
    import p4gf_version
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    import p4gf_version

import p4gf_env_config    # pylint: disable=W0611
import p4gf_atomic_lock
import p4gf_call_git
import p4gf_const
import p4gf_context
import p4gf_copy_p2g
import p4gf_create_p4
import p4gf_gc
import p4gf_git
from   p4gf_git_swarm import GSReviewCollection
import p4gf_gitmirror
import p4gf_group
import p4gf_init
import p4gf_init_repo
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_lock
import p4gf_log
import p4gf_proc
import p4gf_server_common
import p4gf_translate
import p4gf_util

LOG = p4gf_log.for_module()


class OutputSink(object):
    """
    Context manager that redirects standard output and error streams to a
    temporary file, which is automatically deleted.
    """

    def __init__(self):
        self.__temp = None
        self.__stdout = None
        self.__stderr = None

    def __enter__(self):
        if not self.__temp:
            self.__temp = tempfile.NamedTemporaryFile(
                mode='w+', encoding='UTF-8', prefix='http-output-', delete=False)
            self.__stdout = sys.stdout
            self.__stderr = sys.stderr
            sys.stdout.flush()
            sys.stderr.flush()
            sys.stdout = self.__temp
            sys.stderr = self.__temp
            LOG.debug("stdout/stderr redirecting to {}...".format(self.__temp.name))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__temp:
            sys.stdout.flush()
            sys.stderr.flush()
            sys.stdout = self.__stdout
            sys.stderr = self.__stderr
            self.__temp.close()
            try:
                os.unlink(self.__temp.name)
            except OSError:
                LOG.warn("OutputSink failed to delete file {}".format(self.__temp.name))
            self.__temp = None
            LOG.debug("stdout/stderr redirection terminated")
        return False

    def readall(self):
        """
        Return the contents of the temporary file as bytes (returns an empty
        bytes array if the file has been closed).
        """
        if not self.__temp:
            return bytes()
        self.__temp.seek(0)
        txt = self.__temp.read()
        return txt.encode('UTF-8')


def _log_environ(environ):
    """
    Dump our environment to the log at DEBUG level.
    """
    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3("WSGI environment:")
        keys = sorted(environ.keys())
        for name in keys:
            LOG.debug3("    {}: {}".format(name, environ[name]))


def _get_command(environ):
    """
    Determine if this is a pull or push operation, returning the canonical
    git command (i.e. git-upload-pack or git-receive-pack). Returns None if
    the command does not match what is expected, or is not provided.
    """
    cmd = None
    method = environ["REQUEST_METHOD"]
    if method == 'POST':
        # this code expects the repo name to have already been shifted off
        # lop off leading slash
        cmd = environ['PATH_INFO'][1:]
    elif method == 'GET':
        qs = environ.get('QUERY_STRING')
        if qs:
            params = urllib.parse.parse_qs(qs)
            cmd = params.get('service')[0]
    LOG.debug('_get_command() retrieved {} command'.format(cmd))
    # check for the two allowable commands/services
    if cmd == 'git-upload-pack' or cmd == 'git-receive-pack':
        return cmd
    # anything else is wrong
    return None


@contextmanager
def deleting(path):
    """
    Delete the named file upon exit.
    """
    try:
        yield
    finally:
        try:
            os.unlink(path)
        except OSError:
            LOG.warn("@deleting failed to delete file {}".format(path))


@contextmanager
def unmirror(view_name):
    """
    Upon exit, signal the gitmirror code to unregister the named view.
    """
    try:
        yield
    finally:
        p4gf_gitmirror.close(view_name)

_REQUIRED_ENVARS = (
    'PATH',
    'PATH_INFO',
    'QUERY_STRING',
    'REMOTE_ADDR',
    'REMOTE_USER',
    'REQUEST_METHOD'
    )
_OPTIONAL_ENVARS = (
    'CONTENT_TYPE',
    'LANG',
    'P4CONFIG',
    # 'P4PASSWD', -- if nothing else works...
    'P4PORT',
    'P4USER',
    p4gf_const.P4GF_PROTECTS_HOST,
    p4gf_const.P4GF_TEST_LOG_CONFIG_PATH,
    'P4GF_P4D_VERSION_FORCE_ACCEPTABLE',
    p4gf_const.P4GF_ENV_NAME
    )

def _call_git(input_name, environ, ctx):
    """
    Invoke git http-backend with the appropriate environment, using the
    function given by environ['proc.caller'] to invoke the child process
    (same arguments as p4gf_proc.call()) such that its output will be
    directed back to the client.

    Arguments:
        input_name -- file path of input for git-http-backend.
        environ -- environment variables.
        ctx -- context object.
    """
    # Set up an environment not only for git-http-backend but also for
    # our pre-receive hook script, which needs to know how to connect
    # to Perforce and how it might need to log messages. Keep in mind
    # that most web servers are run as users that do not have a home
    # directory (and likewise are lacking shell initialization files).
    env = dict()
    # Set specific values for some of the parameters.
    env['GIT_HTTP_EXPORT_ALL'] = '1'
    env['GIT_PROJECT_ROOT'] = ctx.view_dirs.GIT_DIR
    env['HOME'] = environ.get('HOME', os.path.expanduser('~'))
    env[p4gf_const.P4GF_AUTH_P4USER] = environ['REMOTE_USER']
    env['SERVER_PROTOCOL'] = 'HTTP/1.1'
    # Copy some of the other parameters that are required.
    for name in _REQUIRED_ENVARS:
        env[name] = environ[name]
    # Copy any optional parameters that help everything work properly.
    for name in _OPTIONAL_ENVARS:
        if name in environ:
            env[name] = environ[name]
    # Copy any LC_* variables so sys.getfilesystemencoding() gives the right value.
    for key, val in environ.items():
        if key.startswith('LC_'):
            env[key] = val
    cmd_list = ['git', 'http-backend']
    LOG.debug('_call_git() invoking {} with environment {}'.format(cmd_list, env))
    caller = environ['proc.caller']
    ec = caller(cmd_list, stdin=input_name, env=env)
    LOG.debug("_call_git() {} returned {}".format(cmd_list, ec))

_REQUIRED_HTTP_PARAMS = [
    ('PATH',           _('500 Internal Server Error'), _('Missing PATH value')),
    ('PATH_INFO',      _('400 Bad Request'),           _('Missing PATH_INFO value')),
    ('QUERY_STRING',   _('400 Bad Request'),           _('Missing QUERY_STRING value')),
    ('REQUEST_METHOD', _('400 Bad Request'),           _('Missing REQUEST_METHOD value')),
    ('REMOTE_USER',    _('401 Unauthorized'),          _('Missing REMOTE_USER value'))
    ]

# pylint: disable=R0912, R0914, R0915
def _wsgi_app(environ, start_response):
    """
    WSGI application to process the incoming Git client request. This is
    nearly equivalent to p4gf_auth_server.main() with the exception of
    input validation and error handling.
    """
    p4gf_log.record_http(environ)
    p4gf_version.log_version()
    _log_environ(environ)
    p4gf_version.version_check()
    LOG.debug("processing HTTP request, pid={}".format(os.getpid()))
    # Keep the content type to exactly 'text/plain' so there is at least
    # the remote chance that Git might show our error messages (does not
    # appear to work in practice, however).
    headers = [('Content-Type', 'text/plain')]

    encoding = sys.getfilesystemencoding()
    if encoding == 'ascii':
        # This encoding is wrong and will eventually lead to problems.
        LOG.error("Using 'ascii' file encoding will ultimately result in errors, "
            "please set LANG/LC_ALL to 'utf-8' in web server configuration.")
        start_response(_('500 Internal Server Error'), headers)
        return [b"Filesystem encoding not set to acceptable value.\n"]

    # Sanity check the request.
    for (name, status, msg) in _REQUIRED_HTTP_PARAMS:
        if name not in environ:
            start_response(status, headers)
            return [msg.encode('UTF-8')]

    input_name = environ['wsgi.input']
    # Extract the view_name_git by removing the expected git request suffixes
    path_info = environ['PATH_INFO']
    git_suffixes = ['/info/refs', '/HEAD', '/git-upload-pack', '/git-receive-pack']
    path_end = len(path_info)
    for suffix in git_suffixes:
        try:
            path_end = path_info.index(suffix)
            break
        except ValueError:
            pass
    # slice away the leading slash and the trailing git request suffixes
    view_name_git  = path_info[1:path_end]
    # and remove the view_name_git from the front of PATH_INFO
    environ['PATH_INFO'] = path_info[path_end:]
    LOG.debug("new PATH_INFO {0} view_name_git {1}".format(environ['PATH_INFO'], view_name_git))

    if not view_name_git:
        start_response(_('400 Bad Request'), headers)
        msg = _('Missing required repository name in URL\n')
        return [msg.encode('UTF-8')]
    # translate '/' ':' ' ' .. etc .. for internal view_name
    view_name = p4gf_translate.TranslateReponame.git_to_repo(view_name_git)
    LOG.debug("public view_name: {0}   internal view_name: {1}".format(view_name_git, view_name))

    audit_logger = p4gf_server_common.ExceptionAuditLogger()
    p4_closer = p4gf_create_p4.Closer()
    sink = OutputSink()
    temp_deleter = deleting(input_name)
    mirror_closer = unmirror(view_name)
    with audit_logger   \
        , p4_closer     \
        , sink          \
        , temp_deleter  \
        , mirror_closer:
        LOG.debug(p4gf_log.memory_usage())
        start_time = time.time()

        p4gf_util.reset_git_enviro()
        p4 = p4gf_create_p4.create_p4()
        if not p4:
            start_response(_('500 Internal Server Error'), headers)
            return [b"Perforce connection failed\n"]
        LOG.debug("connected to P4: %s", p4)

        p4gf_server_common.check_readiness(p4)
        p4gf_server_common.check_lock_perm(p4)
        if not p4gf_server_common.check_protects(p4):
            p4gf_server_common.raise_p4gf_perm()

        user = environ['REMOTE_USER']
        if p4gf_server_common.run_special_command(view_name, p4, user):
            start_response(_('200 OK'), headers)
            return [sink.readall()]
        command = _get_command(environ)
        if not command:
            start_response(_('400 Bad Request'), headers)
            return [b"Unrecognized service\n"]
        # Other places in the Perforce-to-Git phase will need to know the
        # name of client user, so set that here. As for Git-to-Perforce,
        # that is handled later by setting the REMOTE_USER envar. Notice
        # also that we're setting os.environ and not 'environ'.
        os.environ[p4gf_const.P4GF_AUTH_P4USER] = user
        # Likewise, some code needs a hint that the request is coming over
        # one protocol (HTTP) or the other (SSH).
        os.environ['REMOTE_ADDR'] = environ['REMOTE_ADDR']

        # Initialize the external process launcher early, before allocating lots
        # of memory, and just after all other conditions have been checked.
        p4gf_proc.init()
        # Prepare for possible spawn of GitMirror worker process by forking
        # now before allocating lots of memory.
        p4gf_gitmirror.setup_spawn(view_name)
        # Kick off garbage collection debugging, if enabled.
        p4gf_gc.init_gc()

        # Go no further, create NOTHING, if user not authorized.
        # We use the translated internal view name here for perm authorization
        required_perm = p4gf_server_common.COMMAND_TO_PERM[command]
        view_perm = p4gf_group.ViewPerm.for_user_and_view(p4, user, view_name, required_perm)
        try:
            p4gf_server_common.check_authorization(p4, view_perm, user, command, view_name)
        except p4gf_server_common.CommandError as ce:
            start_response(_('403 Forbidden'), headers)
            return [str(ce).encode('UTF-8')]

        # Create Git Fusion server depot, user, config. NOPs if already created.
        p4gf_init.init(p4)

        before_lock_time = time.time()
        with p4gf_lock.view_lock(p4, view_name) as view_lock:
            after_lock_time = time.time()

            # Create Git Fusion per-repo client view mapping and config.
            init_repo_status = p4gf_init_repo.init_repo(p4, view_name, view_lock)
            if init_repo_status == p4gf_init_repo.INIT_REPO_OK:
                repo_created = True
            elif init_repo_status == p4gf_init_repo.INIT_REPO_EXISTS:
                repo_created = False
            elif init_repo_status == p4gf_init_repo.INIT_REPO_NOVIEW:
                start_response(_('404 Not Found'), headers)
                return [sink.readall()]
            else:
                start_response(_('500 Internal Server Error'), headers)
                return [b"Repository initialization failed\n"]

            # If authorization came from default, not explicit group
            # membership, copy that authorization to a group now. Could
            # not do this until after p4gf_init_repo() has a chance to
            # create not-yet-existing groups.
            if view_perm:
                view_perm.write_if(p4)

            # Now that we have valid git-fusion-user and
            # git-fusion-<view> client, replace our temporary P4
            # connection with a more permanent Context, shared for the
            # remainder of this process.
            with p4gf_context.create_context(view_name, view_lock) as ctx:
                LOG.debug("reconnected to P4, p4gf=%s", ctx.p4gf)
                ctx.log_context()

                # cd into the work directory. Not all git functions react well
                # to --work-tree=xxxx.
                cwd = os.getcwd()
                os.chdir(ctx.view_dirs.GIT_WORK_TREE)

                # Only copy from Perforce to Git if no other process is cloning
                # from this Git repo right now.
                shared_in_progress = p4gf_lock.shared_host_view_lock_exists(ctx.p4, view_name)
                if not shared_in_progress:
                    # Copy any recent changes from Perforce to Git.
                    try:
                        LOG.debug("bare: No git-upload-pack in progress, force non-bare"
                                  " before update Git from Perforce.")
                        p4gf_git.set_bare(False)
                        p4gf_copy_p2g.copy_p2g_ctx(ctx)
                        p4gf_init_repo.process_imports(ctx)

                        # Now is also an appropriate time to clear out any stale Git
                        # Swarm reviews. We're pre-pull, pre-push, time when we've
                        # got exclusive write access to the Git repo,
                        GSReviewCollection.delete_refs_for_closed_reviews(ctx)

                    except p4gf_lock.LockCanceled as lc:
                        LOG.warning(str(lc))
                    except:
                        # Dump failure to log, BEFORE cleanup, just in case
                        # cleanup ALSO fails and throws its own error (which
                        # happens if we're out of memory).
                        LOG.error(traceback.format_exc())

                        if repo_created:
                            # Return to the original working directory to allow the
                            # config code to call os.getcwd() without dying, since
                            # we are about to delete the current working directory.
                            os.chdir(cwd)
                            p4gf_server_common.cleanup_client(ctx, view_name)
                        raise

                try:
                    exclusive = 'upload' not in command
                    is_push   = 'upload' not in command
                    git_caller = functools.partial(_call_git, input_name, environ, ctx)
                    p4gf_call_git.call_git(git_caller, ctx, view_name, view_lock, exclusive)
                    if is_push:
                        GSReviewCollection.post_push(ctx)
                except p4gf_atomic_lock.LockConflict as lc:
                    start_response(_('500 Internal Server Error'), headers)
                    return ["{}".format(lc).encode('UTF-8')]

        p4gf_gc.process_garbage('at end of auth_server')
        if LOG.isEnabledFor(logging.DEBUG):
            end_time = time.time()
            frm = NTR('Runtime: preparation {} ms, lock acquisition {} ms, processing {} ms')
            LOG.debug(frm.format(before_lock_time - start_time,
                                after_lock_time - before_lock_time,
                                end_time - after_lock_time))
        return []
# pylint: enable=R0914, R0915


def _handle_cgi():
    """
    Respond to the incoming CGI request by wrapping it in something
    akin to a WSGI environment, but with lighter requirements when
    it comes to how and when data is written to the client.
    """
    #
    # In a web server, such as Apache, stdout is redirected to the client,
    # while stderr is written to the server logs, and stdin is supplied by
    # the client.
    #
    headers_set = []
    headers_sent = []

    def wsgi_to_bytes(s):
        """Convert a string to bytes using iso-8859-1 encoding."""
        return s.encode('iso-8859-1')

    def write(data):
        """Ensure headers are written to the client before data."""
        out = sys.stdout.buffer
        if headers_set and not headers_sent:
            # Before the first output, send the stored headers
            # pylint:disable=W0632
            status, response_headers = headers_sent[:] = headers_set
            # pylint:enable=W0632
            out.write(wsgi_to_bytes(NTR('Status: %s\r\n') % status))
            for header in response_headers:
                out.write(wsgi_to_bytes('%s: %s\r\n' % header))
            out.write(wsgi_to_bytes('\r\n'))
        if isinstance(data, str):
            data = wsgi_to_bytes(data)
        out.write(data)
        out.flush()

    def start_response(status, response_headers, exc_info=None):
        """Set the status and headers that will be sent to the client."""
        if exc_info:
            try:
                if headers_sent:
                    # Re-raise original exception if headers sent
                    raise exc_info[1].with_traceback(exc_info[2])
            finally:
                # avoid dangling circular ref
                exc_info = None
        elif headers_set:
            raise AssertionError(_('Headers already set!'))
        headers_set[:] = [status, response_headers]
        # Note: error checking on the headers should happen here, *after*
        # the headers are set. That way, if an error occurs, start_response
        # can only be re-called with exc_info set.
        return write

    def _read(in_file):
        """
        In a hosted CGI environment, the matter of content-length and
        transfer-encoding is handled for us by the server. We simply read
        the input until the EOF is encountered.
        """
        stdin_fd, stdin_name = tempfile.mkstemp(prefix='http-client-input-')
        LOG.debug('_handle_cgi() writing stdin to {}'.format(stdin_name))
        with closing(open(stdin_fd, 'wb')) as stdin_fobj:
            shutil.copyfileobj(in_file, stdin_fobj)
        return stdin_name

    # Set up a WSGI-like environment for our pseudo-WSGI application.
    environ = wsgiref.handlers.read_environ()
    environ['wsgi.version'] = (1, 0)
    environ['wsgi.multithread'] = False
    environ['wsgi.multiprocess'] = True
    environ['wsgi.run_once'] = True
    if environ.get('HTTPS', NTR('off')) in (NTR('on'), '1'):
        environ['wsgi.url_scheme'] = 'https'
    else:
        environ['wsgi.url_scheme'] = 'http'
    # Set up the output streams for Git to write to, using the unbuffered
    # binary stream instead of the text wrapper typically supplied. We
    # duplicate the standard output stream because the application will be
    # redirecting sys.stdout to avoid clobbering Git's own output.
    stdout_fobj = open(os.dup(1), 'wb')
    environ['wsgi.output'] = stdout_fobj
    environ['wsgi.errors'] = sys.stderr
    # Within a web server, content is decoded and read properly for us, we
    # just need to read until EOF is encountered.
    environ['wsgi.input'] = _read(sys.stdin.buffer)

    def proc_caller(*args, **kwargs):
        """Delegates to p4gf_proc.call() without much fanfare."""
        return p4gf_proc.call(*args, **kwargs)
    environ['proc.caller'] = proc_caller

    # lighttpd is not entirely compliant with RFC 3875 in that
    # QUERY_STRING is not set to the empty string as required
    # (http://redmine.lighttpd.net/issues/1339).
    if 'QUERY_STRING' not in environ:
        environ['QUERY_STRING'] = ''
    result = None
    try:
        # Invoke our WSGI application within the context of CGI.
        result = _wsgi_app(environ, start_response)
        for data in result:
            # don't send headers until body appears
            if data:
                write(data)
        if not headers_sent:
            # send headers now if body was empty
            write('')
# pylint:disable=W0703
    except Exception:
        LOG.error(traceback.format_exc())
        result = [b'Error, see the Git Fusion log']
# pylint:enable=W0703
    finally:
        stdout_fobj.close()
# pylint:disable=E1101
        if hasattr(result, 'close'):
            result.close()
# pylint:enable=E1101


def _app_wrapper(user, environ, start_response):
    """
    WSGI wrapper to our WSGI/CGI hybrid application.
    """
    #
    # In our simple web server environment, the standard input and output
    # streams are connected to the console running this script.
    #
    wsgiref.util.setup_testing_defaults(environ)
    if 'REMOTE_USER' not in environ:
        environ['REMOTE_USER'] = user
    log_var = p4gf_const.P4GF_TEST_LOG_CONFIG_PATH
    if log_var not in environ and log_var in os.environ:
        environ[log_var] = os.environ[log_var]

    def proc_caller(*args, **kwargs):
        """
        Hack the arguments to subprocess.call() so the output of Git
        will be directed to our socket rather than the console. Need
        to use subprocess directly so the file descriptor will be
        inherited by the child.
        """
        stdout = environ['wsgi.output']
        fobj = None
        if 'stdin' in kwargs:
            fobj = open(kwargs['stdin'], 'rb')
            kwargs['stdin'] = fobj
        kwargs['close_fds'] = False
        kwargs['stdout'] = stdout
        try:
            # Git client is not happy without the status line. Since we are
            # the origin server, output the first few lines that are
            # expected in a standard HTTP response.
            handler = environ['wsgi.handler']
            handler.send_response(200, 'OK')
            handler.flush_headers()
            return subprocess.call(*args, **kwargs)
        finally:
            if fobj:
                fobj.close()
    environ['proc.caller'] = proc_caller

    def _read_input():
        """
        Read from the client connection and return the name of a temporary
        file containing the contents of what was read. If the request method
        was not POST or PUT, or the Content-Length was zero (or undefined),
        then nothing is read from the connection, resulting in an empty file.
        """
        # For more information on the idiosyncrasies within WSGI 1.0, see
        # http://blog.dscpl.com.au/2009/10/details-on-wsgi-10-amendmentsclarificat.html
        stdin = environ['wsgi.input']
        try:
            content_length = int(environ.get('CONTENT_LENGTH', 0))
        except ValueError:
            content_length = 0
        method = environ['REQUEST_METHOD']
        stdin_fd, stdin_name = tempfile.mkstemp(prefix='http-client-input-')
        # TODO: need to handle case of "Transfer-Encoding: chunked"
        if content_length and (method == "POST" or method == "PUT"):
            LOG.debug('_app_wrapper() writing stdin to {}'.format(stdin_name))
            with closing(open(stdin_fd, 'wb')) as stdin_fobj:
                # To avoid blocking forever reading input from the client, must
                # read _only_ the number of bytes specified in the request
                # (which coincidently permits HTTP/1.1 keep-alive connections)
                while content_length > 0:
                    length = min(16 * 1024, content_length)
                    buf = stdin.read(length)
                    if not buf:
                        break
                    stdin_fobj.write(buf)
                    content_length -= len(buf)
        return stdin_name

    # Read the input from the client.
    environ['wsgi.input'] = _read_input()

# pylint:disable=W0703
    try:
        result = _wsgi_app(environ, start_response)
    except Exception:
        LOG.error(traceback.format_exc())
        result = [b'Error, see the Git Fusion log']
    return result
# pylint:enable=R0912,W0703

# pylint:disable=R0904
class GitFusionHandler(wsgiref.handlers.SimpleHandler):
    """
    A WSGI handler that allows for sending data that already includes
    relevant headers, without the need for calling start_response.
    """

    def setup_environ(self):
        """
        Insert additional properties into the environment.
        """
        wsgiref.handlers.BaseHandler.setup_environ(self)
        # Provide the means for our customized WSGI application to write
        # back directly to the client, circumventing the WSGI Python code.
        self.environ['wsgi.output'] = self.stdout
        self.environ['wsgi.handler'] = self.request_handler

    def finish_content(self):
        """
        Only finish the content if the headers have been set.
        """
        if self.headers:
            wsgiref.handlers.SimpleHandler.finish_content(self)

    def write(self, data):
        """
        Write whatever is given, sending status and headers if they have
        been provided, otherwise assume they have been provided and pass
        through to _write().
        """
        if self.status and not self.headers_sent:
            # Before the first output, send the stored headers
            self.send_headers()
        self.headers_sent = True
        self._write(data)
        self._flush()


class GitFusionRequestHandler(wsgiref.simple_server.WSGIRequestHandler):
    """
    In order to make writing headers optional, need to override this class
    and set up our own handler, defined above.
    """

    def handle(self):
        """
        Prepare to handle an incoming request.
        """
        # Identical to WSGIRequestHandler.handle() except for the
        # construction of the handler.
        self.raw_requestline = self.rfile.readline()
        if not self.parse_request():
            # An error code has been sent, just exit
            return

        handler = GitFusionHandler(
            self.rfile, self.wfile, self.get_stderr(), self.get_environ()
        )
        handler.request_handler = self
        handler.run(self.server.get_app())
# pylint:enable=R0904


def main():
    """
    Parse command line arguments and decide what should be done.
    """
    desc = _("""p4gf_http_server.py handles http(s) requests. Typically it
is run via a web server and protected by some form of user
authentication. The environment variable REMOTE_USER must be set to
the name of a valid Perforce user, which is taken to be the user
performing a pull or push operation.
""")
    epilog = _("""If the --user argument is given then a simple HTTP server
will be started, listening on the port specified by --port. The
REMOTE_USER value will be set to the value given to the --user
argument. To stop the server, send a terminating signal to the process.
""")
    log_l10n()
    parser = p4gf_util.create_arg_parser(desc, epilog=epilog)
    parser.add_argument('-u', '--user',
                        help=_('value for REMOTE_USER variable'))
    parser.add_argument('-p', '--port', type=int, default=8000,
                        help=_('port on which to listen (default 8000)'))
    args = parser.parse_args()
    if args.user:
        LOG.debug("Listening for HTTP requests on port {} as user {}, pid={}".format(
            args.port, args.user, os.getpid()))
        wrapper = functools.partial(_app_wrapper, args.user)
        httpd = wsgiref.simple_server.make_server('', args.port, wrapper,
            handler_class=GitFusionRequestHandler)
        print(_('Serving on port {}...').format(args.port))

        def _signal_handler(signum, _frame):
            """
            Ensure the web server is shutdown properly.
            """
            LOG.info("received signal {}, pid={}, exiting".format(signum, os.getpid()))
            httpd.server_close()
            sys.exit(0)
        LOG.debug("installing HTTP server signal handler, pid={}".format(os.getpid()))
        signal.signal(signal.SIGHUP, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGQUIT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGTSTP, _signal_handler)

        p4gf_proc.install_stack_dumper()
        httpd.serve_forever()
    else:
        # Assume we are running inside a web server...
        _handle_cgi()


if __name__ == "__main__":
    # Get the logging configured properly...
    with p4gf_log.ExceptionLogger(squelch=False, write_to_stderr_=True):
        try:
            main()
        # pylint:disable=W0703
        except Exception:
            LOG.error(traceback.format_exc())
        # pylint:enable=W0703
