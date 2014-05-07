#! /usr/bin/env python3.3
"""
p4gf_auth_server.py

A shell replacement that ssh invokes to run push or pull commands on the
Git Fusion server.

Arguments:
--user=p4user  required  which Perforce user account is the pusher/puller
--keyfp=<key>  required  SSH key fingerprint of key used to authenticate
<command>      required  one of git-upload-pack or git-receive-pack
                         no other commands permitted

Record the request, along with p4user and key fingerprint and requested
git command, to an audit log.

Run the appropriate protocol interceptor for git-upload-pack or
git-receive-pack.

Reject attempt if p4user lacks read privileges for the entire view.

Reject unknown git command

"""
import argparse
import functools
import logging
import os
import re
import sys
import time
import traceback

import p4gf_env_config  # pylint: disable=W0611
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
import p4gf_util
import p4gf_version
import p4gf_atomic_lock
import p4gf_translate

LOG = p4gf_log.for_module()


def _log_environ(environ):
    """Dump our environment to the log at DEBUG3 level."""
    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3("SSH environment:")
        keys = sorted(environ.keys())
        for name in keys:
            LOG.debug3("    {}: {}".format(name, environ[name]))


def illegal_option(option):
    """Trying to sneak a shell command into my world? Please do not do that."""
    if ';' in option:
        return True

    # git-upload-pack only understands --strict and --timeout=<n>.
    # git-receive-pack understands no options at all.
    re_list = [ re.compile("^--strict$"),
                re.compile(r"^--timeout=\d+$")]
    for reg in re_list:
        if reg.match(option):
            return False
    return True


def is_special_command(view):
    """See if view is actually a special command masquerading as a view"""
    if view.startswith(p4gf_const.P4GF_UNREPO_FEATURES+'@'):
        return True
    return view in p4gf_const.P4GF_UNREPO


def parse_args(argv):
    """Parse the given arguments into a struct and return it.

    On error, print error to stdout and return None.

    If unable to parse, argparse.ArgumentParser.parse_args() will exit,
    so we add our own exit() calls, too. Otherwise some poor unsuspecting
    programmer would see all our "return None" calls and think that
    "None" is the only outcome of a bad argv.
    """

    # pylint:disable=C0301
    # line too long? Too bad. Keep tabular code tabular.
    parser = p4gf_util.create_arg_parser(_("Records requests to audit log, performs only permitted requests."),
                            usage=_("usage: p4gf_auth_server.py [-h] [-V] [--user] [--keyfp] git-upload-pack | git-receive-pack [options] <view>"))
    parser.add_argument(NTR('--user'),  metavar="",                           help=_('Perforce user account requesting this action'))
    parser.add_argument(NTR('--keyfp'), metavar="",                           help=_('ssh key used to authenticate this connection'))
    parser.add_argument(NTR('command'), metavar=NTR("command"), nargs=1,      help=_('git-upload-pack or git-receive-pack, plus options'))
    parser.add_argument(NTR('options'), metavar="", nargs=argparse.REMAINDER, help=_('options for git-upload-pack or git-receive-pack'))

    # reverse git's argument modifications
    # pylint:disable=W1401
    # raw strings don't play well with this lambda function.
    fix_arg = lambda s: s.replace("'\!'", "!").replace("'\\''", "'")
    argv = [fix_arg(arg) for arg in argv]
    args = parser.parse_args(argv)

    if not args.command[0] in p4gf_server_common.COMMAND_TO_PERM:
        raise p4gf_server_common.CommandError(_("Unknown command '{bad}', must be one of '{good}'.")
                           .format(bad=args.command[0],
                                   good = "', '".join(p4gf_server_common.COMMAND_TO_PERM.keys())),
                           usage = parser.usage)

    if not args.options:
        raise p4gf_server_common.CommandError(_("Missing directory in '{cmd}' <view>")
                           .format(cmd=args.command[0]))

    # Carefully remove quotes from any view name, allowing for imbalanced quotes.
    view_name = args.options[-1]
    if view_name[0] == '"' and view_name[-1] == '"' or\
            view_name[0] == "'" and view_name[-1] == "'":
        view_name = view_name[1:-1]
    # Allow for git+ssh URLs where / separates host and repository.
    if view_name[0] == '/':
        view_name = view_name[1:]
    args.options[-1] = view_name

    # Reject impossible view names/client spec names
    if not is_special_command(view_name) and not view_name.startswith('@') and not p4gf_util.is_legal_view_name(view_name):
        raise p4gf_server_common.CommandError(_("Illegal view name '{}'").format(view_name))

    for o in args.options[:-1]:
        if illegal_option(o):
            raise p4gf_server_common.CommandError(_("Illegal option: '{}'").format(o))

    # Require --user if -V did not early return.
    if not args.user:
        raise p4gf_server_common.CommandError(_("--user required."),
                           usage=parser.usage)

    # LOG.warn("running command: {}\n".format(argv)) ### DEBUG REMOVE FOR GA

    return args
    # pylint:enable=C0301


def read_motd():
    '''
    If there is a message of the day file, return its contents.
    If not, return None.
    '''
    p4gf_dir = p4gf_const.P4GF_HOME
    motd_file_path = p4gf_const.P4GF_MOTD_FILE.format(P4GF_DIR=p4gf_dir)
    if not os.path.exists(motd_file_path):
        return None
    with open(motd_file_path, 'r') as f:
        content = f.read()
    return content


def write_motd():
    '''
    If there is a .git-fusion/motd.txt file, return it on stderr.
    '''
    motd = read_motd()
    if motd:
        sys.stderr.write(motd)


def _call_git(args, ctx):
    """
    Invoke the git command, returning its exit code.

    Arguments:
        args -- parsed command line arguments object.
        ctx -- context object.
    """
    # Pass to git-upload-pack/git-receive-pack. But with the view
    # converted to an absolute path to the Git Fusion repo.
    converted_argv = args.options[:-1]
    converted_argv.append(ctx.view_dirs.GIT_DIR)
    cmd_list = args.command + converted_argv
    return p4gf_proc.call(cmd_list)


# pylint: disable=R0915, R0912
def main(poll_only=False):
    """set up repo for a view
       view_name_git    is the untranslated repo name
       view_name        is the translated repo name
    """
    p4gf_proc.install_stack_dumper()
    _log_environ(os.environ)
    with p4gf_server_common.ExceptionAuditLogger()\
    , p4gf_create_p4.Closer():
        LOG.debug(p4gf_log.memory_usage())
        start_time = time.time()
        args = parse_args(sys.argv[1:])
        if not args:
            return 1

        is_push = 'upload' not in args.command[0]

        # Record the p4 user in environment. We use environment to pass to
        # git-invoked hook. We don't have to set ctx.authenticated_p4user because
        # Context.__init__() reads it from environment, which we set here.
        os.environ[p4gf_const.P4GF_AUTH_P4USER] = args.user

        # view_name_git    is the untranslated repo name
        # view_name        is the translated repo name

        # print "args={}".format(args)
        view_name_git = args.options[-1]
        # translate '/' ':' ' '  .. etc .. for internal view_name
        view_name = p4gf_translate.TranslateReponame.git_to_repo(view_name_git)
        LOG.debug("public view_name: {0}   internal view_name: {1}".
                format(view_name_git, view_name))


        p4gf_util.reset_git_enviro()
        p4 = p4gf_create_p4.create_p4()
        if not p4:
            return 2
        LOG.debug("connected to P4: %s", p4)

        p4gf_server_common.check_readiness(p4)

        p4gf_server_common.check_lock_perm(p4)

        if not p4gf_server_common.check_protects(p4):
            p4gf_server_common.raise_p4gf_perm()

        if p4gf_server_common.run_special_command(view_name, p4, args.user):
            return 0

        # Initialize the external process launcher early, before allocating lots
        # of memory, and just after all other conditions have been checked.
        p4gf_proc.init()
        # Prepare for possible spawn of GitMirror worker process by forking
        # now before allocating lots of memory.
        p4gf_gitmirror.setup_spawn(view_name)
        # Kick off garbage collection debugging, if enabled.
        p4gf_gc.init_gc()

        if poll_only:
            view_perm = None
        else:
            # Go no further, create NOTHING, if user not authorized.
            # We use the translated internal view name here for perm authorization
            required_perm = p4gf_server_common.COMMAND_TO_PERM[args.command[0]]
            view_perm = p4gf_group.ViewPerm.for_user_and_view(p4, args.user,
                        view_name, required_perm)
            p4gf_server_common.check_authorization(p4, view_perm, args.user, args.command[0],
                                                   view_name)

        # Create Git Fusion server depot, user, config. NOPs if already created.
        p4gf_init.init(p4)

        write_motd()

        # view_name is the internal view_name (identical when notExist special chars)
        before_lock_time = time.time()
        with p4gf_lock.view_lock(p4, view_name) as view_lock:
            after_lock_time = time.time()

            # Create Git Fusion per-repo client view mapping and config.
            #
            # NOPs if already created.
            # Create the empty directory that will hold the git repo.
            init_repo_status = p4gf_init_repo.init_repo(p4, view_name, view_lock)
            if init_repo_status == p4gf_init_repo.INIT_REPO_OK:
                repo_created = True
            elif init_repo_status == p4gf_init_repo.INIT_REPO_EXISTS:
                repo_created = False
            else:
                return 1

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

                # Find directory paths to feed to git.
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

                if poll_only:
                    code = os.EX_OK
                else:

                    git_caller = functools.partial(_call_git, args, ctx)
                    try:

                        # Deep in call_git(), we grab an 'p4 reviews' lock on
                        # ctx.clientmap's LHS. Switch that clientmap to our
                        # full union view to prevent simultaneous 'git push'es
                        # from clobbering each other in some shared depot
                        # branch. Must include all lightweight branches, too.
                        ctx.switch_client_view_to_union()

                        exclusive = 'upload' not in args.command[0]
                        code = p4gf_call_git.call_git(
                                git_caller, ctx, view_name, view_lock, exclusive)
                        if is_push:
                            GSReviewCollection.post_push(ctx)
                    except p4gf_atomic_lock.LockConflict as lc:
                        sys.stderr.write("{}\n".format(lc))
                        code = os.EX_SOFTWARE

            p4gf_gc.process_garbage(NTR('at end of auth_server'))
            if LOG.isEnabledFor(logging.DEBUG):
                end_time = time.time()
                frm = NTR("Runtime: preparation {} ms, lock acquisition {} ms,"
                          " processing {} ms")
                LOG.debug(frm.format(before_lock_time - start_time,
                                    after_lock_time - before_lock_time,
                                    end_time - after_lock_time))
        return code


#pylint:disable=E0602
#pylint does not know about BrokenPipeError
def main_ignores():
    """
    Calls main() while ignoring certain exceptions that we cannot do anything
    with and are best served by concisely logging their occurrence.
    """
    try:
        return main()
    except BrokenPipeError:
        LOG.warn("client connection terminated?")
#pylint:enable=E0602


if __name__ == "__main__":
    # Ensure any errors occurring in the setup are sent to stderr, while the
    # code below directs them to stderr once rather than twice.
    try:
        with p4gf_log.ExceptionLogger(squelch=False, write_to_stderr_=True):
            p4gf_log.record_argv()
            p4gf_version.log_version()
            log_l10n()
            p4gf_version.version_check()
    # pylint: disable=W0702
    except:
        # Cannot continue if above code failed.
        sys.exit(1)
    # main() already writes errors to stderr, so don't let logger do it again
    p4gf_log.run_with_exception_logger(main_ignores, write_to_stderr=False)
