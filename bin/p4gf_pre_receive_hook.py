#! /usr/bin/env python3.3

'''Called by git after 'git push' has transferred one or more commits
along with their tree and blob objects, but before git moves the head
pointer to the end of the newly transferred commits.

Pass control to p4gf_copy_to_p4.py to copy pending git commits into Perforce.
Fail with error if any git commit collides with a Perforce commit, git
user must pull, rebase, and re-attempt push.

This file must be copied or symlinked into .git/hooks/pre-receive
'''

import logging
import os
import sys

import p4gf_env_config    # pylint: disable=W0611
from p4gf_branch_id import Assigner, PreReceiveTuple
import p4gf_call_git
import p4gf_context
import p4gf_create_p4
import p4gf_gc
from   p4gf_git_swarm import GSReviewCollection
import p4gf_gitmirror
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_lock
import p4gf_log
import p4gf_proc
import p4gf_tag
import p4gf_util
import p4gf_version
import p4gf_copy_to_p4

LOG = logging.getLogger('p4gf_pre_receive_hook')


def _copy( ctx              # P4GF context
         , prl              # list of pushed PreReceiveTuple elements
         , assigner         # commit-to-branch assignments
         , gsreview_coll    # git-swarm review collection GSReviewCollection
         ):
    """Copy a sequence of commits from git to Perforce.

    Returns error message, or None if okay.
    """
    branch_dict = ctx.branch_dict()
    LOG.debug2('allowing branch creation: {}'.format(ctx.branch_creation))

    for prt in prl:
        LOG.debug("copy: current branch_dict {0}".format(branch_dict))
        LOG.debug("copy {0}".format(prt))
        if not prt.ref.startswith('refs/heads/'):
            # Do not process tags at this point.
            continue
        branch = is_gitref_in_gf(prt.ref, branch_dict, is_lightweight=False)
        if not ctx.branch_creation and not branch:
            msg = (_("Branch creation is not authorized for this repo."
                    "\nRejecting push of '{0}'.")
                  .format(prt.ref))
            return msg
        err = p4gf_copy_to_p4.copy_git_changes_to_p4(
                      ctx
                    , prt           = prt
                    , assigner      = assigner
                    , gsreview_coll = gsreview_coll )
        if err:
            return err
    return None


def _delete(ctx, prl):
    """Delete a git branch from GF config files.

    Arguments:
        ctx -- P4GF context
        prl -- list of pre-receive "tuples"

    Returns error message, or None if okay.
    """
    branch_dict = ctx.branch_dict()
    for prt in prl:
        LOG.debug("delete {0}".format(prt))
        if not prt.ref.startswith('refs/heads/'):
            # Do not process tags at this point.
            LOG.debug("skipping {0}".format(prt.ref))
            continue
        branch = is_gitref_in_gf(prt.ref, branch_dict, is_lightweight=True)
        if not branch:
            # Branch is not known to Git Fusion branch, so we've nothing
            # more to do. But branch might still be in the Git repo. Let
            # Git sort that out, report its own Git errors if necessary.
            continue

        if branch.is_lightweight:
            ctx.mirror.delete_branch_config(ctx, branch)
            continue

                        # Should never happen.
        return (NTR('BUG: p4gf_pre_receive_hook.main() must not permit'
                   ' a delete of fully populated branch {0}')
                .format(prt.ref))
    return None


def is_gitref_in_gf(ref, branch_dict, is_lightweight=True):
    """Return branch object if is git ref in already in GF"""
    git_branch_name = ref[len('refs/heads/'):]
    LOG.debug("is_gitref_in_gf: branch name {} ".format(git_branch_name))
    branch = None
    for b in branch_dict.values():
        if ( b.git_branch_name == git_branch_name
                and b.is_lightweight == is_lightweight
                and not b.deleted):
            branch = b
            break
    return branch


def _clean_exit(err):
    """
    Perform a clean exit of the process by bleeding the standard input,
    printing the given error message, and returning an exit code.
    """
    sys.stdin.readlines()
    print(str(err))
    LOG.error(str(err))
    return 1


def _log_environ(environ):
    """
    Dump our environment to the log at DEBUG3 level.
    """
    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3("pre-receive environment:")
        keys = sorted(environ.keys())
        for name in keys:
            LOG.debug3("    {}: {}".format(name, environ[name]))


#pylint: disable=R0912,R0915
def main():
    """Copy incoming Git commits to Perforce changelists."""
    _log_environ(os.environ)
    log_l10n()
    LOG.debug("main() running, pid={}".format(os.getpid()))
    p4gf_proc.install_stack_dumper()
    for h in ['-?', '-h', '--help']:
        if h in sys.argv:
            print(_('Git Fusion pre-receive hook.'))
            return 2
    with p4gf_create_p4.Closer():
        p4gf_version.print_and_exit_if_argv()
        p4 = p4gf_create_p4.create_p4()
        if not p4:
            return 2
        p4gf_util.reset_git_enviro(p4)

        view_name = p4gf_util.cwd_to_view_name()
        view_lock = p4gf_lock.view_lock_heartbeat_only(p4, view_name)
        with p4gf_context.create_context(view_name, view_lock) as ctx:

            # this script is called by git while a context and temp clients
            # are already in use.  Don't sabotage that context by deleting
            # the temp clients from here.
            ctx.cleanup_client_pool = False

            # Read each input line (usually only one unless pushing multiple branches)
            # and convert to a list of "tuples" from which we can assign branches.
            prl = []
            delete_prl = []
            while True:
                line = sys.stdin.readline()
                if not line:
                    break
                LOG.debug('main() raw pre-receive-tuple: {}'.format(line))
                prt = PreReceiveTuple.from_line(line)
                if int(prt.new_sha1, 16) == 0:
                    delete_prl.append(prt)
                else:
                    prl.append(prt)

            # Initialize the external process launcher early, before allocating lots
            # of memory, and just after all other conditions have been checked.
            p4gf_proc.init()
            # Prepare for possible spawn of GitMirror worker process by forking
            # now before allocating lots of memory.
            p4gf_gitmirror.setup_spawn(view_name)
            # Kick off garbage collection debugging, if enabled.
            p4gf_gc.init_gc()

            # Reject attempt to delete any fully populated branch defined in
            # p4gf_config. Git Fusion never edits p4gf_config, so Git Fusion never
            # deletes fully populated branches. Edit p4gf_config yourself if you
            # want to remove a branch from history.
            for prt in delete_prl:
                git_branch_name = prt.git_branch_name()
                if not git_branch_name:
                    continue
                branch = ctx.git_branch_name_to_branch(git_branch_name)
                if not branch:
                    LOG.debug('attempt to delete branch {} which does not exist'
                              .format(git_branch_name))
                    break
                if not branch.is_lightweight:
                    raise RuntimeError(_('Cannot delete branches defined in'
                                         ' Git Fusion repo config file: {}')
                                       .format(git_branch_name))

            # Swarm review creates new Git merge commits. Must occur before branch
            # assignment so that the review reference can be moved to the new merge
            # commit.
            gsreview_coll = GSReviewCollection.from_prl(ctx, prl)
            if gsreview_coll:
                gsreview_coll.pre_copy_to_p4(prl)

            # Assign branches to each of the received commits for pushed branches  - skip deletes.
            if prl:
                assigner = Assigner(ctx.branch_dict(), prl, ctx)
                assigner.assign()

            # For each of the heads being pushed, copy their commits to Perforce.
            if prl:
                try:
                    err = _copy( ctx
                               , prl           = prl
                               , assigner      = assigner
                               , gsreview_coll = gsreview_coll)   # branch push
                    if err:
                        return _clean_exit(err)
                except RuntimeError as err:
                    # Log the error. The return call below eats the error and stack trace.
                    LOG.exception(NTR("_copy() raised exception."))
                    return _clean_exit(err)
            # For each of the heads being deleted, remove the branch definition from p4gf_config2
            if delete_prl:
                p4gf_call_git.prohibit_interrupt(view_name, os.getpid())
                try:
                    err = _delete(ctx, delete_prl)     # branch delete
                    if err:
                        return _clean_exit(err)
                except RuntimeError as err:
                    # Log the error. The return call below eats the error and stack trace.
                    LOG.exception(NTR("_delete() raised exception."))
                    return _clean_exit(err)
            # Process all of the tags at once.
            err = p4gf_tag.process_tags(ctx, prl + delete_prl)
            if err:
                return _clean_exit(err)

                            # If we have any new Git Swarm review references that
                            # auth/http_server must rename, send a list of such
                            # references across process boundary, via a file.
            if gsreview_coll:
                gsreview_coll.to_file()

            p4gf_gc.process_garbage("at end of pre_receive_hook")
            p4gf_gc.report_objects(NTR("at end of pre_receive_hook"))

        return 0
#pylint: enable=R0912,R0915

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)
