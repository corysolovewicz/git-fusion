#! /usr/bin/env python3.3
"""Copy change history from Perforce to git.

View must already be defined in Perforce: it must have its
"git-fusion-<view>" client with a Root and a View.

Git repo must already be inited in view_dirs.GIT_DIR.
The repo can be empty.
"""

import shutil
import os
import sys

import pygit2

import p4gf_branch
import p4gf_const
import p4gf_copy_to_git
from   p4gf_l10n import _, NTR
import p4gf_log
import p4gf_path
import p4gf_proc
import p4gf_util

LOG = p4gf_log.for_module()


def _p4_empty(ctx):
    """
    Is our client view completely empty, no files, not even deleted or purged?
    """
    ### wanted to avoid doing the expensive client-creation-view-switching thing
    ### and use the code below, but couldn't work around the spaces-in-paths issue
    # p4map = p4gf_branch.calc_branch_union_client_view(ctx.config.p4client, ctx.branch_dict())
    # cmd = ['files', '-m1'] + p4map.lhs()
    # r = p4gf_util.p4run_logged(ctx.p4, cmd)
    with ctx.switched_to_union():
        r = ctx.p4.run('files', '-m1', p4gf_path.slash_dot_dot_dot(ctx.p4.client))
    return not r


def _has_commits(ctx):
    ''' Return True if this repo has commits on at least one named branch.'''
    branch_dict = ctx.branch_dict()
    for v in branch_dict.values():
        if not v.git_branch_name:
            continue
        if p4gf_util.sha1_for_branch(v.git_branch_name):
            return True
    return False


def copy_p2g(ctx, start):
    """Fill git with content from Perforce."""

    view_name = ctx.config.view_name
    view_dirs = ctx.view_dirs
    git_dir = view_dirs.GIT_DIR
    if not os.path.exists(git_dir):
        LOG.warn("mirror Git repository {} missing, recreating...".format(git_dir))
        # it's not the end of the world if the git repo disappears, just recreate it
        create_git_repo(ctx, git_dir)

    # If Perforce client view is empty and git repo is empty, someone is
    # probably trying to push into an empty repo/perforce tree. Let them.
    if _p4_empty(ctx) and p4gf_util.git_empty():
        LOG.info("Nothing to copy from empty view {}".format(view_name))
        return

    # We're not empty anymore, we no longer need this to avoid
    # git push rejection of push to empty repo refs/heads/master.
    delete_empty_repo_branch(ctx, view_dirs.GIT_DIR)

    # Remove branches from git which may have been deleted by another GF instance
    # Get list of new branches add to GF (by git or P4) by another GF instance -
    # for which this local git repo will yet have no branch ref
    copy_new_branches_to_git = synchronize_git_with_gf_branches(ctx)
    if copy_new_branches_to_git:
        LOG.debug("Found new GF branches to add to git:{0}".format(copy_new_branches_to_git))
    start_at = None
    if start is not None:
        # Does this git need to add a new GF branch
        # or does any branch in our branch dict contain a commit?
        has_commits = copy_new_branches_to_git or _has_commits(ctx)
        if has_commits:
            raise RuntimeError(_("Cannot use --start={start} when repo already has commits.")
                               .format(start=start))
    if start:
        start_at = "@{}".format(start)

    p4gf_copy_to_git.copy_p4_changes_to_git(ctx, start_at, "#head", copy_new_branches_to_git)
    ctx.checkout_master_ish()


def synchronize_git_with_gf_branches(ctx):
    '''
    Synchronize git named refs with named branches in p4config2.
    Remove branches from git which have been deleted by another GF instance.
    Return list of task branches in GF but not in git: branches in this
    list will be added to git by p4gf_copy_to_git.

    p4config2 contains a list of names task branches. If a branch is deleted
    it acquires "deleted = True" option. The same branch may be re-created
    with a new branch-id. We retain both branch definitions as there
    may be dependencies on the deleted branch_id. If one GF instance
    deletes a named task branch, then the current GF instance needs
    to notify git to remove the branch ref. We do this by
    examining the list of non-deleted branch definitions
    and remove from git any that are not found. If p4config2
    has been removed, we cannot determine that a branch
    needs to be deleted from git - so do nothing.
    '''
    cmd = [ 'git', 'branch']
    d = p4gf_proc.popen(cmd)
    git_branches = d['out'].replace('*','').splitlines()
    if not git_branches:       # no git branches - this must be during init repo
        return None
    p4_deleted_branch_names = [b.git_branch_name for b in ctx.branch_dict().values()
                            if b.git_branch_name and b.deleted]
    p4_active_branch_names_lw = [b.git_branch_name for b in ctx.branch_dict().values()
                            if b.git_branch_name and b.is_lightweight and not b.deleted]
    p4_branch_names_lw = [b.git_branch_name for b in ctx.branch_dict().values()
                            if b.git_branch_name and b.is_lightweight]
    p4_branch_names_non_lw = [b.git_branch_name for b in ctx.branch_dict().values()
                            if not b.is_lightweight]
    cmd = [ 'git', 'branch', '-D']
    git_branches_cleaned = []
    for branch in git_branches:
        if "(no branch)" in branch:
            continue
        branch = branch.split()
        i = 0
        if branch[0] == '*':
            i = 1
        if branch[i] in p4_branch_names_non_lw:
            git_branches_cleaned.append(branch[i])
            continue            # Do not delete non-lightweight
        if ( branch[i] in p4_deleted_branch_names
                and not branch[i] in p4_active_branch_names_lw):
            LOG.debug("Removing branch :{0}: from git".format(branch[i]))
            d = p4gf_proc.popen(cmd + [branch[i]])
        else:
            git_branches_cleaned.append(branch[i])
    # which branches are marked as deleted but have not been re-created
    really_deleted = [ b for b in p4_deleted_branch_names if b not in p4_active_branch_names_lw]
    git_branches_cleaned.extend(really_deleted)
    # Return list of LW branches in GF but not in git
    # Adding a new fully populated to p4gf_config needs testing.
    return [b for b in p4_branch_names_lw if b not in git_branches_cleaned]


def create_empty_repo_branch(ctx, git_dir):
    '''
    Create and switch to branch empty_repo.

    This avoids Git errors when pushing to a brand-new empty repo which
    prohibits pushes to master.

    We'll switch to master and delete this branch later, when there's
    something in the repo and we can now safely detach HEAD from master.
    '''
    master_ish = p4gf_branch.most_equal(ctx.branch_dict())
    for branch in [ master_ish.git_branch_name
                  , p4gf_const.P4GF_BRANCH_EMPTY_REPO]:
        p4gf_proc.popen(['git', '--git-dir=' + git_dir, 'checkout', '-b', branch])


def delete_empty_repo_branch(_ctx, git_dir):
    '''
    Delete branch empty_repo. If we are currently on that branch,
    detach head before switching.

    Only do this if our HEAD points to an actual sha1: we have to have
    at least one commit.
    '''
    p4gf_proc.popen_no_throw([ 'git', '--git-dir=' + git_dir
                             , 'checkout', 'HEAD~0'])
    p = p4gf_proc.popen_no_throw(['git', '--git-dir=' + git_dir
                                 , 'branch', '--list',
            p4gf_const.P4GF_BRANCH_EMPTY_REPO])
    if p['out']:
        p = p4gf_proc.popen_no_throw(['git', '--git-dir=' + git_dir
                                     , 'branch', '-D'
                                     , p4gf_const.P4GF_BRANCH_EMPTY_REPO])

def is_bare_git_repo(git_dir):
    '''
    Is this Git repo already loaded for --bare?
    '''
    ### this explodes the Python process, in p4api.so, oddly enough
    # repo = pygit2.Repository(git_dir)
    # return 'core.bare' in repo.config and repo.config['core.bare'] == 'true'
    cmd = ['git', '--git-dir=' + git_dir, 'config', '--get', NTR('core.bare')]
    result = p4gf_proc.popen_no_throw(cmd)
    return 'true' in result['out']


def hook_file_content():
    """Return the text of a script that can call our pre-receive hook."""

    lines = [NTR('#! /usr/bin/env bash'),
             NTR(''),
             NTR('export PYTHONPATH={bin_dir}:$PYTHONPATH'),
             NTR('{bin_dir}/{script_name}'),
             NTR('')]

    abs_path = os.path.abspath(__file__)
    bin_dir = os.path.dirname(abs_path)
    script_name = NTR('p4gf_pre_receive_hook.py')

    file_content = '\n'.join(lines).format(bin_dir=bin_dir,
                                           script_name=script_name)
    return file_content


def install_hook(git_dir):
    """Install Git Fusion's pre-receive hook"""

    hook_path = os.path.join(git_dir, NTR('hooks'), NTR('pre-receive'))
    with open (hook_path, 'w') as f:
        f.write(hook_file_content())
    os.chmod(hook_path, 0o755)    # -rwxr-xr-x


def create_git_repo(ctx, git_dir):
    """Create the git repository in the given root directory."""

    # Test if the Git repository has already been created.
    if os.path.exists(os.path.join(git_dir, 'HEAD')):
        return

    # Prepare the Git repository directory, cleaning up if necessary.
    work_tree = os.path.dirname(git_dir)
    if not os.path.exists(git_dir):
        if os.path.exists(work_tree):
            # weird case where git view dir exists but repo was deleted
            LOG.warn("mirror Git repository {} in bad state, repairing...".format(git_dir))
            shutil.rmtree(work_tree)
        LOG.debug("creating directory %s for Git repo", git_dir)
        os.makedirs(git_dir)

    # Initialize the Git repository for that directory.
    LOG.debug("creating Git repository in %s", git_dir)
    pygit2.init_repository(git_dir)

    # Configure the Git repository to avoid creating pack objects, so we can
    # read them directly ourselves and avoid the overhead of git-cat-file.
    cwd = os.getcwd()
    os.chdir(work_tree)
    settings = {
        # Prevent conflicting change history by disallowing rewinds.
        'receive.denyNonFastForwards': 'true',
        # # Turn off default compression (suspenders)
        # 'core.compression': '0',
        # # Turn off compression for loose objects (belt)
        # 'core.loosecompression': '0',
        # # threshold beyond which deltaCompression is compression is disabled
        # 'core.bigFileThreshold': '0',
        # # Packs smaller than this are unpacked into loose object files (belt)
        # 'fetch.unpackLimit': '1000000000',
        # Turn off garbage collection
        'gc.auto': '0',
        # Should never be used
        'gc.autopacklimit': '0',
        # Should never be used
        'pack.compression': '0',
        # git-receive-pack will not run gc
        'receive.autogc': 'false',
        # Packs smaller than this are unpacked into loose object files (belt)
        'receive.unpackLimit': '1000000000',
        # Packs smaller than this are unpacked into loose object files (suspenders)
        'transfer.unpackLimit': '1000000000'
    }
    # repo = pygit2.Repository(git_dir)
    for k, v in settings.items():
        ### this explodes the Python process, in p4api.so, oddly enough
        # repo.config[k] = v
        cmd = ['git', 'config', '--local', '--replace-all', k, v]
        result = p4gf_proc.popen_no_throw(cmd)
        if result['ec']:
            LOG.error("configuring git repo failed for {}={} => {}".format(
                k, v, result['err']))
            sys.stderr.write(_("error: git init failed with '{}' for '{}'\n").format(
                result['ec'], work_tree))
    os.chdir(cwd)

    install_hook(git_dir)

    # Don't bother changing branches in a --bare repo.
    if is_bare_git_repo(git_dir):
        return

    create_empty_repo_branch(ctx, git_dir)


def copy_p2g_ctx(ctx, start=None):
    """Using the given context, copy its view from Perforce to Git.

    Common code for p4gf_auth_server.py and p4gf_init_repo.py for setting up
    the eventual call to copy_p2g."""

    # cd into the work directory. Not all git functions react well to --work-tree=xxxx.
    os.chdir(ctx.view_dirs.GIT_WORK_TREE)

    # Fill git with content from Perforce.
    copy_p2g(ctx, start)
