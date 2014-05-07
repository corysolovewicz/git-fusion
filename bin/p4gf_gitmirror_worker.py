#! /usr/bin/env python3.3
"""GitMirror worker process"""

import atexit
import os
import re
import sys

import p4gf_env_config  # pylint: disable=W0611
import p4gf_context
import p4gf_git
import p4gf_gitmirror
from   p4gf_l10n    import _, NTR, log_l10n
import p4gf_log
import p4gf_object_type
import p4gf_proc
from   p4gf_profiler import Timer
import p4gf_util

from P4 import P4Exception

LOG = p4gf_log.for_module()

def main():
    '''
    Parse command line arguments and do the work of mirroring Git objects.
    '''
    LOG.debug("main: {}, pid={}".format(p4gf_log.memory_usage(), os.getpid()))
    log_l10n()
    if 3 != len(sys.argv):
        sys.stderr.write(_('usage: p4gf_gitmirror_worker.py <repo> <work-queue-file>'))
        return 1
    view_name = sys.argv[1]
    path = sys.argv[2]
    LOG.debug("reading commits from {}".format(path))
    p4gf_proc.init()
    __do_trees(view_name, path)
    p4gf_proc.stop()
    LOG.debug("worker done for {}".format(view_name))
    return 0


def __do_trees(view_name, path):
    '''Process any and all files associated with this view'''
    # don't leave a mess: clean up file even if there's a problem processing it
    atexit.register(os.unlink, path)
    with p4gf_context.create_context(view_name, None) as ctx:
        # we don't create any temp clients here, so don't try deleting them either.
        # leave that to processes that actually use them.
        ctx.cleanup_client_pool = False
        os.chdir(ctx.view_dirs.GIT_WORK_TREE)
        LOG.debug("processing trees for view {}".format(view_name))

        with open(path, "r") as f:
            with Timer(p4gf_gitmirror.ADD_SUBMIT):
                trees = set()
                last_tree = None
                while True:
                    line = f.readline().strip()
                    LOG.debug("processing line '{}'".format(line))
                    if line == "end":
                        break
                    elif line == '---':
                        last_tree = None
                    else:
                        if not last_tree:
                            last_tree = __get_snapshot_trees(line, trees)
                        else:
                            last_tree = __get_delta_trees(last_tree, line, trees)
                if trees:
                    LOG.debug("submitting trees for {}".format(view_name))
                    __add_trees_to_p4(ctx, trees)


# line is: mode SP type SP sha TAB path
# we only want the sha from lines with type "tree"
TREE_REGEX = re.compile("^[0-7]{6} tree ([0-9a-fA-F]{40})\t.*")

def __get_snapshot_trees(commit, trees):
    """get all tree objects for a given commit
        commit: SHA1 of commit

    each tree is added to the list to be mirrored
    """
    #ls-tree doesn't return the top level tree, so add it here
    commit_tree = __get_commit_tree(commit, trees)
    po = p4gf_proc.popen_no_throw(['git', 'ls-tree', '-rt', commit_tree])['out']
    for line in po.splitlines():
        m = TREE_REGEX.match(line)
        if m:
            LOG.debug("adding subtree {}".format(m.group(1)))
            trees.add(m.group(1))
    return commit_tree

# line is: :mode1 SP mode2 SP sha1 SP sha2 SP action TAB path
# we want sha2 from lines where mode2 indicates a dir
TREE_ENT_RE = re.compile("^:[0-7]{6} 04[0-7]{4} [0-9a-fA-F]{40} ([0-9a-fA-F]{40}) .*")

def __get_delta_trees(commit_tree1, commit2, trees):
    """get all tree objects new in one commit vs another commit
        commit1: SHA1 of first commit
        commit2: SHA1 of second commit

    each tree is added to the list to be mirrored
    """
    # diff-tree doesn't return the top level tree, so add it here
    commit_tree2 = __get_commit_tree(commit2, trees)
    #po = p4gf_proc.popen_no_throw(['git', 'diff-tree', '-t', commit1, commit2])['out']
    po = p4gf_proc.popen_no_throw(['git', 'diff-tree', '-t', commit_tree1, commit_tree2])['out']
    for line in po.splitlines():
        m = TREE_ENT_RE.match(line)
        if m:
            LOG.debug("adding subtree {}".format(m.group(1)))
            trees.add(m.group(1))
    return commit_tree2

def __get_commit_tree(commit, trees):
    """get the one and only tree at the top of commit

        commit: SHA1 of the commit

    add the tree object to the list of trees to be mirrored
    """

    po = p4gf_git.get_commit(commit)
    for line in iter(po.splitlines()):
        if not line.startswith("tree"):
            continue
        # line is: tree sha
        parts = line.strip().split(' ')
        sha1 = parts[1]
        LOG.debug("adding commit tree {}".format(sha1))
        trees.add(sha1)
        return sha1

def __add_trees_to_p4(ctx, trees):
    """
    Attempt to add a set of trees to the mirror, retrying if there
    is an exception. If it doesn't go on the second attempt,
    raise the exception. Use numbered changelists to aid in recovery.
    """
    for i in range(2):
        try:
            __really_add_trees_to_p4(ctx, trees)
            break
        except P4Exception:
            if i:
                raise

def __really_add_trees_to_p4(ctx, trees):
    """actually run p4 add, submit to create mirror files in .git-fusion"""
    desc = _("Git Fusion '{view}' trees copied to Git.").format(view=ctx.config.view_name)
    with p4gf_util.NumberedChangelist(gfctx=ctx, description=desc) as nc:
        LOG.debug("adding {} trees to .git-fusion...{}".format(len(trees), trees))

        # build list of trees to add, extracting them from git
        add_files = [__add_tree_to_p4(ctx, sha1) for sha1 in list(trees)]
        add_files = p4gf_gitmirror.GitMirror.optimize_objects_to_add_to_p4(ctx, add_files)

        if not len(add_files):
            return

        files_added = p4gf_gitmirror.GitMirror.add_objects_to_p4_2(ctx, add_files)

        if files_added:
            nc.submit()
        else:
            LOG.debug("ignoring empty change list...")

def __add_tree_to_p4(ctx, sha1):
    """add a tree to the git-fusion perforce client workspace

    return the path of the client workspace file suitable for use with
    p4 add
    """
    # get client path for .git-fusion file
    dst = os.path.join( ctx.gitlocalroot, "objects", NTR('trees')
                      , p4gf_object_type.slashify_sha1(sha1))

    # A tree is likely to already exist, in which case we don't need
    # or want to try to recreate it.  We'll just use the existing one.
    if os.path.exists(dst):
        LOG.debug("reusing existing object: " + dst)
        return dst

    # make sure dir exists
    dstdir = os.path.dirname(dst)
    if not os.path.exists(dstdir):
        try:
            os.makedirs(dstdir)
        #pylint:disable=E0602
        # pylint running on python 3.2 does not know about 3.3 features
        except FileExistsError:
        #pylint:enable=E0602
            # For file exists error, just ignore it, probably another
            # process creating the same thing simultaneously.
            pass
        except OSError as e:
            raise e

    # Hardlink the Git object into the Perforce workspace
    LOG.debug2("adding new object: " + dst)
    op = p4gf_git.object_path(sha1)
    os.link(op, dst)

    return dst

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=False)

