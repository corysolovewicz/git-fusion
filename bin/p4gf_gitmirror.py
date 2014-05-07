#! /usr/bin/env python3.3
"""GitMirror class"""

import atexit
import configparser
import copy
import functools
import os
import shutil
import subprocess
import sys
import tempfile

import p4gf_config
import p4gf_const
from   p4gf_fastimport_mark import Mark
import p4gf_git
from   p4gf_l10n            import _, NTR
import p4gf_log
from p4gf_object_type import CommitDetails, ObjectType, OBJPATH_TREE_REGEX
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_proc
from   p4gf_profiler import Timer
import p4gf_progress_reporter as ProgressReporter
import p4gf_util

from P4 import OutputHandler, P4Exception

LOG = p4gf_log.for_module()

_BITE_SIZE = 1000       # How many files to pass in a single 'p4 xxx' operation.

# pylint:disable=C0103
# yes, many "invalid" names in our code, from classes to supposed constants


class FilterAddFstatHandler(OutputHandler):
    """OutputHandler for p4 fstat, builds list of files that don't already exist.
    """
    def __init__(self):
        OutputHandler.__init__(self)
        self.files = []

    def outputMessage(self, m):
        """outputMessage call expected for any files not already added;
        otherwise indicates an error
        """
        if m.msgid == p4gf_p4msgid.MsgDm_ExFILE:
            self.files.append(m.dict['argc'])
        return OutputHandler.REPORT


class CommitList:
    """A list of commit ObjectType instances."""

    def __init__(self):
        self.commits = {}

    def __len__(self):
        return len(self.commits)

    def __delitem__(self, key):
        raise KeyError

    def __getitem__(self, key):
        raise KeyError

    def __setitem__(self, key, value):
        raise KeyError

    def add_commit(self, sha1, details):
        """skip over duplicate objects (e.g. tree shared by commits)"""
        key = sha1
        if details.branch_id:
            key += ',' + details.branch_id

        if key not in self.commits:
            commit = ObjectType(sha1, "commit", details)
            self.commits[key] = commit

    def clear(self):
        """Clear the list of all commits"""
        self.commits.clear()

    def __str__(self):
        return "{} commits".format(len(self.commits))

    def __repr__(self):
        items = [repr(commit) for commit in self.commits]
        items.append(str(self))
        return "\n".join(items)


# spawn function derived from code posted on stack overflow:
# http://stackoverflow.com/questions/8425116/indefinite-daemonized-process-spawning-in-python
# Made various updates for Python3.
# In order to avoid subprocess inheriting the memory footprint of the parent
# process at the time we actually want to do the spawn, it must be initiated
# at the start of the parent process, when it has a smaller memory footprint.
# In order to accomplish this, use a pipe to signal when it's time to actually
# start the process.  Maybe there's a better way but this seems to work.
#
# pylint:disable=W0212
# access to protected memory _exit
def _double_fork(func):
    """
    do the UNIX double-fork magic, see Stevens' "Advanced
    Programming in the UNIX Environment" for details (ISBN 0201563177)
    http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
    """
    # flush before fork rather than later so that buffer contents doesn't get
    # written twice
    sys.stderr.flush()

    try:
        pid = os.fork()
        if pid > 0:
            # main/parent process
            return
    except OSError as e:
        sys.stderr.write(_('fork #1 failed: %d (%s)\n') % (e.errno, e.strerror))
        sys.exit(1)

    # decouple from parent environment
    os.setsid()
    os.umask(0)

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent
            os._exit(0)
    except OSError as e:
        sys.stderr.write(_('fork #2 failed: %d (%s)\n') % (e.errno, e.strerror))
        os._exit(1)

    # redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    si = open('/dev/null', 'r')
    so = open('/dev/null', 'a+')
    se = open('/dev/null', 'a+')
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    # call the given function
    p4gf_proc.install_stack_dumper()
    func()
    os._exit(0)
# pylint:enable=W0212


def _spawn(script, view_name, ppid, pipe):
    """
    Read commit checksums from the pipe and write to a queue file,
    then kick off a subprocess to process that queue file.

    Arguments:
        script -- command to be executed in subprocess
        view_name -- repo for which processing is taking place
        ppid -- parent process ID, for logging
        pipe -- tuple containing read/write file descriptors
    """
    LOG.debug("spawn for {} processing {}, pid={}".format(ppid, view_name, os.getpid()))
    os.close(pipe[1])
    pipe_r = os.fdopen(pipe[0], 'r')
    LOG.debug2("spawn for {} creating queue file".format(ppid))
    # Copy the commits from the pipe to the queue file.
    lines_read = 0
    tmp_file = tempfile.mkstemp(prefix='mirror-queue-')[1]
    with open(tmp_file, 'w') as tmp_fobj:
        for line in pipe_r:
            line = line.strip()
            LOG.debug2("_spawn() writing '{}' to queue".format(line))
            tmp_fobj.write("{}\n".format(line))
            lines_read += 1
        LOG.debug2("_spawn() writing 'end' to queue")
        tmp_fobj.write(NTR('end\n'))
    if lines_read:
        # Move the temporary file into the repo directory now that it is
        # ready (and not a moment earlier, to avoid creating the repo
        # directory when permissions checks ultimately fail).
        q_dir = _queue_dir(view_name)
        if not os.path.exists(q_dir):
            os.makedirs(q_dir)
        q_file = os.path.join(q_dir, "{}.worker".format(ppid))
        shutil.move(tmp_file, q_file)
        # Now that the queue file is ready, launch the worker process.
        LOG.debug("spawn for {} launching subprocess".format(ppid))
        subprocess.Popen([script, view_name, q_file])
    else:
        # Apparently nothing was to be processed, clean up.
        LOG.debug("spawn for {} not needed".format(ppid))
        os.unlink(tmp_file)
    pipe_r.close()
    LOG.debug("spawn for {} exited, pid={}".format(ppid, os.getpid()))


pipe_for_view = dict()
def setup_spawn(view_name):
    '''do double fork magic to prepare for later spawn of worker process
    Do this in advance to reduce memory size of worker process.'''
    if view_name in pipe_for_view:
        LOG.debug("spawn already setup for {}".format(view_name))
        return
    bindir = os.path.dirname(os.path.abspath(__file__))
    script = os.path.join(bindir, "p4gf_gitmirror_worker.py")
    pid = os.getpid()
    LOG.debug("setup_spawn for {}, pid={}".format(view_name, pid))
    pipe = os.pipe()
    pipe_for_view[view_name] = pipe
    func = functools.partial(_spawn, script, view_name, pid, pipe)
    _double_fork(func)


def close(view_name):
    '''
    Processing of the current Git request is coming to a close, signal
    the mirror worker for this view and remove the bookkeeping data.
    In general, only the long-running form of Git Fusion needs to call
    this function, as in the case of HTTP (versus SSH in which the
    process terminates at the end of the request).
    '''
    pipe = pipe_for_view.pop(view_name, None)
    if pipe:
        if isinstance(pipe, tuple):
            # Pipe was never opened, close the file descriptors.
            os.close(pipe[0])
            os.close(pipe[1])
        else:
            # Already opened the pipe, just need to close our end.
            pipe.close()
    LOG.debug("close() complete for view {}".format(view_name))


def _copy_commit_trees(view_name, commits):
    '''add a commit to the queue so that its trees will be mirrored.

    If not already done, create the file used to pass a list of commits to the
    worker process and write the path to this file to the pipe, causing the
    double-fork magic process to fire up the worker process.

    Then, write the commit sha1 to the file.  The worker process will read
    commits from this file and mirror the trees referenced by each one.

    A sequence of commits is mirrored for each branch; these sequences
    are separated by "---" in the file.

    The worker will quit when it reads "end" from the file, so an atexit
    function is used to write that.
    '''
    if not commits:
        return
    pipe = pipe_for_view.get(view_name)
    if pipe is None:
        LOG.warning('no active pipe for {}'.format(view_name))
        return
    if not hasattr(pipe, 'fileno'):
        # If not a file object, then 'pipe' is a tuple of the pipe file
        # descriptors which we will now open/close as appropriate.
        os.close(pipe[0])
        pipe = os.fdopen(pipe[1], 'w')
        pipe_for_view[view_name] = pipe
        # make sure the pipe is closed eventually
        deferred = functools.partial(close, view_name)
        atexit.register(deferred)
    for commit in commits:
        if commit:
            pipe.write("{}\n".format(commit))
        else:
            pipe.write("---\n")


def _queue_dir(view_name):
    '''return dir containing queue files'''
    return os.path.join(p4gf_const.P4GF_HOME, "views", view_name, "tree-queue")


def copying_trees():
    '''is worker process still processing trees?'''
    queues_path = os.path.join(_queue_dir('*'), "*.worker")
    LOG.debug("checking for tree workers in {}".format(queues_path))
    try:
        count = int(subprocess.check_output(NTR('find {} -type f | wc -l').format(queues_path),
                                            shell=True, stderr=subprocess.STDOUT))
    except subprocess.CalledProcessError:
        return False
    except ValueError:
        return False
    return count > 0

# timer/counter names
OVERALL         = NTR('GitMirror Overall')

BUILD           = NTR('Build')

ADD_SUBMIT      = NTR('Add/Submit')
EXTRACT_OBJECTS = NTR('extract objects')
P4_FSTAT        = NTR('p4 fstat')
P4_ADD          = NTR('p4 add')
P4_SUBMIT       = NTR('p4 submit')


class GitMirror:
    """handle git things that get mirrored in perforce"""

    def __init__(self, view_name):
        self.commits = CommitList()
        self.view_name = view_name

        self.branch_list            = None
        self.depot_branch_info_list = None

                        # List of depot files already written to local disk by
                        # ChangelistDataFile.write().
                        # Most are new (require 'p4 add'), some will be updates
                        # to existing (require 'p4 sync -k' + 'p4 edit')
        self.changelist_data_file_list = None

    @staticmethod
    def get_last_change_for_commit(commit, ctx, branch_id=None):
        """Given a commit SHA1, find the latest corresponding Perforce change.
        Note that a Git commit may correspond to several Perforce changes.
        """
        return ObjectType.change_for_sha1(ctx,
                                          commit,
                                          branch_id)

    def add_objects_to_p4(self, marks, mark_list, mark_to_branch_id, ctx):
        """Submit Git commit and tree objects associated with the given marks.

        marks:      list of commit marks output by git-fast-import
                    formatted as: :marknumber sha1 branch-id
        mark_list:  MarkList instance that maps mark number to changelist number.
                    Can be None if mark number == changelist number.
        mark_to_branch_id:
                    dict to find branch_id active when mark's commit
                    was added.
                    Can be None if branch_id encoded in mark lines.
        ctx:        P4GF context
        """
        try:
            with Timer(OVERALL):
                # Unpack the received packs so we can work with loose objects.
                p4gf_git.unpack_objects()
                with ProgressReporter.Indeterminate():
                    with Timer(BUILD):
                        commit_shas = []
                        for mark_line in marks:
                            mark = Mark.from_line(mark_line)
                            mark_num = mark.mark
                            if mark_list:
                                change_num = mark_list.mark_to_cl(mark_num)
                            else:
                                change_num = mark_num
                            sha1 = mark.sha1
                            branch_id = mark.branch
                            if (not branch_id) and mark_to_branch_id:
                                branch_id = mark_to_branch_id.get(mark_num)
                            # add commit object
                            details = CommitDetails(
                                change_num, self.view_name, branch_id)
                            self.commits.add_commit(sha1, details)

                            commit_shas.append(sha1)
                            if len(self.commits) >= _BITE_SIZE:
                                # now that we have a few commits, submit them to P4
                                self._add_commits_to_p4(ctx)
                                _copy_commit_trees(self.view_name, commit_shas)
                                commit_shas.clear()
                                self.commits.clear()

                    # submit the remaining objects to P4
                    self._add_commits_to_p4(ctx)
                    with Timer(BUILD):
                        _copy_commit_trees(self.view_name, commit_shas)
        finally:
            # Let my references go!
            self.commits.clear()

    def _add_commits_to_p4(self, ctx):
        """
        Attempt to add a set of commits to the cache, retrying if there
        is an exception. If it doesn't go on the second attempt,
        raise the exception. Use numbered changelists to aid in recovery.
        """
        for i in range(2):
            try:
                self._really_add_commits_to_p4(ctx)
                ObjectType.reset_cache()
                break
            except P4Exception:
                if i:
                    raise

    def _really_add_commits_to_p4(self, ctx):
        """actually run p4 add, submit to create mirror files in .git-fusion"""
        desc = _("Git Fusion '{view}' copied to Git.").format(view=ctx.config.view_name)
        with p4gf_util.NumberedChangelist(gfctx=ctx, description=desc) as nc:
            with Timer(ADD_SUBMIT):
                LOG.debug("adding {0} commits to .git-fusion...".
                          format(len(self.commits.commits)))

                # build list of objects to add, extracting them from git
                add_files = [self.__add_object_to_p4(ctx, go)
                             for go in self.commits.commits.values()]
                add_files = GitMirror.optimize_objects_to_add_to_p4(ctx, add_files)

                if not (   len(add_files)
                        or self.depot_branch_info_list
                        or self.branch_list ):
                    # Avoid a blank line in output by printing something
                    ProgressReporter.write(_('No Git objects to submit to Perforce'))
                    LOG.debug("_really_add_objects_to_p4() nothing to add...")
                    return

                with Timer(P4_ADD):
                    files_added = self.add_objects_to_p4_2(ctx, add_files)

                    depot_branch_infos_added = \
                                    self._add_depot_branch_infos_to_p4(ctx)

                    config2_added = self._add_branch_defs_to_p4(ctx)

                    cldfs_added = self._add_cldfs_to_p4(ctx)

                with Timer(P4_SUBMIT):
                    if (   files_added
                        or depot_branch_infos_added
                        or config2_added
                        or cldfs_added ):
                        ProgressReporter.increment(
                               _('Submitting new Git commit objects to Perforce'))
                        r = nc.submit()
                        ObjectType.update_indexes(ctx, r)
                    else:
                        ProgressReporter.write(
                               _('No new Git objects to submit to Perforce'))
                        LOG.debug("ignoring empty change list...")

    @staticmethod
    def optimize_objects_to_add_to_p4(ctx, add_files):
        """if many files to add, filter out those which are already added
        Only do this if the number of files is large enough to justify
        the cost of the fstat"""
        enough_files_to_use_fstat = 100
        if len(add_files) < enough_files_to_use_fstat:
            return add_files
        with Timer(P4_FSTAT):
            LOG.debug("using fstat to optimize add")
            original_count = len(add_files)
            ctx.p4gf.handler = FilterAddFstatHandler()
            # spoon-feed p4 to avoid blowing out memory
            while len(add_files):
                bite = add_files[:_BITE_SIZE]
                add_files = add_files[_BITE_SIZE:]
                with ctx.p4gf.at_exception_level(ctx.p4gf.RAISE_NONE):
                    ctx.p4gf.run("fstat", bite)
            add_files = ctx.p4gf.handler.files
            ctx.p4gf.handler = None
            LOG.debug("{} files removed from add list"
                      .format(original_count - len(add_files)))
            return add_files

    @staticmethod
    def add_objects_to_p4_2(ctx, add_files):
        '''
        'p4 add' Git tree and commit objects to Perforce. Does not submit.

        Returns number of files successfully 'p4 add'ed and waiting for submit.

        Returned count does not include files that could not be 'p4 add'ed
        probably because they already exist in the depot.
        '''
        files_to_add = len(add_files)
        files_not_added = 0
        treecount = 0
        commitcount = 0
        # spoon-feed p4 to avoid blowing out memory
        while len(add_files):
            bite = add_files[:_BITE_SIZE]
            add_files = add_files[_BITE_SIZE:]
            result = ctx.p4gfrun(["add", "-t", "binary+F", bite])
            for r in result:
                if isinstance(r, dict) and r["action"] != 'add' or\
                    isinstance(r, str) and r.find("currently opened for add") < 0:
                    # file already exists in depot, perhaps?
                    files_not_added += 1
                    LOG.debug(r)
                elif isinstance(r, dict):
                    if OBJPATH_TREE_REGEX.search(r["depotFile"]):
                        treecount += 1
                    else:
                        commitcount += 1
        LOG.debug("Added {} commits and {} trees"
                  .format(commitcount, treecount))
        return files_to_add - files_not_added

    @staticmethod
    def _p4_sync_k_edit(ctx, path_list):
        '''
        'p4 sync -k' then 'p4 edit' the paths.

        Return list of depotFile successfully opened.
        '''
        if not path_list:
            return []

        cmd = ['sync', '-k', path_list]
        ctx.p4gfrun(cmd)
        cmd = ['edit', '-k', path_list]
        ctx.p4gfrun(cmd)

        l = p4gf_p4msg.find_msgid(ctx.p4gf, p4gf_p4msgid.MsgDm_OpenSuccess)
        success_list = [x['depotFile'] for x in l]
        return success_list

    @staticmethod
    def _p4_add_in_bites(ctx, path_list):
        '''
        'p4 add' all the files in path_list, running multiple 'p4 add'
        commands if necessary to avoid adding more than _BITE_SIZE
        files in a single command.

        Return list of depotFile successfully opened.
        '''
        # Why copy.copy()? Because even though our caller doesn't really care
        # that we drain our list *today*, it might *tomorrow*. The C++
        # programmer in me really misses const and copy semantics. So I'll
        # write C++ in Python here to avoid that nasty surprise in the future.
        remainder_list = copy.copy(path_list)
        success_list = []
        while remainder_list:
            bite_list      = remainder_list[:_BITE_SIZE]
            remainder_list = remainder_list[_BITE_SIZE:]
            cmd = ['add', bite_list]
            ctx.p4gfrun(cmd)

            l = p4gf_p4msg.find_msgid(ctx.p4gf, p4gf_p4msgid.MsgDm_OpenSuccess)
            success_list.extend([x['depotFile'] for x in l])

        LOG.debug('_p4_add_in_bites() want={} success={}'
                  .format(len(path_list), len(success_list)))
        return success_list

    def _add_depot_branch_infos_to_p4(self, ctx):
        '''
        If we created any new depot branches, 'p4 add' their branch-info
        file to Perforce. Does not submit.

        If we edited any existing depot branches, 'p4 edit' them.

        Return number of branch-info files added or edited.
        '''
        if not self.depot_branch_info_list:
            return

        add_path_list = []
        edit_path_list = []
        for dbi in self.depot_branch_info_list:
            config      = dbi.to_config()
            depot_path  = dbi.to_config_depot_path()
            local_path  = p4gf_util.depot_to_local_path( depot_path
                                                       , ctx.p4gf
                                                       , ctx.client_spec_gf )
            p4gf_util.ensure_dir(p4gf_util.parent_dir(local_path))
            p4gf_util.make_writable(local_path)
            with open(local_path, 'w') as f:
                config.write(f)
            if dbi.needs_p4add:
                add_path_list.append(local_path)
            else:
                edit_path_list.append(local_path)
            p4gf_config.clean_up_parser(config)
            del config

        success_list = self._p4_add_in_bites(ctx, add_path_list)
        success_list.extend(self._p4_sync_k_edit(ctx, edit_path_list))
        return len(success_list)

    #pylint:disable=R0912, R0915
    # Too many branches AND Too many statements
    def _add_branch_defs_to_p4(self, ctx):
        '''
        If we defined any new named+lightweight branches, update (or write the
        first revision of) this repo's p4gf_config2 file with all the
        currently defined named+lightweight branches.
        '''
        # Nothing to write? well maybe we have just deleted the remaining refs
        have_branches = bool(self.branch_list)

        # What does the file look like now?
        p4           = ctx.p4gf         # For less typing later.
        old_content  = None
        new_content  = None
        depot_path   = p4gf_config.depot_path_repo2(ctx.config.view_name)
        local_path   = p4gf_util.depot_to_local_path(depot_path, p4)
        depot_exists = False

        # 'p4 print' will fail if file doesn't exist yet. Okay.
        with ctx.p4gf.at_exception_level(p4.RAISE_NONE):
            b = p4gf_util.print_depot_path_raw(p4, depot_path)
            if b:
                old_content = b.decode()    # as UTF-8
                depot_exists = True

        # What do we want the file to look like? ConfigParser writes only to
        # file, not to string, so we have to give it a file path. Ooh! I know!
        # How about writing to the very file that we have to 'p4 add' or 'p4
        # edit' if its content differs?
        if have_branches:
            config = configparser.ConfigParser(interpolation=None)
            for b in self.branch_list:
                LOG.debug("add branch {0}".format(b))
                b.add_to_config(config)

            p4gf_util.ensure_dir(p4gf_util.parent_dir(local_path))
            p4gf_util.make_writable(local_path)
            with open(local_path, 'w') as f:
                config.write(f)
            with open(local_path, 'r') as f:
                new_content = f.read()
            p4gf_config.clean_up_parser(config)
            del config

        # Did nothing change? Then nothing to write.
        if p4gf_config.compare_configs_string(old_content, new_content):
            LOG.debug("No change to p4gf_config2 file")
            return False

        # Have to add or edit or delete the file.
        if not have_branches:
            ctx.p4gfrun(['sync', '-fkq', depot_path])
            ctx.p4gfrun(['delete', depot_path])
            LOG.debug("Deleted p4gf_config2 file")
        else:
            if depot_exists:
                ctx.p4gfrun(['sync', '-fkq', depot_path])
                ctx.p4gfrun(['edit', depot_path])
                LOG.debug("Edited p4gf_config2 file")
            else:
                ctx.p4gfrun(['add', '-t', 'text',  local_path])
                LOG.debug("Added  p4gf_config2 file")

        return True
    #pylint:enable=R0912

    def _add_cldfs_to_p4(self, ctx):
        '''
        If we have any ChangelistDataFile local paths in
        changelist_data_file_list, 'p4 add' or 'p4 sync -k' + 'p4 edit' them
        now.

        Return True if we added/edited at least one file, False if not.
        '''
        if not self.changelist_data_file_list:
            return False

                        # Rather than run 'p4 opened' and then 'p4 sync' + 'p4
                        # edit -k' on any files we failed to 'p4 add', we can
                        # save a lot of thinking by just blindly 'p4 add'ing and
                        # 'p4 edit'ing all files. Yeah it's stupid, yeah it will
                        # pollute logs with a lot of warnings.
        with ctx.p4gf.at_exception_level(ctx.p4gf.RAISE_NONE):
            ctx.p4gfrun(['sync', '-k', self.changelist_data_file_list])
            ctx.p4gfrun(['edit', '-k', self.changelist_data_file_list])
            ctx.p4gfrun(['add',        self.changelist_data_file_list])

        return True

    # pylint: disable=W1401
    # W1401 Unescaped backslash
    # We want that null for the header, so we're keeping the backslash.
    @staticmethod
    def __add_object_to_p4(ctx, go):
        """add a commit to the git-fusion perforce client workspace

        return the path of the client workspace file suitable for use with
        p4 add
        """
        ProgressReporter.increment(_('Adding new Git commit objects to Perforce...'))
        ctx.heartbeat()

        # get client path for .git-fusion file
        dst = os.path.join(ctx.gitlocalroot, go.to_p4_client_path())

        # A tree is likely to already exist, in which case we don't need
        # or want to try to recreate it.  We'll just use the existing one.
        if os.path.exists(dst):
            LOG.debug("reusing existing object: " + dst)
            return dst

        with Timer(EXTRACT_OBJECTS):

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
            op = p4gf_git.object_path(go.sha1)
            os.link(op, dst)
            LOG.debug2("adding new object: " + dst)

            return dst

    def add_depot_branch_infos(self, ctx):
        '''
        If we created any new depot branches to house lightweight branches,
        record a branch_info file for each new depot branch.
        '''
        self.depot_branch_info_list = [
            dbi for dbi in ctx.depot_branch_info_index().by_id.values()
            if dbi.needs_p4add or dbi.needs_p4edit]

    def add_changelist_data_file_list(self, cldf_list):
        '''
        Remember a list of local file paths that we'll eventually add
        at the same time we add all our other files.
        '''
        self.changelist_data_file_list = cldf_list

    def add_branch_config2(self, ctx):
        '''
        If we defined any new lightweight branches, record those mappings
        in p4gf_config2.
        If we have any stream-based branches, record their initial views
        in p4gf_config2.
        '''
        self.branch_list = [b for b in ctx.branch_dict().values()
                            if b.is_lightweight or b.stream_name]

    def delete_branch_config(self, ctx, branch):
        '''
        Git user has deleted task branch - record the branch as deleted
        in p4gf_config2.
        '''
        if branch.is_lightweight:
            self.add_branch_config2(ctx)
            # Mark the branch as deleted and add it to the list of deleted branches
            branch.deleted = True
            with p4gf_util.NumberedChangelist(gfctx=ctx, description=
                    _("Deleting git branch '{0}'").format(branch.git_branch_name)) as nc:
                if self._add_branch_defs_to_p4(ctx):
                    nc.submit()
