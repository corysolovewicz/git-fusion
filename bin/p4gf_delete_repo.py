#! /usr/bin/env python3.3
"""Deletes Git Fusion repositories and Perforce artifacts.

During testing, we often create and destroy Git Fusion repositories.
As such, we need an easy way to clean up and try again, without
destroying the entire Perforce server and starting from scratch. In
particular, this script will:

* delete client git-fusion-<space> workspace files
* delete client git-fusion-<space>

If the --all option is given, all git-fusion-<view> clients are
found and deleted, in addition to the following:

* delete object client workspace files
* obliterate //P4GF_DEPOT/objects/...

Invoke with -h for usage information.

"""

import binascii
import os
import sys

import P4
import p4gf_env_config    # pylint: disable=W0611
import p4gf_config
import p4gf_const
import p4gf_context
import p4gf_create_p4
from   p4gf_l10n import _, NTR, log_l10n
import p4gf_log
import p4gf_lock
from   p4gf_object_type import ObjectType
import p4gf_util
import p4gf_view_dirs
import p4gf_translate

LOG = p4gf_log.for_module()


class DeletionMetrics:
    """
    DeletionMetrics captures the number of Perforce objects removed.
    """
    def __init__(self):
        self.clients = 0
        self.groups = 0
        self.files = 0
        self.counters = 0


def raise_if_homedir(homedir, view_name, rm_list):
    """If any path in rm_list is user's home directory, fail with error
    rather than delete the home directory."""
    for e in rm_list:
        if e == homedir:
            raise P4.P4Exception(_("One of view '{}'s directories is"
                                   " user's home directory!").format(view_name))


def print_verbose(args, msg):
    """If args.verbose, print msg, else NOP."""
    if args.verbose:
        print(msg)

#pylint:disable=E0602
# pylint running on python 3.2 does not know about 3.3 features
def _remove_tree(tree, contents_only=True):
    """Delete a directory tree."""
    if not os.path.exists(tree):
        return
    try:
        p4gf_util.rm_dir_contents(tree)
        if not contents_only:
            if os.path.isdir(tree) and not os.path.islink(tree):
                os.rmdir(tree)
            else:
                os.remove(tree)
    except FileNotFoundError as e:
        sys.stderr.write(_('File not found error while removing tree: {}\n').format(e))
    except PermissionError as e:
        sys.stderr.write(_('Permission error while removing tree: {}\n').format(e))
#pylint:enable=E0602


def _tree_scanner(blob):
    """Generator function that returns a series of SHA1's for each tree found
    in the given tree blob. If no trees found, returns nothing.
    The object header should not be part of the input.
    """
    # Format: [mode string] [name string]\0[20-byte-SHA1-value]... (no line seperator)
    # Mask of mode string for trees is 040000 (that is, second digit is a 4)
    # Unsure if tree entries _always_ have a file mode of 040000 (stored as '40000'),
    # so allow for leading zero when checking mode.
    idx = 0
    end = len(blob)
    while idx < end:
        nindex = blob.index(b'\x00', idx) + 21
        # Check entry mode, first non-zero digit is a 4
        if (blob[idx] == 48 and blob[idx + 1] == 52) or blob[idx] == 52:
            yield binascii.hexlify(blob[nindex - 20:nindex]).decode()
        idx = nindex


def _find_commit_files(path, client_name):
    """Generator function that walks a directory tree, returning each commit
    file found for the given client.

    Arguments:
        path -- root of directory tree to walk.
        client_name -- name of client for which to find commits.
    """
    for root, _dirs, files in os.walk(path):
        for fyle in files:
            fpath = os.path.join(root, fyle)
            # Convert the object file path to an ObjectType, but don't
            # let those silly non-P4GF objects stop us.
            ot = ObjectType.commit_from_filepath(fpath)
            if ot and ot.applies_to_view(client_name):
                yield fpath


def _tree_mirror_path(root, sha1):
    '''Construct a path to the object file.'''
    return os.path.join(root, 'trees', sha1[:2], sha1[2:4], sha1[4:])


def _fetch_tree(root, sha1):
    """
    Fetches the Git tree object as raw text, or returns None if the file is missing.
    """
    path = _tree_mirror_path(root, sha1)
    if os.path.exists(path):
        return p4gf_util.local_path_to_git_object(path)
    LOG.warn('Missing file for tree object {}'.format(path))
    return None



def _find_client_commit_objects(args, p4, view_name):
    """Finds the object cache commit files associated only with the given view.
    These are objects that can be deleted from the cache without affecting
    other Git Fusion views. This does not return the eligible tree objects.

    Arguments:
        args -- parsed command line arguments
        p4 -- P4API object, client for object cache, already connected
        view_name -- name of view for which files are to be pruned from cache

    Returns:
        List of cached commit objects to be deleted.
    """

    # Bring the workspace up to date and traverse that rather than
    # fetching large numbers of small files from Perforce.
    repo_commit_objects_path = "{0}/repos/{1}/...".format(p4gf_const.objects_root()
                ,view_name)
    repos_path = "{0}/repos/...".format(p4gf_const.P4GF_DEPOT)
    with p4.at_exception_level(P4.P4.RAISE_NONE):
        # Raises an exception when there are no files to sync?
        p4.run('sync', '-q', repo_commit_objects_path )
        p4.run('sync', '-q' , repos_path)

# TBD Optimization:
# Rather than delete batches of files based on workspace file discovery
# we could do the following -- ??could overwhelm the server or be slower??
#   r = p4.run('delete', repo_commit_objects_path)
#   count = sum([int('depotFile' in rr and rr['action'] == 'delete') for rr in r])
#   r = p4.run("submit", "-d",
#            "Deleting {0} commit objects for repo '{1}'".format(count, view_name))
#   return count

    root = os.path.join(get_p4gf_localroot(p4), 'objects')
    print_verbose(args,
            _("Selecting cached commit objects for '{}'...").format(view_name))
    paths = [os.path.join(root, 'repos', view_name, '...')]
    return paths




def _delete_files(p4, files, view_name=None):
    """
    Delete a set of files, doing so in chunks.
    """
    if view_name:
        msgstr = "Deleting {0} commit objects for repo '" + view_name + "'."
    else:
        msgstr = "Deleting {0} commit objects for all repos."
    count = 0
    bite_size = 1000
    while len(files):
        to_delete = files[:bite_size]
        files = files[bite_size:]
        r = p4.run("delete", to_delete)
        count += sum([int('depotFile' in rr and rr['action'] == 'delete') for rr in r])
        for d in to_delete:
            if os.path.isfile(d):
                os.remove(d)
        r = p4.run("submit", "-d", msgstr.format(count))
    return count


def delete_client(args, p4, client_name, metrics, prune_objs=True):
    """Delete the named Perforce client and its workspace. Raises
    P4Exception if the client is not present, or the client configuration is
    not set up as expected.

    Keyword arguments:
    args        -- parsed command line arguments
    p4          -- Git user's Perforce client
    client_name -- name of client to be deleted
    metrics     -- DeletionMetrics for collecting resulting metrics
    prune_objs  -- if True, delete associated objects from cache

    """
    # pylint: disable=R0912,R0915
    group_list = [p4gf_const.P4GF_GROUP_VIEW_PULL, p4gf_const.P4GF_GROUP_VIEW_PUSH]
    p4.user = p4gf_const.P4GF_USER

    print_verbose(args, _("Checking for client '{}'...").format(client_name))
    if not p4gf_util.spec_exists(p4, 'client', client_name):
        raise P4.P4Exception(_("No such client '{}' defined")
                             .format(client_name))
    view_name = p4gf_util.client_to_view_name(client_name)
    p4gf_dir = p4gf_util.p4_to_p4gf_dir(p4)
    view_dirs = p4gf_view_dirs.from_p4gf_dir(p4gf_dir, view_name)
    p4gf_util.ensure_spec_values(p4, 'client', client_name, {'Root': view_dirs.p4root})

    view_lock = None  # We're clobbering and deleting. Overrule locks.
    with p4gf_context.create_context(view_name, view_lock) as ctx:
        command_path = ctx.client_view_path()

        homedir = os.path.expanduser('~')
        raise_if_homedir(homedir, view_name, view_dirs.view_container)

        # Scan for objects associated only with this view so we can remove them.
        objects_to_delete = []
        if prune_objs:
            objects_to_delete = _find_client_commit_objects(args, p4, view_name)

        # Do we have a repo config file to delete?
        config_file = p4gf_config.depot_path_repo(view_name) + '*'
        config_file_exists = p4gf_util.depot_file_exists(p4, config_file)

        # What counters shall we delete?
        counter_list = []
        counter_list.append(p4gf_context.calc_last_copied_change_counter_name(
                            view_name, p4gf_util.get_server_id()))
        for spec in p4.run('counters', '-u', '-e', "git-fusion-index-last-{},*"
                                                   .format(view_name)):
            counter_list.append(spec['counter'])
        for spec in p4.run('counters', '-u', '-e', "git-fusion-index-branch-{},*"
                                                   .format(view_name)):
            counter_list.append(spec['counter'])

        if not args.delete:
            print(NTR('p4 sync -f {}#none').format(command_path))
            print(NTR('p4 client -f -d {}').format(client_name))
            print(NTR('rm -rf {}').format(view_dirs.view_container))
            print(NTR('Deleting {} objects from //{}/objects/...').format(
                len(objects_to_delete), p4gf_const.P4GF_DEPOT))
            for group_template in group_list:
                group = group_template.format(view=view_name)
                print(NTR('p4 group -a -d {}').format(group))
            for c in counter_list:
                print(NTR('p4 counter -u -d {}').format(c))

            if config_file_exists:
                print(NTR('p4 sync -f {}').format(config_file))
                print(NTR('p4 delete  {}').format(config_file))
                print(NTR('p4 submit -d "Delete repo config for {view_name}" {config_file}')
                      .format(view_name=view_name, config_file=config_file))
        else:
            print_verbose(args, NTR('Removing client files for {}...').format(client_name))
            ctx.p4.run('sync', '-fq', command_path + '#none')
            print_verbose(args, NTR('Deleting client {}...').format(client_name))
            p4.run('client', '-df', client_name)
            metrics.clients += 1
            print_verbose(args, NTR("Deleting repo {0}'s directory {1}...").format(view_name,
                view_dirs.view_container))
            _remove_tree(view_dirs.view_container, contents_only=False)
            metrics.files += _delete_files(p4, objects_to_delete, view_name)
            for group_template in group_list:
                _delete_group(args, p4, group_template.format(view=view_name), metrics)
            for c in counter_list:
                _delete_counter(p4, c, metrics)

            if config_file_exists:
                p4gf_util.p4run_logged(p4, ['sync', '-fq', config_file])
                with p4gf_util.NumberedChangelist(
                        p4=p4, description=_("Delete repo config for '{}'")
                                           .format(view_name)) as nc:
                    nc.p4run(["delete", config_file])
                    nc.submit()
    # pylint: enable=R0912,R0915


def _delete_counter(p4, name, metrics):
    """Attempt to delete counter. Report and continue on error."""
    try:
        p4.run('counter', '-u', '-d', name)
        metrics.counters += 1
    except P4.P4Exception as e:
                        ### NONONO Look for specific message ID not US English text.
        if str(e).find(NTR("No such counter")) < 0:
            LOG.info('failed to delete counter {ctr}: {e}'.
                     format(ctr=name, e=str(e)))


def get_p4gf_localroot(p4):
    """Calculate the local root for the object client."""
    if p4.client != p4gf_util.get_object_client_name():
        raise RuntimeError(_('incorrect p4 client'))
    client = p4.fetch_client()
    rootdir = client["Root"]
    if rootdir.endswith(os.sep):
        rootdir = rootdir[:len(rootdir) - 1]
    client_map = P4.Map(client["View"])
    lhs = client_map.lhs()
    if len(lhs) > 1:
        # not a conforming Git Fusion client, ignore it
        return None
    rpath = client_map.translate(lhs[0])
    localpath = p4gf_context.client_path_to_local(rpath, p4.client, rootdir)
    localroot = p4gf_context.strip_wild(localpath)
    localroot = localroot.rstrip('/')
    return localroot


def _delete_group(args, p4, group_name, metrics):
    """Delete one group, if it exists and it's ours."""
    LOG.debug("_delete_group() {}".format(group_name))
    r = p4.fetch_group(group_name)
    if r and r.get('Owners') and p4gf_const.P4GF_USER in r.get('Owners'):
        print_verbose(args, _("Deleting group '{}'...").format(group_name))
        p4.run('group', '-a', '-d', group_name)
        metrics.groups += 1
    else:
        print_verbose(args, _("Not deleting group '{group}':"
                              " Does not exist or '{user}' is not an owner.")
                            .format(group=group_name, user=p4gf_const.P4GF_USER))


def _delete_cache(args, p4, metrics):
    """Delete all of the Git Fusion cached objects."""
    if not args.no_obliterate:
        print_verbose(args, _('Obliterating object cache...'))
        r = p4.run('obliterate', '-y', '//{}/objects/...'.format(p4gf_const.P4GF_DEPOT))
        results = p4gf_util.first_dict_with_key(r, 'revisionRecDeleted')
        if results:
            metrics.files += int(results['revisionRecDeleted'])


def delete_clients(args, p4, metrics):
    """Delete all of the Git Fusion clients, except the object cache clients.
    """
    views = p4gf_util.view_list(p4)
    if not views:
        print(_('No Git Fusion clients found.'))
        return
    for view in views:
        client = p4gf_util.view_to_client_name(view)
        try:
            delete_client(args, p4, client, metrics, False)
        except P4.P4Exception as e:
            sys.stderr.write(str(e) + '\n')
            sys.exit(1)


def _remove_local_root(localroot):
    """
    Remove the contents of the P4GF local workspace, disregarding whether
    the root is a symbolic link.
    Save and re-write the server-id file after removing contents.
    """
    LOG.debug2("_remove_local_root(): {}".format(localroot))
    # get the server_id
    server_id = p4gf_util.get_server_id()
    _remove_tree(localroot)
    # re-write server_id
    p4gf_util.write_server_id_to_file(server_id)


def _lock_all_repos(p4):
    """
    Quickly acquire locks on all Git Fusion repositories, failing immediately
    (raises RuntimeError) if any repos are currently locked. Waiting would
    only increase the chance of getting blocked on another repo, so scan and
    fail fast instead.

    Returns a list of the CounterLock instances acquired.
    """
    locks = []
    views = p4gf_util.view_list(p4)
    if not views:
        print(_('No Git Fusion clients found.'))
    else:
        for view in views:
            lock = p4gf_lock.view_lock(p4, view)
            lock.acquire(-1)
            # If that didn't raise an error, then add to the list of locks acquired.
            locks.append(lock)
    return locks


def _release_locks(locks):
    """
    Release all of the given locks, reporting any errors to the log.
    """
    for lock in locks:
        # pylint:disable=W0703
        try:
            lock.release()
        except Exception as e:
            LOG.error("Error releasing lock {}: {}".format(lock.counter_name(), e))
        # pylint:enable=W0703


def _prevent_access(p4):
    """
    Prevent further access to Git Fusion while deleting everything.
    Return the previous value of the counter so it can be restored later.
    """
    result = p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_PREVENT_NEW_SESSIONS)
    old_value = None
    if result:
        old_value = p4gf_util.first_value_for_key(result, 'value')
    p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_PREVENT_NEW_SESSIONS, 'true')
    return old_value


#pylint:disable=R0912
# Pylint, would you rather have several functions taking 6+ args, or one complex func?
def delete_all(args, p4, metrics):
    """Find all git-fusion-* clients and remove them, as well as
    the entire object cache (//P4GF_DEPOT/objects/...).

    Keyword arguments:
        args -- parsed command line arguments
        p4   -- Git user's Perforce client
    """
    p4.user = p4gf_const.P4GF_USER
    group_list = [p4gf_const.P4GF_GROUP_PULL, p4gf_const.P4GF_GROUP_PUSH]
    print(_('Connected to {P4PORT}').format(P4PORT=p4.port))
    print_verbose(args, _('Scanning for Git Fusion clients...'))
    client_name = p4gf_util.get_object_client_name()
    locks = _lock_all_repos(p4)
    was_prevented = _prevent_access(p4)
    delete_clients(args, p4, metrics)
    # Retrieve the names of the initialization/upgrade "lock" counters.
    counters = []
    counter_names = ['git-fusion-init-started*',
                     'git-fusion-init-complete*',
                     'git-fusion-upgrade-started*',
                     'git-fusion-upgrade-complete*',
                     'git-fusion-index-*']
    for counter_name in counter_names:
        r = p4.run('counters', '-u', '-e', counter_name)
        for spec in r:
            counters.append(spec['counter'])
    localroot = get_p4gf_localroot(p4)
    if not args.delete:
        if localroot:
            print(NTR('p4 sync -f {}...#none').format(localroot))
            if not args.no_obliterate:
                print(NTR('p4 client -f -d {}').format(client_name))
                print(NTR('rm -rf {}').format(localroot))
        if not args.no_obliterate:
            print(NTR('p4 obliterate -y //{}/objects/...').format(p4gf_const.P4GF_DEPOT))
        for counter in counters:
            print(NTR('p4 counter -u -d {}').format(counter))
        for group in group_list:
            print(NTR('p4 group -a -d {}').format(group))
    else:
        if localroot and not args.no_obliterate:
            print_verbose(args, _("Removing client files for '{}'...").format(client_name))
            p4.run('sync', '-fq', localroot + '/...#none')
            # Need this in order to use --gc later on
            print_verbose(args, _("Deleting client '{}'...").format(client_name))
            p4.run('client', '-df', client_name)
            metrics.clients += 1
            print_verbose(args, _("Deleting client '{}'s workspace...").format(client_name))
            _remove_local_root(localroot)
        _delete_cache(args, p4, metrics)
        print_verbose(args, _('Removing initialization counters...'))
        for counter in counters:
            _delete_counter(p4, counter, metrics)
        for group in group_list:
            _delete_group(args, p4, group, metrics)
    _release_locks(locks)
    if was_prevented and was_prevented != '0':
        p4.run('counter', '-u', p4gf_const.P4GF_COUNTER_PREVENT_NEW_SESSIONS, was_prevented)
    else:
        p4.run('counter', '-u', '-d', p4gf_const.P4GF_COUNTER_PREVENT_NEW_SESSIONS)


def main():
    """
    Process command line arguments and call functions to do the real
    work of cleaning up the Git mirror and Perforce workspaces.
    """
    log_l10n()
    p4gf_util.has_server_id_or_exit()

                        # pylint:disable=C0301
                        # Line too long? Too bad. Keep tabular code tabular.
    # Set up argument parsing.
    parser = p4gf_util.create_arg_parser(
        _('Deletes Git Fusion repositories and workspaces.'))
    parser.add_argument('-a',   '--all',            action='store_true',    help=_('remove all known Git mirrors'))
    parser.add_argument('-y',   '--delete',         action='store_true',    help=_('perform the deletion'))
    parser.add_argument('-v',   '--verbose',        action='store_true',    help=_('print details of deletion process'))
    parser.add_argument('-N',   '--no-obliterate',  action='store_true',    help=_('with the --all option, do not obliterate object cache'))
    parser.add_argument(NTR('views'), metavar=NTR('view'), nargs='*',       help=_('name of view to be deleted'))
    args = parser.parse_args()
                        # pylint:enable=C0301

    # Check that either --all, or 'views' was specified.
    if not args.all and len(args.views) == 0:
        sys.stderr.write(_('Missing view names; try adding --all option.\n'))
        sys.exit(2)

    # Check that --no-obliterate occurs only with --all
    if not args.all and args.no_obliterate:
        sys.stderr.write(_('--no-obliterate permitted only with the --all option.\n'))
        sys.exit(2)

    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4(client=p4gf_util.get_object_client_name())
        if not p4:
            return 2
        # Sanity check the connection (e.g. user logged in?) before proceeding.
        try:
            p4.fetch_client()
        except P4.P4Exception as e:
            sys.stderr.write(_('P4 exception occurred: {}').format(e))
            sys.exit(1)

        metrics = DeletionMetrics()
        if args.all:
            try:
                delete_all(args, p4, metrics)
            except P4.P4Exception as e:
                sys.stderr.write("{}\n".format(e))
                sys.exit(1)
        else:
            # Delete the client(s) for the named view(s).
            for git_view in args.views:
                view_name = p4gf_translate.TranslateReponame.git_to_repo(git_view)
                client_name = p4gf_util.view_to_client_name(view_name)
                try:
                    with p4gf_lock.view_lock(p4, view_name, -1):
                        delete_client(args, p4, client_name, metrics)
                except P4.P4Exception as e:
                    sys.stderr.write("{}\n".format(e))
        if  not args.delete:
            print(_('This was report mode. Use -y to make changes.'))
        else:
            print(_('Deleted {:d} files, {:d} groups, {:d} clients, and {:d} counters.').format(
                metrics.files, metrics.groups, metrics.clients, metrics.counters))
            if args.all:
                print(_('Successfully deleted all repos\n'))
            else:
                print(_('Successfully deleted repos:\n{}').format("\n".join(args.views)))
#pylint:enable=R0912

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)
