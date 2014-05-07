#! /usr/bin/env python3.3
"""Functions to support storing and reconstituting Git tags."""

import os
import re
import sys
import zlib

import P4
import pygit2

import p4gf_const
import p4gf_git
from p4gf_l10n import _, NTR
import p4gf_log
from p4gf_object_type import ObjectType
import p4gf_util

LOG = p4gf_log.for_module()
_BITE_SIZE = 1000  # How many files to pass in a single 'p4 xxx' operation.


def _client_path(ctx, sha1):
    """
    Construct the client path for the given tag object.
    For example, 5716ca5987cbf97d6bb54920bea6adde242d87e6 might return as
    objects/repos/foobar/tags/57/16/ca5987cbf97d6bb54920bea6adde242d87e6
    """
    return os.path.join("objects", "repos", ctx.config.view_name, "tags",
                        sha1[:2], sha1[2:4], sha1[4:])


def _add_tag(ctx, name, sha1, edit_list, add_list):
    """
    Add a tag to the object cache. If adding another lightweight tag
    that refers to the same object, edit the file rather than add.
    """
    LOG.debug("_add_tag() adding tag {}".format(name))
    fpath = os.path.join(ctx.gitlocalroot, _client_path(ctx, sha1))
    if os.path.exists(fpath):
        # Overwriting an existing tag? Git prohibits that.
        # But, another lightweight tag of the same object is okay.
        # Sanity check if this is a lightweight tag of an annotated
        # tag and reject with a warning.
        with open(fpath, 'rb') as f:
            contents = f.read()
        try:
            zlib.decompress(contents)
            form = _("Tag '{}' of annotated tag will not be stored in Perforce\n")
            sys.stderr.write(form.format(name))
            return
        except zlib.error:
            pass
        # it's a lightweight tag, just append the name
        with open(fpath, 'ab') as f:
            f.write(b'\n')
            f.write(name.encode('UTF-8'))
        edit_list.append(fpath)
    else:
        fdir = os.path.dirname(fpath)
        if not os.path.exists(fdir):
            os.makedirs(fdir)
        obj = ctx.view_repo.get(sha1)
        if obj.type == pygit2.GIT_OBJ_TAG:
            LOG.debug("_add_tag() annotated tag {}".format(name))
            op = p4gf_git.object_path(sha1)
            os.link(op, fpath)
        else:
            # Lightweight tags can be anything: commit, tree, blob
            LOG.debug("_add_tag() lightweight tag {}".format(name))
            with open(fpath, 'wb') as f:
                f.write(name.encode('UTF-8'))
        add_list.append(fpath)


def _remove_tag(ctx, name, sha1, edit_list, delete_list):
    """
    Remove the tag from the object cache. If removing one of several
    lightweight tags which reference the same object, the corresponding
    file will be edited rather than deleted.
    """
    LOG.debug("_remove_tag() removing tag {}".format(name))
    fpath = os.path.join(ctx.gitlocalroot, _client_path(ctx, sha1))
    if not os.path.exists(fpath):
        # Already gone (or never stored), nothing else to do.
        return
    with open(fpath, 'rb') as f:
        contents = f.read()
    try:
        zlib.decompress(contents)
        # Must be an annotated tag...
        delete_list.append(fpath)
    except zlib.error:
        tag_names = contents.decode('UTF-8').splitlines()
        tag_names.remove(name)
        if tag_names:
            contents = '\n'.join(tag_names).encode('UTF-8')
            os.chmod(fpath, 0o644)
            with open(fpath, 'wb') as f:
                f.write(contents)
            edit_list.append(fpath)
        else:
            delete_list.append(fpath)


def _get_tag_target(repo, sha1):
    """
    Return the pygit2 object referred to by the tag given by the SHA1.
    """
    obj = repo.get(sha1)
    if obj.type == pygit2.GIT_OBJ_TAG:
        # Get the tag's target object
        obj = repo.get(obj.target)
    return obj


#pylint:disable=R0912,R0915
def process_tags(ctx, prl):
    """
    For each tag reference in the pre-receive-tuple list, add or remove the
    object from the Git Fusion mirror.

    Arguments:
        ctx - P4GF context with initialized pygit2 Repository.
        prl - list of PreReceiveTuple objects.

    Returns None if successful and an error string otherwise.
    """
    tags = [prt for prt in prl if prt.ref.startswith('refs/tags/')]
    if not tags:
        LOG.debug("process_tags() no incoming tags to process")
        return None

    # Screen the tags to ensure their names won't cause problems sometime
    # in the future (i.e. when we create Perforce labels). Several of these
    # characters are not allowed in Git tag names anyway, but better to
    # check in case that changes in the future.
    # In particular git disallows a leading '-', but we'll check for it anyway
    # Otherwise allow internal '-'
    regex = re.compile(r'[*@#,]|\.\.\.|%%')
    for prt in tags:
        tag = prt.ref[10:]
        if regex.search(tag) or tag.startswith('-'):
            return _("illegal characters (@#*,...%%) in tag name: '{}'").format(tag)

    if not ctx.view_repo:
        # In some cases the Git repository object is not yet created.
        ctx.view_repo = pygit2.Repository(ctx.view_dirs.GIT_DIR)

    LOG.debug("process_tags() beginning...")
    tags_path = "objects/repos/{repo}/tags/...".format(repo=ctx.config.view_name)
    with ctx.p4gf.at_exception_level(P4.P4.RAISE_NONE):
        # Raises an exception when there are no files to sync?
        ctx.p4gfrun(['sync', '-q', "//{}/{}/...".format(ctx.config.p4client_gf, tags_path)])

    # Decide what to do with the tag references.
    tags_to_delete = []
    tags_to_add = []
    tags_to_edit = []
    for prt in tags:
        tag = prt.ref[10:]
        if prt.old_sha1 == p4gf_const.NULL_COMMIT_SHA1:
            if prt.new_sha1 == p4gf_const.NULL_COMMIT_SHA1:
                # No idea how this happens, but it did, so guard against it.
                sys.stderr.write(_('Ignoring double-zero pre-receive-tuple line'))
                continue
            # Adding a new tag; if it references a commit, check that it
            # exists; for other types, it is too costly to verify
            # reachability from a known commit, so just ignore them.
            obj = _get_tag_target(ctx.view_repo, prt.new_sha1)
            is_commit = obj.type == pygit2.GIT_OBJ_COMMIT
            if is_commit and not ObjectType.commits_for_sha1(ctx, obj.hex):
                return _("Tag '{}' references unknown objects."
                         " Push commits before tags.").format(tag)
            if obj.type == pygit2.GIT_OBJ_TREE:
                sys.stderr.write(_("Tag '{}' of tree will not be stored in Perforce\n").format(tag))
                continue
            if obj.type == pygit2.GIT_OBJ_BLOB:
                sys.stderr.write(_("Tag '{}' of blob will not be stored in Perforce\n").format(tag))
                continue
            _add_tag(ctx, tag, prt.new_sha1, tags_to_edit, tags_to_add)
        elif prt.new_sha1 == p4gf_const.NULL_COMMIT_SHA1:
            # Removing an existing tag
            _remove_tag(ctx, tag, prt.old_sha1, tags_to_edit, tags_to_delete)
        else:
            # Older versions of Git allowed moving a tag reference, while
            # newer ones seemingly do not. We will take the new behavior as
            # the correct one and reject such changes.
            return _('Updates were rejected because the tag already exists in the remote.')

    # Seemingly nothing to do.
    if not tags_to_add and not tags_to_edit and not tags_to_delete:
        LOG.debug("process_tags() mysteriously came up empty")
        return None

    # Add and remove tags as appropriate, doing so in batches.
    LOG.info("adding {} tags, removing {} tags from Git mirror".format(
        len(tags_to_add), len(tags_to_delete)))
    desc = _("Git Fusion '{repo}' tag changes").format(repo=ctx.config.view_name)
    with p4gf_util.NumberedChangelist(gfctx=ctx, description=desc) as nc:
        while len(tags_to_add):
            bite = tags_to_add[:_BITE_SIZE]
            tags_to_add = tags_to_add[_BITE_SIZE:]
            ctx.p4gfrun(["add", "-t", "binary+F", bite])
        while len(tags_to_edit):
            bite = tags_to_edit[:_BITE_SIZE]
            tags_to_edit = tags_to_edit[_BITE_SIZE:]
            ctx.p4gfrun(["edit", "-k", bite])
        while len(tags_to_delete):
            bite = tags_to_delete[:_BITE_SIZE]
            tags_to_delete = tags_to_delete[_BITE_SIZE:]
            ctx.p4gfrun(["delete", bite])
        nc.submit()
        if nc.submitted:
            _write_last_copied_tag(ctx, nc.change_num)
    LOG.debug("process_tags() complete")
    return None
#pylint:enable=R0912,R0915


def _calc_last_copied_tag_counter_name(view_name, server_id):
    '''
    Return a counter that holds the changelist number of the most recently
    updated tag on the given Git Fusion server.
    '''
    return p4gf_const.P4GF_COUNTER_LAST_COPIED_TAG.format(repo_name=view_name, server_id=server_id)


def _last_copied_tag_counter_name(ctx):
    '''
    Return the name of a counter that holds the latest tag changelist
    number for this Git Fusion server.
    '''
    return _calc_last_copied_tag_counter_name(ctx.config.view_name, p4gf_util.get_server_id())


def _read_last_copied_tag(ctx):
    '''
    Return the changelist number for the most recent tags change for this
    Git Fusion server.
    '''
    r = ctx.p4gfrun(['counter', '-u', _last_copied_tag_counter_name(ctx)])
    return r[0]['value']


def _write_last_copied_tag(ctx, change_num):
    '''
    Update the changelist number for the most recent tags change for this
    Git Fusion server.
    '''
    ctx.p4gfrun(['counter', '-u', _last_copied_tag_counter_name(ctx), change_num])


def any_tags_since_last_copy(ctx):
    """
    Return True if there is at least one new change to the tags since the
    last time we copied between Git and Perforce.
    """
    last_copied_change = _read_last_copied_tag(ctx)
    tags_path = '{root}/repos/{repo}/tags/...@{num},#head'.format(
        root=p4gf_const.objects_root(),
        repo=ctx.config.view_name,
        num=1 + int(last_copied_change))
    r = ctx.p4gfrun(['changes', '-m1', tags_path])
    new = True if r else False
    LOG.debug('any_tags_since_last_copy() found new tags: {}'.format(new))
    return new


def _create_tag_ref(repo, name, sha1):
    """
    Create a single tag reference in the repository.
    """
    if not name or not sha1:
        LOG.warning("_create_tag_ref() invalid params: ({}, {})".format(name, sha1))
        return
    if repo.get(sha1) is None:
        LOG.warning("_create_tag_ref() unknown object: {}".format(sha1))
        return
    tag_refs = os.path.join('.git', 'refs', 'tags')
    if not os.path.exists(tag_refs):
        os.makedirs(tag_refs)
    tag_file = os.path.join(tag_refs, name)
    tag_path = os.path.dirname(tag_file)
    if not os.path.exists(tag_path):
        os.makedirs(tag_path)
    with open(tag_file, 'w') as f:
        f.write(sha1)


def _remove_tag_ref(name, sha1):
    """
    Remove a single tag reference from the repository.
    """
    if not name or not sha1:
        LOG.warning("_remove_tag_ref() invalid params: ({}, {})".format(name, sha1))
        return
    tag_refs = os.path.join('.git', 'refs', 'tags')
    if not os.path.exists(tag_refs):
        return
    tag_file = os.path.join(tag_refs, name)
    if os.path.exists(tag_file):
        os.unlink(tag_file)


def _install_tag(repo, fname):
    """
    Given the path of a tag copied from Perforce object cache, copy
    the tag to the repository, with the appropriate name and SHA1.
    There may be multiple lightweight tags associated with the same
    SHA1, in which case multiple tags will be created.

    Arguments:
        repo -- pygit2 repository
        fname -- clientFile attr for sync'd tag
    """
    sha1 = fname[-42:].replace('/', '')
    LOG.debug("_install_tag() examining {}...".format(sha1))
    with open(fname, 'rb') as f:
        contents = f.read()
    try:
        zlib.decompress(contents)
        # Must be an annotated tag...
        blob_dir = os.path.join('.git', 'objects', sha1[:2])
        if not os.path.exists(blob_dir):
            os.makedirs(blob_dir)
        blob_path = os.path.join(blob_dir, sha1[2:])
        os.link(fname, blob_path)
        tag_obj = repo.get(sha1)
        tag_name = tag_obj.name
        LOG.debug("_install_tag() annotated tag {}".format(tag_name))
        _create_tag_ref(repo, tag_name, sha1)
    except zlib.error:
        # Lightweight tags are stored simply as the tag name, but
        # there may be more than one name for a single SHA1.
        tag_names = contents.decode('UTF-8')
        for name in tag_names.splitlines():
            LOG.debug("_install_tag() lightweight tag {}".format(name))
            _create_tag_ref(repo, name, sha1)


def _uninstall_tag(repo, fname):
    """
    Given the path of a tag copied from Perforce object cache, remove
    the tag from the repository.

    Arguments:
        repo -- pygit2 repository
        fname -- clientFile attr for sync'd tag
    """
    sha1 = fname[-42:].replace('/', '')
    LOG.debug("_uninstall_tag() examining {}...".format(sha1))
    with open(fname, 'rb') as f:
        contents = f.read()
    try:
        zlib.decompress(contents)
        # Must be an annotated tag...
        tag_obj = repo.get(sha1)
        tag_name = tag_obj.name
        LOG.debug("_uninstall_tag() annotated tag {}".format(tag_name))
    except zlib.error:
        # Lightweight tags are stored simply as the tag name
        tag_name = contents.decode('UTF-8')
        LOG.debug("_uninstall_tag() lightweight tag {}".format(tag_name))
    # Remove the tag reference
    tag_refs = os.path.join('.git', 'refs', 'tags')
    if not os.path.exists(tag_refs):
        return
    tag_file = os.path.join(tag_refs, tag_name)
    if os.path.exists(tag_file):
        os.unlink(tag_file)


def _read_tags(ctx, depot_path, change_num=None):
    """
    Return the set of (lightweight) tag names read from the given file.
    """
    if change_num:
        cmd = ['sync', '-f', "{}@{}".format(depot_path, change_num)]
    else:
        cmd = ['sync', '-f', depot_path]
    r = ctx.p4gfrun(cmd)
    r = p4gf_util.first_dict(r)
    with open(r['clientFile'], 'rb') as f:
        contents = f.read()
    tag_names = contents.decode('UTF-8').splitlines()
    return set(tag_names)


def update_tags(ctx):
    """
    Based on the recent changes to the tags, update our repository
    (remove deleted tags, add new pushed tags).
    """
    if not ctx.view_repo:
        # In some cases the Git repository object is not yet created.
        ctx.view_repo = pygit2.Repository(ctx.view_dirs.GIT_DIR)

    last_copied_change = _read_last_copied_tag(ctx)
    tags_path = '{root}/repos/{repo}/tags/...@{num},#head'.format(
        root=p4gf_const.objects_root(),
        repo=ctx.config.view_name,
        num=1 + int(last_copied_change))
    r = ctx.p4gfrun(['changes', '-s', 'submitted', tags_path])
    changes = sorted(r, key=lambda k: int(k['change']))
    for change in changes:
        d = ctx.p4gfrun(['describe', change['change']])
        d = p4gf_util.first_dict(d)
        for d_file, action in zip(d['depotFile'], d['action']):
            if action == 'add':
                r = ctx.p4gfrun(['sync', '-f', d_file])
                r = p4gf_util.first_dict(r)
                _install_tag(ctx.view_repo, r['clientFile'])
            elif action == 'delete':
                change_num = int(change['change']) - 1
                r = ctx.p4gfrun(['sync', '-f', "{}@{}".format(d_file, change_num)])
                r = p4gf_util.first_dict(r)
                _uninstall_tag(ctx.view_repo, r['clientFile'])
            elif action == 'edit':
                # get the tags named in the file prior to this change
                tags_before = _read_tags(ctx, d_file, int(change['change']) - 1)
                # get the tags named in the file after this change
                tags_after = _read_tags(ctx, d_file)
                # remove old (lightweight) tags and add new ones
                sha1 = d_file[-42:].replace('/', '')
                for old_tag in tags_before - tags_after:
                    _remove_tag_ref(old_tag, sha1)
                for new_tag in tags_after - tags_before:
                    _create_tag_ref(ctx.view_repo, new_tag, sha1)
            else:
                LOG.error("update_tags() received an unexpected change action: " +
                          "@{}, '{}' on {}".format(change['change'], action, d_file))
    _write_last_copied_tag(ctx, changes[-1]['change'])


def generate_tags(ctx):
    """
    Regenerate the original tags into the (rebuilt) Git repository.
    This should only be called when the repository was just rebuilt
    from Perforce, otherwise it will do a bunch of work for nothing.
    """
    # Fetch everything under //.git-fusion/objects/repos/<repo>/tags/...
    tags_path = NTR('objects/repos/{repo}/tags').format(repo=ctx.config.view_name)
    with ctx.p4gf.at_exception_level(P4.P4.RAISE_NONE):
        client_path = "//{}/{}/...".format(ctx.config.p4client_gf, tags_path)
        ctx.p4gfrun(['sync', '-f', '-q', client_path])

    # Walk the tree looking for tags, reconstituting those we encounter.
    tags_root = os.path.join(ctx.gitlocalroot, tags_path)
    for walk_root, _, files in os.walk(tags_root):
        for name in files:
            fname = os.path.join(walk_root, name)
            _install_tag(ctx.view_repo, fname)

    # Update the tag change counter to avoid repeating our efforts.
    last_copied_change = _read_last_copied_tag(ctx)
    tags_path = '{root}/repos/{repo}/tags/...@{num},#head'.format(
        root=p4gf_const.objects_root(),
        repo=ctx.config.view_name,
        num=1 + int(last_copied_change))
    r = ctx.p4gfrun(['changes', '-m1', '-s', 'submitted', tags_path])
    changes = p4gf_util.first_dict(r)
    if changes:
        _write_last_copied_tag(ctx, changes['change'])
