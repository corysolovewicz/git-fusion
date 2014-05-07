#! /usr/bin/env python3.3
"""
Functions for operating on Git repositories.
"""

import binascii
import configparser
import io
import os
import shutil
import zlib

import pygit2

import p4gf_char
import p4gf_const
import p4gf_log
from   p4gf_l10n    import _
import p4gf_proc

LOG = p4gf_log.for_module()


def set_bare(is_bare):
    '''
    Reconfigure a repo for --bare or not-bare. Assumes current working
    directory is the Git repository to modify.
    '''
    ### this explodes the Python process, in p4api.so, oddly enough
    # path = pygit2.discover_repository('.')
    # repo = pygit2.Repository(path)
    # repo.config['core.bare'] = 'true' if is_bare else 'false'
    if is_bare:
        value = 'true'
    else:
        value = 'false'
    cmd = ['git', 'config', '--replace', 'core.bare', value]
    p4gf_proc.popen(cmd)


def _setup_temp_repo():
    """
    Set up a temporary Git repository in which to house pack files
    for unpacking into another repository.

    Returns the path to the new .git/objects/pack directory.
    """
    tmpname = 'p4gf_git_tmp'
    tmprepo = os.path.join(os.path.dirname(os.getcwd()), tmpname)
    if os.path.exists(tmprepo):
        shutil.rmtree(tmprepo)
    pygit2.init_repository(tmprepo)
    packdir = os.path.join(tmprepo, '.git', 'objects', 'pack')
    return (tmprepo, packdir)


def unpack_objects():
    """
    Find all existing pack objects in the Git repository, unpack them,
    and then remove the now defunct pack and index files.

    Returns True if successful, False otherwise.
    """
    pack_dir = os.path.join(".git", "objects", "pack")
    if not os.path.exists(pack_dir):
        return True
    pack_files = [os.path.join(pack_dir, f) for f in os.listdir(pack_dir) if f.endswith('.pack')]
    if pack_files:
        tmprepo, tmp_pack = _setup_temp_repo()
        if not tmp_pack:
            return False
        cmd = ['git', 'unpack-objects', '-q']
        for pack in pack_files:
            fname = os.path.basename(pack)
            newpack = os.path.join(tmp_pack, fname)
            os.rename(pack, newpack)
            index = pack[:-4] + "idx"
            os.rename(index, os.path.join(tmp_pack, fname[:-4] + "idx"))
            ec = p4gf_proc.wait(cmd, stdin=newpack)
            if ec:
                raise RuntimeError(_("git-unpack-objects failed with '{}'").format(ec))
        shutil.rmtree(tmprepo)
    return True


def cat_file_to_local_file(sha1, p4filetype, local_file):
    """
    Perform the equivalent of the git-cat-file command on the given object.
    Write content to given local_file path.

    Assumes .git/objects hierarchy stored completely loose, no packed objects.
    """
    chunksize      = 64 * 1024
    decompressor   = zlib.decompressobj()
    header         = None

    if p4filetype == 'symlink':
        with open(_cat_file_sha1_to_path(sha1), 'rb') as fin:
            chunk = fin.read(chunksize)
            de_bytes = decompressor.decompress(chunk)
            # Strip header.
            i = de_bytes.find(0)
            assert 0 < i
            header   = de_bytes[:i]
            de_bytes = de_bytes[i+1:]
            os.symlink(de_bytes, local_file)
    else:
        with open(_cat_file_sha1_to_path(sha1), 'rb') as fin\
           , open(local_file                  , 'wb') as fout:

            while True:
                chunk = fin.read(chunksize)
                if not chunk:
                    break
                de_bytes = decompressor.decompress(chunk)

                if not header:
                    # Strip header. Once.
                    i = de_bytes.find(0)
                    assert 0 < i
                    header   = de_bytes[:i]
                    de_bytes = de_bytes[i+1:]

                fout.write(de_bytes)

            # Flush any stragglers from the decompression buffer.
            de_bytes = decompressor.flush()
            fout.write(de_bytes)


def _cat_file_sha1_to_path(sha1):
    '''
    Error-checking code common to cat_file() and cat_file_to_local_file().
    Return path to local .git/objects/xxx file if exists, None if not.
    '''
    if len(sha1) != 40:
        raise RuntimeError(_('malformed SHA1: {}').format(sha1))
    if not os.path.exists(".git"):
        LOG.error("No Git repository found in {}".format(os.getcwd()))
    path = object_path(sha1)
    if path is None:
        LOG.warn("cat_file() file {} not found".format(sha1))
        return None
    return path


def cat_file(sha1):
    """
    Perform the equivalent of the git-cat-file command on the given object.
    Returns an empty bytes object if the file cannot be found.

    Operates in memory, so please do not call this for blobs of unusual size.
    Use only for commit and tree and other small objects.

    Assumes .git/objects hierarchy stored completely loose, no packed objects.
    """
    with open(_cat_file_sha1_to_path(sha1), 'rb') as f:
        blob = f.read()
    data = zlib.decompress(blob)
    return data


def get_commit(sha1):
    """
    Retrieve the text of a Git commit given its SHA1.
    Returns an empty string if the commit file cannot be found.
    """
    blob = cat_file(sha1)
    blob = blob[blob.index(b'\x00') + 1:]
    return p4gf_char.decode(blob)


def object_exists(sha1):
    """
    Check if a Git object exists. Caller should invoke unpack_objects() first.
    """
    return object_path(sha1) is not None


def object_path(sha1):
    """
    Get the path to a Git object, returning None if it does not exist.
    Caller should invoke unpack_objects() first.
    """
    # Files may be named de/adbeef... or de/ad/beef... in .git/objects directory
    base = os.path.join(".git", "objects")
    if not os.path.exists(base):
        LOG.error("Git objects directory missing!")
    path = os.path.join(base, sha1[:2], sha1[2:])
    if not os.path.exists(path):
        path = os.path.join(base, sha1[:2], sha1[2:4], sha1[4:])
        if not os.path.exists(path):
            return None
    return path


def tree_from_commit(blob):
    """
    For the given commit object data (an instance of bytes), extract the
    corresponding tree SHA1 (as a str). The object header should not be
    part of the input.
    """
    if not isinstance(blob, bytes):
        LOG.error("tree_from_commit() expected bytes, got {}".format(type(blob)))
        return None
    if len(blob) == 0:
        LOG.error("tree_from_commit() expected non-zero bytes")
        return None
    idx = 0
    end = len(blob)
    try:
        while idx < end and blob[idx:idx + 5] != b'tree ':
            idx = blob.index(b'\n', idx + 1) + 1
    except ValueError:
        return None
    nl = blob.index(b'\n', idx + 1)
    return blob[idx + 5:nl].decode()


def find_tree(path, tree):
    """Locate the tree object for the given path.

    Arguments:
        repo -- pygit2.Repository instance.
        path -- path for which to locate tree.
        tree -- initially the root tree object.

    Returns the pygit2 Tree object, or None if not found.

    """
    if not path or tree is None:
        return tree
    head, tail = os.path.split(path)
    if head:
        tree = find_tree(head, tree)
        if tree:
            tree = tree.to_object()
    return tree[tail] if tree and tail in tree else None


def make_tree(repo, path, tree):
    """Build a new tree structure from the given path.

    Given a tree object that represents the path, build up the parent
    trees, using whatever existing structure already exists in the
    repository. The result will be a tree object suitable for use in
    creating a Git commit.

    Arguments:
        repo -- pygit2.Repository instance.
        path -- path for which to build the tree structure.
        tree -- tree object to represent the path.

    Returns the pygit2 Tree object.

    """
    head, tail = os.path.split(path)
    if not head:
        rtree = None if repo.is_empty else repo.head.tree
        tb = repo.TreeBuilder(rtree) if rtree else repo.TreeBuilder()
        tb.insert(tail, tree.oid, pygit2.GIT_FILEMODE_TREE)
        return repo.get(tb.write())
    else:
        ptree = find_tree(head, tree)
        tb = repo.TreeBuilder(ptree) if ptree else repo.TreeBuilder()
        tb.insert(tail, tree.oid, pygit2.GIT_FILEMODE_TREE)
        ptree = repo.get(tb.write())
        return make_tree(repo, head, ptree)


def _add_to_gitmodules(repo, repo_name, path, url, tb):
    """Update the .gitmodules file and insert the entry in the tree.

    Given the path and URL for a Git repository, add a new submodule
    section to the .gitmodules file in this repository. The changes
    are made directly into the repository, without touching the working
    directory.

    The newly generated file blob will be inserted into the given
    instance of pygit2.TreeBuilder.

    Arguments:
        repo -- pygit2.Repository instance.
        repo_name -- name of the Git Fusion repository (e.g. depot_0xS_foo).
        path -- full path of the submodule.
        url -- URL used to access submodule.
        tb -- pygit2.TreeBuilder to insert entry into tree.

    """
    header = '[submodule "{}"]'.format(path)
    frm = "{header}\n\t{tag} = {repo}\n\tpath = {path}\n\turl = {url}\n"
    section = frm.format(header=header, tag=p4gf_const.P4GF_MODULE_TAG, repo=repo_name,
                         path=path, url=url)
    blob = None
    if not repo.is_empty:
        try:
            entry = repo.head.tree['.gitmodules']
        except KeyError:
            entry = None
        if entry:
            # modify file and hash to object store
            blob = repo[entry.oid]
            text = blob.data.decode('UTF-8')
            if header in text:
                # TODO: update the existing information?
                oid = entry.oid
            else:
                text = text + '\n' + section
                oid = repo.create_blob(text.encode('UTF-8'))
    if blob is None:
        # generate file and hash to object store
        oid = repo.create_blob(section.encode('UTF-8'))
    sha1 = binascii.hexlify(oid).decode()
    tb.insert('.gitmodules', sha1, pygit2.GIT_FILEMODE_BLOB)


# pylint:disable=R0913
# too many arguments
def add_submodule(repo, repo_name, path, sha1, url, user):
    """Add the named submodule to the repository.

    Adds or modifies the .gitmodules file at the root of the tree.

    Arguments:
        repo -- pygit2.Repository instance.
        repo_name -- name of the Git Fusion repository (e.g. depot_0xS_foo).
        path -- full path of the submodule.
        sha1 -- SHA1 of the submodule.
        url -- URL used to access submodule.
        user -- one of the p4gf_usermap 3-tuples.

    Returns the SHA1 of the new commit.

    """
    leading_path, sub_name = os.path.split(path)
    tree = None if repo.is_empty else repo.head.tree
    tree = find_tree(leading_path, tree)
    tb = repo.TreeBuilder(tree.oid) if tree else repo.TreeBuilder()
    action = 'Updating' if tree and sub_name in tree else 'Adding'
    tb.insert(sub_name, sha1, pygit2.GIT_FILEMODE_COMMIT)
    tree = repo.get(tb.write())
    if leading_path:
        tree = make_tree(repo, leading_path, tree)
    # This unfortunately wastes the previously built tree but simplicity
    # wins over complexity, as does working code.
    tb = repo.TreeBuilder(tree.oid)
    _add_to_gitmodules(repo, repo_name, path, url, tb)
    tree = repo.get(tb.write())
    # Are we actually changing anything?
    if not repo.is_empty and tree.oid == repo.head.tree.oid:
        # Nope, nothing changed
        return
    author = pygit2.Signature(user[2], user[1])
    message = '{} submodule {}'.format(action, path)
    parents = [] if repo.is_empty else [repo.head.oid]
    repo.create_commit('HEAD', author, author, message, tree.oid, parents)
# pylint:enable=R0913


def parse_gitmodules(repo):
    """Read the .gitmodules file and return an instance of ConfigParser.

    Arguments:
        repo -- pygit2.Repository instance.

    Returns an instance of ConfigParser which contains the contents of the
    .gitmodules file. If no such file was found, the parser will be empty.

    """
    parser = configparser.ConfigParser(interpolation=None)
    if not repo.is_empty:
        try:
            entry = repo.head.tree['.gitmodules']
            blob = repo[entry.oid]
            text = blob.data.decode('UTF-8')
            parser.read_string(text, source='.gitmodules')
        except KeyError:
            pass
    return parser


def _remove_from_gitmodules(repo, path, tb):
    """Update the .gitmodules file and return the new SHA1.

    Arguments:
        repo -- pygit2.Repository instance.
        repo_name -- name of the Git Fusion repository (e.g. depot_0xS_foo).
        path -- full path of the submodule.
        sha1 -- SHA1 of the submodule.
        url -- URL used to access submodule.
        user -- one of the p4gf_usermap 3-tuples.

    """
    modules = parse_gitmodules(repo)
    section_to_remove = None
    for section in modules.sections():
        # we can only consider those submodules under our control
        if modules.has_option(section, p4gf_const.P4GF_MODULE_TAG):
            mpath = modules.get(section, 'path', raw=True, fallback=None)
            if path == mpath:
                section_to_remove = section
                break
    if not section_to_remove:
        return
    modules.remove_section(section_to_remove)
    out = io.StringIO()
    sections = modules.sections()
    count = len(sections)
    pos = 0
    for section in sections:
        out.write('[{name}]\n'.format(name=section))
        for key, value in modules.items(section):
            out.write('\t{key} = {value}\n'.format(key=key, value=value))
        pos += 1
        if pos < count:
            out.write('\n')
    oid = repo.create_blob(out.getvalue().encode('UTF-8'))
    sha1 = binascii.hexlify(oid).decode()
    tb.insert('.gitmodules', sha1, pygit2.GIT_FILEMODE_BLOB)


def remove_submodule(repo, path, user):
    """Remove the submodule whose path matches the given path.

    Arguments:
        repo -- pygit2.Repository instance.
        path -- path of submodule to be removed.
        user -- one of the p4gf_usermap 3-tuples.

    """
    if repo.is_empty:
        return
    leading_path, sub_name = os.path.split(path)
    tree = find_tree(leading_path, repo.head.tree)
    if not tree:
        return
    tb = repo.TreeBuilder(tree.oid)
    tb.remove(sub_name)
    tree = repo.get(tb.write())
    if leading_path:
        tree = make_tree(repo, leading_path, tree)
    # This unfortunately wastes the previously built tree but simplicity
    # wins over complexity, as does working code.
    tb = repo.TreeBuilder(tree.oid)
    _remove_from_gitmodules(repo, path, tb)
    tree = repo.get(tb.write())
    # Are we actually changing anything?
    if tree.oid == repo.head.tree.oid:
        # Nope, nothing changed
        return
    author = pygit2.Signature(user[2], user[1])
    message = 'Removing submodule {}'.format(path)
    repo.create_commit('HEAD', author, author, message, tree.oid, [repo.head.oid])
