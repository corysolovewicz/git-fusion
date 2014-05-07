#! /usr/bin/env python3.3
"""Functions related to streams."""

from collections import OrderedDict
import logging

import P4

import p4gf_translate
import p4gf_util

LOG = logging.getLogger(__name__)


def _split_path(path):
    """Split the stream path into its parts."""

    def scan_token(path, pos):
        """Return the next (possibly quoted) path element."""
        if pos >= len(path):
            return pos, None
        if path[pos] == '"':
            i = path.find('"', pos + 1)
            pos += 1
            return i + 2, path[pos:i]
        else:
            i = path.find(' ', pos)
            if i < 0:
                i = len(path)
            return i + 1, path[pos:i]

    pos, path_type = scan_token(path, 0)
    pos, client_path = scan_token(path, pos)
    _, depot_path = scan_token(path, pos)
    return (path_type, client_path, depot_path)


def stream_import_exclude(stream_paths):
    """Convert import paths to exclude for conversion to submodules.

    If the stream lacks a "share ..." path entry, the input will be
    returned unmodified, meaning no submodules (can or) will be created.
    Without the top-level directory, we cannot add a .gitmodules file as
    it would be outside of the client view.

    Any import path entry that lacks a depot_path will not be considered
    a candidate for conversion to a submodule, since it is considered to
    be a part of the view. Likewise any path not ending with /... cannot
    be converted to a submodule (potentially overlaps the submodule with
    files from the stream).

    Nested paths are currently not supported; only the deepest path will
    be converted to a submodule.

    """
    #
    # share  ...                      -->  share   ...
    # import x/...   //depot/x/...@5  -->  exclude x/...
    # import y/...   //depot/y/...    -->  import  y/... //depot/y/...
    # import y/z/... //depot/y/z/...  -->  exclude y/z/...
    #
    if 'share ...' not in stream_paths:
        # Without a top-level directory, adding .gitmodules would fall
        # outside of the client view and thus fail when pushed to Perforce.
        return stream_paths
    numbered = list((n,) + _split_path(entry) for n, entry in enumerate(stream_paths))
    # sort by the view path to make nested paths easier to find
    numbered.sort(key=lambda e: e[2])
    # find path entries that could be imported as submodules
    submod_candidates = set()
    previous_view = None
    previous_pos = 0
    for pos, path_type, view_path, depot_path in numbered:
        if depot_path is None:
            continue
        # this operation does not concern itself with changelist specifiers
        if '@' in depot_path:
            depot_path = depot_path.split('@')[0]
        if previous_view and view_path.startswith(previous_view[:-4]):
            submod_candidates.remove(previous_pos)
        if not depot_path.endswith("/...") or not view_path.endswith("/..."):
            continue
        submod_candidates.add(pos)
        previous_pos = pos
        previous_view = view_path
    # restore the original order of the view paths
    numbered.sort(key=lambda e: e[0])
    # replace 'import' with 'exclude' in submodule candidates
    for pos in submod_candidates:
        entry = numbered[pos]
        numbered[pos] = (entry[0], 'exclude', entry[2], None)
    # drop the first column, convert back to possibly quoted strings
    results = []
    for pos, path_type, view_path, depot_path in numbered:
        qv = p4gf_util.enquote(view_path)
        qd = p4gf_util.enquote(depot_path) if depot_path else ''
        results.append("{} {} {}".format(path_type, qv, qd).strip())
    return results


def match_import_paths(v_paths, p_paths):
    """Match excludes to imports and return a list of tuples.

    Arguments:
        v_paths -- paths from virtual stream.
        p_paths -- paths from parent stream.

    Returns:
        List of tuples of parent paths that are of type import
        for which the virtual stream had excluded. The tuples
        consist of the view path and the depot path.

    """
    v_list = [_split_path(entry) for entry in v_paths]
    p_list = [_split_path(entry) for entry in p_paths]
    results = []
    for v_type, v_vpath, _v_dpath in v_list:
        if v_type == 'exclude':
            for p_type, p_vpath, p_dpath in p_list:
                if p_vpath == v_vpath and p_type == 'import':
                    if '@' in p_dpath:
                        p_dpath = p_dpath.split('@')[0]
                    results.append((p_vpath, p_dpath))
    return results


def stream_view_submods(submod_views, change_views=None):
    """Produce a list of tuples suitable for creating submodule repos.

    Arguments:
        submod_views -- list of stream view entries.
        change_views -- optional list of view/change lines from stream's ChangeView field.

    Returns:
        list of tuples consisting of depot path, change number, local path;
        the change number will be None if change_views did not have a match

    """
    # make the order predictable for testing purposes
    submod_map = OrderedDict()
    change_nums = OrderedDict()
    p4map = P4.Map(submod_views)
    for left, right in zip(p4map.lhs(), p4map.rhs()):
        submod_map[left] = right
        change_nums[left] = None
    if change_views:
        for cview in change_views:
            cpath, cnum = cview.split('@')
            if cpath in submod_map:
                change_nums[cpath] = cnum
    results = []
    for cpath, cnum in change_nums.items():
        results.append((cpath, cnum, submod_map[cpath]))
    return results


def stream_imports_with_changes(view, change_view, import_paths):
    """Return import paths with change numbers for given views."""
    #
    # >>> stream['View']
    # ['//flow/dev/... ...',
    #  '//depot/x/... x/...',
    #  '//flow/main/y/... y/...',
    #  '//depot/z/... z/...']
    #
    # >>> stream['ChangeView']
    # ['//depot/z/...@10']
    #
    # >>> client['View']
    # ['//flow/dev/... //flow_client/...',
    #  '//depot/x/... //flow_client/x/...',
    #  '//flow/main/y/... //flow_client/y/...',
    #  '//depot/z/... //flow_client/z/...']
    #
    # >>> import_paths
    # [('x/...', '//depot/x/...'),
    #  ('z/...', '//depot/z/...')]
    #
    view_changes = stream_view_submods(view, change_view)
    #
    # >>> view_changes
    # [('//flow/dev/...', None, '...'),
    #  ('//depot/x/...', None, 'x/...'),
    #  ('//flow/main/y/...', None, 'y/...'),
    #  ('//depot/z/...', '10', 'z/...')]
    #
    imports = {entry[1] for entry in import_paths}
    results = [tup for tup in view_changes if tup[0] in imports]
    #
    # >>> results
    # [('//depot/x/...', None, 'x/...'),
    #  ('//depot/z/...', '10', 'z/...')]
    #
    return results


def repo_name_from_depot_path(depot_path):
    """Translate the depot path into a safe repo name."""
    # Drop the leading double-slash and trailing ellipsis.
    if len(depot_path) > 2 and depot_path[0:2] == '//':
        depot_path = depot_path[2:]
    if len(depot_path) > 4 and depot_path[-4:] == '/...':
        depot_path = depot_path[:-4]
    return p4gf_translate.TranslateReponame.git_to_repo(depot_path)
