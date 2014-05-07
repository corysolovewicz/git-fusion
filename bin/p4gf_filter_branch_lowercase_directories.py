#! /usr/bin/env python3.3
'''
git-filter-branch operator to convert all directories to lowercase.

To use:
 git filter-branch --tree-filter 'p4gf_filter_branch_lowercase_directories.py .' master

or if you want ALL branches cleaned up, use -- --all (yes, that's an
intentional "--" before the "--all" option which git-filter-branch relays to
git-rev-list
 git filter-branch --tree-filter 'p4gf_filter_branch_lowercase_directories.py .' -- --all

Only works when run on a case-senstive filesystem. Does not work on a
Mac or Windows box unless you've specifically set your filesystem to
be case sensitive.

Fails with collision if filesystem contains both upper and
lowercase versions of same directory name.

You can _almost_ do this in a shell script, except for quote"/tick'/space
characters that are downright ornery when it comes to passing them to 'mv'.
For a shell script that _almost_ works, see
  http://stackoverflow.com/questions/152514/how-to-rename-all-folders-and-files-to-lowercase-on-linux
'''
import os
import sys
import shutil

try:
    from   p4gf_l10n      import _
except ImportError:
    def _(x):
        '''NOP replacement for i18n function gettext()'''
        return x

def main():
    '''Main entry point'''
    collisions = dict()

    root_list = sys.argv
    if not root_list:
        root_list = ['.']

    for root in root_list:
        for (dir_path, dir_name) in dir_iter(root):
            dir_name_lower = dir_name.lower()
            if dir_name == dir_name_lower:
                continue

            src = os.path.join(dir_path, dir_name)
            dst = os.path.join(dir_path, dir_name_lower)

            if os.path.lexists(dst):
                        # Cannot rename, there's already something
                        # there at dst.
                collisions[src] = dst
                continue

            shutil.move(src, dst)

    if collisions:
        for src in sorted(collisions.keys()):
            sys.stderr.write(_("Case collision: {src:<30}  {dst}")
                             .format(src=src, dst=collisions[src]) + "\n")
        exit(1)

def dir_iter(root):
    '''
    Visit every directory within root, including root itself.

    Sequence is deepmost first so that we rename things on our
    way out of the recursive walk, avoiding any complication
    due to renaming a container out from under its entry in
    our to-do list.
    '''
    for (dir_path, dir_list, _file_list) in os.walk(root, topdown=False):
        for dir_name in dir_list:
            yield (dir_path, dir_name)


main()

