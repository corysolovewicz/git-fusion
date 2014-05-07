#! /usr/bin/env python3
'''
preflight-commit-require-case.py

A Git Fusion preflight-commit script that prohibits any 'git push' of a commit
that introduces a file with uppercase in its filename.

To use globally, edit //.git-fusion/p4gf_config to include these lines:
  [git-to-perforce]
  preflight-commit = /path/to/preflight-commit-require-job.py %formfile%

To use for just a specific repo, edit //.git-fusion/repos/<repo-name>/p4gf_config
to include these lines:
  [@repo]
  preflight-commit = /path/to/preflight-commit-require-job.py %formfile%
'''
import sys
import os

P4PASS = 0
P4FAIL = 1


def acceptable(depot_path):
    '''Is this depot path acceptable?'''

    # Get just the filename.ext portion of the path.
    (dir_path, file_ext) = os.path.split(depot_path)

    # Reject uppercase.
    return file_ext == file_ext.lower()

    # If adapting this sample to your needs, never apply case
    # rules to the Git Fusion depot branch identifier portion of the path, which
    # is a 22-character encoded GUID and will almost always contain both upper
    # and lowercase characters.


def iter_depot_paths(spec_text):
    '''
    Generator that produces all the depot_paths listed in the "Files:" section
    of a spec-like file.
    '''
                        # Scan until you see double-newline, Files:, and then a
                        # list of tab-prefixed depot paths
    nn_files_n = '\n\nFiles:\n'
    i = spec_text.find(nn_files_n)
    if i <= 0:
        sys.stderr.write('Malformed spec. Bug in Git Fusion.')
        exit(P4FAIL)

    lines_start_i = i + len(nn_files_n)

                        # Scan depot paths until we run out.
    file_lines = spec_text[lines_start_i:].splitlines()
    for file_line in file_lines:
        if not file_line.startswith('\t'):
            break
        yield(file_line[1:])


def main():
    '''Main entry point.'''
    formfile = sys.argv[1]

    # Read in the commit as a Perforce changelist-like spec text form.
    with open(formfile, 'r') as f:
        spec_text = f.read()

    # Check depot paths for proper case.
    all_acceptable = True
    for depot_path in iter_depot_paths(spec_text):
        if not acceptable(depot_path):
            sys.stderr.write('Incorrect file path case: {}'.format(depot_path))
            all_acceptable = False

    if not all_acceptable:
        exit(P4FAIL)

    exit(P4PASS)


if __name__ == '__main__':
    main()

