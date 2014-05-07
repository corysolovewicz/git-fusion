#! /usr/bin/env python3
'''
preflight-commit-require-job.py

A Git Fusion preflight-commit script that prohibits any 'git push' of a commit
that does not have at least one Perforce Job attached.

To use globally, edit //.git-fusion/p4gf_config to include these lines:
  [git-to-perforce]
  preflight-commit = /path/to/preflight-commit-require-job.py %formfile%

To use for just a specific repo, edit //.git-fusion/repos/<repo-name>/p4gf_config
to include these lines:
  [@repo]
  preflight-commit = /path/to/preflight-commit-require-job.py %formfile%
'''
import sys

jobs = [x for x in sys.argv[1:] if x]

if not jobs:
    sys.stderr.write('Jobs required\n')
    exit(1)

exit(0)


