#! /usr/bin/env python3.3
'''
Parse git-fast-import mark output.
'''

class Mark:
    '''
    One line returned by git-fast-import:

    <mark> SP <commit-sha1>

    Right now, mark == changelist number (but that's about to change).
    '''
    def __init__(self):
        self.mark   = None # When set, is set to string representation of mark
                           # number. 1 or greater.
        self.sha1   = None
        self.branch = None # branch_id, not Branch pointer.

    @staticmethod
    def from_line(line):
        '''
        Parse a marks line of output from git-fast-import.
        '''
        parts = line.split(' ')
        result = Mark()
        result.mark = int(parts[0][1:])
        result.sha1 = parts[1].strip()
        result.branch = parts[2].strip() if len(parts) > 2 else None
        return result
