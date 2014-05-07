#! /usr/bin/env python3.3

'''
Convert between a Perforce changelist number and N git-fast-import marks.

We used to have a 1:1 correspondence, used the Perforce changelist number as
the git-fast-import mark.

But now that a single Perforce changelist can intersect multiple Git Fusion
branch views, a single Perforce changelist can map to multuple, distinct, Git
commits. So we have to decouple 1:1 and go to 1:n.

Internal storage is an ordered list of N changelist numbers. Sorted lists of
scalars can be passed to bisect.bisect_left() for O(ln(N)) fast lookup
'''
import bisect

from   p4gf_l10n    import _

class MarkList:
    '''
    Associate one or more git-fast-import mark numbers with a single Perforce
    changelist number.

    Returned marks are always integers not strings.

    Input changelists may be integers or strings.
    Always stored internally and returned as ints.
    '''

    def __init__(self):
        self.change_list    = []    # change_list[i] is the changelist number
                                    # assigned to mark (i+1)

    def assign(self, changelist_number):
        '''
        Assign and return a new mark number for a changelist.
        '''
        # Force to integer type so we can do numeric checks for bad input.
        changelist_number_int = int(changelist_number)
        # Reject bad input.
        if changelist_number_int < 1:
            raise RuntimeError(_('Changelist numbers must be positive integers.'))
        if self.change_list and (changelist_number_int < self.change_list[-1]):
            raise RuntimeError(_('Changelist numbers must repeat or increase,'
                                 ' must not decrease.'))

        next_mark = 1 + len(self.change_list)
        self.change_list.append(changelist_number_int)
        return next_mark

    def cl_to_mark_list(self, changelist_number):
        '''
        Return a list of all previously assign()ed mark numbers for a changelist.
        Return empty list if we never assign()ed a mark for this changelist.
        '''
        changelist_number_int = int(changelist_number)
        lower_bound = bisect.bisect_left(self.change_list, changelist_number_int)
        result = []
        for i in range(lower_bound, len(self.change_list)):
            if changelist_number_int == self.change_list[i]:
                result.append(i+1)
            else:
                break
        return result

    def mark_to_cl(self, mark_number):
        '''
        Return the one and only changelist to which this mark was assign()ed.

        Return None if mark_number was never assign()ed.
        '''
        mark_number_int = int(mark_number)
        if (    (mark_number_int < 1)
             or (1 + len(self.change_list) <= mark_number_int)):
            return None
        return self.change_list[mark_number_int-1]
