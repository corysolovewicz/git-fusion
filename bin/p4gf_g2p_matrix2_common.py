#! /usr/bin/env python3.3
'''
Common Matrix 2 code that several modules use.

Avoid import cycles: import no Matrix 2 modules here.
'''

import logging

from p4gf_g2p_matrix2_cell     import G2PMatrixCell as Cell
from p4gf_g2p_matrix2_decided  import Decided

                        # Yes, explicitly grab top-level matrix2 here.
                        # Other modules should use common.LOG or
                        # common.LOG.getChild('xxx')
                        #
LOG = logging.getLogger('p4gf_g2p_matrix2')


# Git action conversion tables:
_EXISTS = True
GIT_ACTION_AM = {
        # If a file exists or is pending an integrate,
        # then 'Add'=='p4 add' won't work. Use 'Modify'=='p4 edit' .
      _EXISTS : { 'A'  : 'M'
                , 'M'  : 'M'
                , 'T'  : 'T'
                , 'D'  : 'D'
                , None : None }

        # If file neither exists nor is part of a pending integrate, then
        # 'Modify'=='p4 edit' won't work, use 'Add'=='p4 add' instead. Don't
        # even both with trying to 'Delete'=='p4 delete'ing a file that does
        # not exist.
, not _EXISTS : { 'A'  : 'A'
                , 'M'  : 'A'
                , 'T'  : 'A'
                , 'D'  : None
                , None : None }
}
# See p4gf_g2p_matrix2_row for this:
#
# GIT_TO_P4_ACTION = { 'A'  : 'add'
#                    , 'M'  : 'edit'
#                    , 'T'  : 'edit'
#                    , 'D'  : 'delete'
#                    , None : None
#                    }

def debug3(msg, *arg, **kwarg):
    '''
    If logging at DEBUG3, do so. If not, do nothing.
    '''
    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3(msg.format(*arg, **kwarg))


def debug2(msg, *arg, **kwarg):
    '''
    If logging at DEBUG2, do so. If not, do nothing.
    '''
    if LOG.isEnabledFor(logging.DEBUG2):
        LOG.debug2(msg.format(*arg, **kwarg))


def gdest_cell_exists_in_p4(gdest_cell):
    '''
    Does this cell.discovered contain a depotFile?

    Returns True even if depotFile is deleted at head.
    '''
    return Cell.safe_discovered(gdest_cell, 'depotFile') != None


def gdest_cell_deleted_at_head(gdest_cell):
    '''
    Does this cell.discovered contain a depotFile?
    Is that depotFile deleted at GDEST's revision (which we assume is #head)?
    '''
    action = Cell.safe_discovered(gdest_cell, 'action')
    return (    action
            and 'delete' in gdest_cell.discovered['action'])


def apply_git_delta_cell(gdest_cell, git_delta_cell):
    '''
    If git-fast-export or git-diff-tree says to Add/Modify/Delete
    this GWT path, then do so.

    Internally only applies to row if we've no integ action. The big
    integ decision table already factors in Git actions when integrating.
    '''
    git_action = Cell.safe_discovered(git_delta_cell, 'git-action')
    if not git_action:
        return

    exists = (        gdest_cell_exists_in_p4(gdest_cell)
              and not gdest_cell_deleted_at_head(gdest_cell))

    debug3('apply_git_delta_cell() e={e} g={g} '.format(e=exists, g=git_action))
    action = GIT_ACTION_AM[exists][git_action]
    if not action:
        return

    if not git_delta_cell.decided:
        git_delta_cell.decided = Decided()
    git_delta_cell.decided.add_git_action(action)


