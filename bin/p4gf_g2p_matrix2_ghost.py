#! /usr/bin/env python3.3
'''
Ghost changelist decision matrix

Rearrange a Perforce depot branch so that it looks just like what Git expects
before we copy a Git commit. Put all branch management work in ghost changelists
so that we do not commingle Git actions with branch management actions in the
same Perforce changelist.

* populate a new depot branch (lightweight or fully populated)
  branch files from parent

* just-in-time branch for edit or delete
  branch files from fully populated basis

* rearrange to reuse existing depot branch (lightweight or fully populated)
  branch/add/edit/delete existing files in depot branch

= Rearrange =

Branch reuse looks like this. Some previous branch "A" is already merged back
into master or whatever. Then a new branch "B" appears, starting with Git commit
B4:
          -- A2                .-- B4
               .              /
        ... -- Mx -- ... -- M3


            P4IMPLY,
            also
            GDEST.p4            GHOST of GPARN      GDEST.git

     ... -- A2 . . . . * . . . .GH(M3) . . . . . . .B4
                       |        .                  /
                P4IMPLY->GPARN  .   GPARN->GDEST  /
                git-action      .   git-action --*
                                .               /
             .--- ...  * ... -- M3-------------
            /          |
           /    GPARFPN->GPARN
    GPARFPN     git-action


GPARFPN is only there if M3 is on a lightweight branch.

= Populate new depot branch =

Similar to "Rearrange", but...
* no P4IMPLY column
* P4IMPLY->GPARN action is Add for files that exist in GPARN, blank for
  all other files
* B4 is the first changelist on this currently empty depot branch.

= Just-in-time branch for edit or delete =

       GPARN
       also
       GDEST.p4

    ... B3 -- GH(B3) -- B4
   /
GPARFPN


= JIT-branch + (Rearrange or Populate) =

It is common to have both JIT-branch for edit or delete AND either rearrange or
populate in the same commit.

= What we run =
* git-diff-tree
* git-fast-export (run previously, we consume)
* p4 files
* p4 copy -n

Remember, Git knows nothing out lightweight branches, so it will report NOP for
files not yet branched into a lightweight branch.

A warning about "git-action" from git-fast-export. git-fast-export never reports
'A' for add. It reports 'M' for either "add" or "modify". We have to check for
file existence in Git first-parent to differentiate. I'm not adding Yet Another
Column here to handle that case. Easier to sanitize that input before we get to
this table.

'''
from   collections  import namedtuple
import logging

from p4gf_g2p_matrix2_cell import G2PMatrixCell as Cell
from p4gf_l10n             import NTR

LOG = logging.getLogger(__name__)

                        # pylint:disable=C0103
                        # Invalid function name _G, _UN_G.
                        # Yeah, they're short to keep the table width down.

# -- Inputs ---------------------------------------------------------------

                        # Set in all these values so debug code can
                        # differentiate between Ghost and non-Ghost input
                        # integers.
                        #
GHOST_BIT   = 0b0100000000000000000000000000

                        # Does this file already exist in the destination
                        # Perforce depot branch. Look for 'depotFile' and
                        # non-delete 'action' in P4IMPLY.discovered.
                        #
P4IMPLY__E  = 0b0100000000000000000000000001      # Exists at rev.
P4IMPLY_NE  = 0b0100000000000000000000000010      # Never existed.
P4IMPLY_DL  = 0b0100000000000000000000000100      # Existed, but deleted at rev.
P4IMPLY___  = 0b0100000000000000000000000110      # NE | DL : doesn't exist
P4IMPLY_XX  = 0b0100000000000000000000000111

                        # "Rearrangement" Git actions: git-diff-tree A2 M3
                        # result. Stored in GHOST.discovered('git-action').
                        #
A           = 0b0100000000000000000000001000
M           = 0b0100000000000000000000010000
T           = 0b0100000000000000000000100000
D           = 0b0100000000000000000001000000
N           = 0b0100000000000000000010000000
X           = 0b0100000000000000000011111000  # P4IMPLY-to-GPARN actions
G_X         = 0b0100000000000001111100000000  # GPARN-to-GDEST actions
GFP_X       = 0b0100000000111110000000000000  # GPARFPN-to-GPARN actions

def _G(action) :
    '''Shift Git action bits into "GPARN->GDEST" bit positions.'''
    return ((action & ~GHOST_BIT) << 5) | GHOST_BIT

def _UN_G(action) :
    '''Shift Git action bits out of "GPARN->GDEST" bit positions.'''
    return ((action & ~GHOST_BIT) >> 5) | GHOST_BIT

def _GFP(action) :
    '''Shift Git action bits into "GPARN->GDEST" bit positions.'''
    return ((action & ~GHOST_BIT) << 10) | GHOST_BIT

def _UN_GFP(action) :
    '''Shift Git action bits out of "GPARN->GDEST" bit positions.'''
    return ((action & ~GHOST_BIT) >> 10) | GHOST_BIT

                        # Does this file exist in the source Perforce depot
                        # branch? Look for 'depotFile' and non-delete 'action'
                        # in either GPARN or GPARNFP.
                        #
P4GPARN_EP  = 0b0100000001000000000000000000       # E in GPARN itself
P4GPARN_EFP = 0b0100000010000000000000000000       # not E in GPARN, E in GPARFPN
P4GPARN_E   = 0b0100000011000000000000000000       # E in either GPARN or GPARFPN
P4GPARN__   = 0b0100000100000000000000000000       # not E in either
P4GPARN_X   = 0b0100000111000000000000000000

                        # Is the destination Perforce depot branch lightweight?
                        # Fully populated?
                        #
LW          = 0b0100001000000000000000000000
FP          = 0b0100010000000000000000000000
LW_X        = 0b0100011000000000000000000000

                        # Must we populate a new lightweight depot branch?
POP         = 0b0100100000000000000000000000
P__         = 0b0101000000000000000000000000
P_X         = 0b0101100000000000000000000000

# -- Outputs --------------------------------------------------------------

                        # What to do?
BRANCH     = NTR('branch')
EDIT       = NTR('edit')
DELETE     = NTR('delete')
IMPOSSIBLE = NTR('impossible')
ADD_DELETE = NTR('add+del')
NOP        = None



                        # pylint:disable=C0103
                        # Invalid class name "R"
                        # Intentionally short name here.
                        # Type does not matter. Only content matters.
R = namedtuple('R', ['input', 'output', 'comment'])
                        # pylint:enable=C0103

                        # pylint:disable=C0301
                        # Line too long
                        # Keep tabular code tabular.
TABLE = [

#              |                |             |               |                 |        | pop? Populating
#   Exist in   | P4IMPLY->GPARN |             | GPARN->GDEST  | GPARFPN->GPARN  |        | first changelist
#   P4 depot   | git-action     | Exist in    | git-action    | git-action      |        | on new depot branch?
#   branch     |                | P4 depot    |               | .               | lw? is | .
#   P4IMPLY,   | Stored in      | branch      | Stored in     | Stored in       | GDEST  | .     rearrange
#   also       | GHOST.disc     | GPARN or    | GDEST.disc    | GPARFPN.disc    | light  | .     p4 action?
#   GDEST.p4   | .git-action    | GPARNFP     | .git-action   | .git-action     | weight | .
#   .          | .              | .           | .             | .               | .      | .     branch from
#   A2         | A2->M3         | M3          | M3->B4        | .               | .      | .     GPARN?
#   .          | .              | .           | .             | .               | .      | .     .

# Rearrange: branch for impending GPARN->GDEST action.
# Don't branch for NOP if lightweight.
#
# Re-branch for NOP if was deleted in LW branch, otherwise we'll erroneously
# look like that file really _should_ be deleted
#
  R(P4IMPLY___ | A|M|T          | P4GPARN_E   | _G(  M|T|D  ) | _GFP(        X) | LW_X   | P_X , BRANCH, "branch-for-MTD")
, R(P4IMPLY_DL | A|M|T          | P4GPARN_E   | _G(        N) | _GFP(        X) | LW     | P_X , BRANCH, "branch-for-NOP re-add to undo previous D")

, R(P4IMPLY___ | A|M|T          | P4GPARN_EP  | _G(        N) | _GFP(        X) | LW     | POP , BRANCH, "branch to populate LW from LW GPARN")
, R(P4IMPLY___ | A|M|T          | P4GPARN_EFP | _G(        N) | _GFP(        X) | LW     | POP , NOP   , "branch to populate LW from GPARFPN skipped")
, R(P4IMPLY___ | A|M|T          | P4GPARN_E   | _G(        N) | _GFP(A|M|T    ) | LW     | P__ , BRANCH, "branch-for-NOP still needs update LW")
, R(P4IMPLY___ | A|M|T          | P4GPARN_E   | _G(        N) | _GFP(        N) | LW     | P__ , NOP   , "branch-for-NOP into LW skipped")
, R(P4IMPLY___ | A|M|T          | P4GPARN_E   | _G(        N) | _GFP(        X) | FP     | P_X , BRANCH, "branch-for-NOP into FP")

# Rearrange: edit existing file content/type to match M3. No need to branch:
# already exists in destination depot branch. Edit for delete is kind of
# pointless, but  might as well, just for completeness.
#
, R(P4IMPLY__E |   M|T          | P4GPARN_E   | _G(X        ) | _GFP(        X) | LW_X   | P_X , EDIT  , "edit to match GPARN")

# NOP. destination depot branch P4IMPLY/A2 has no file, GPARN/M3 has no file,
# everybody has no file.
#
# P4IMPLY->GPARN git-actions A|M|T moved to "impossible" section far below.
#
, R(P4IMPLY___ |       D|N      | P4GPARN__   | _G(A|      N) | _GFP(A|M|T  |N) | LW_X   | P_X , NOP   , "NOP if both GPARN and P4IMPLY have no file")
, R(P4IMPLY_NE |       D|N      | P4GPARN__   | _G(A|      N) | _GFP(      D  ) | LW_X   | P_X , ADD_DELETE, "ADD+DELETE to update LW with deleted since FP basis")
, R(P4IMPLY_DL |       D|N      | P4GPARN__   | _G(A|      N) | _GFP(      D  ) | LW_X   | P_X , NOP   , "NOP update lw already deleted")

# Rearrange: delete existing P4IMPLY/A2 file to match GPARN/M3.
#
, R(P4IMPLY__E |       D        | P4GPARN__   | _G(A|      N) | _GFP(        X) | LW_X   | P_X , DELETE, "D to match GPARN's no file")

# P4IMPLY/A2 lacks a file that Git thinks is there. Lightweight branching never
# bothered to branch it into P4IMPLY/A2's depot branch. Time to JIT-branch it
# before we add/edit/delete it. Don't bother JIT-branching before we NOP it: it
# can remain inherited from JIT basis.
#
, R(P4IMPLY___ |         N      | P4GPARN_E   | _G(  M|T|D  ) | _GFP(        X) | LW     | P_X , BRANCH, "branch-for-MTD a1=N")
, R(P4IMPLY___ |         N      | P4GPARN_E   | _G(        N) | _GFP(A|M|T    ) | LW     | P_X , BRANCH, "branch-for-NOP still needs update LW a1=N")
, R(P4IMPLY___ |         N      | P4GPARN_E   | _G(        N) | _GFP(        N) | LW     | P_X , NOP   , "branch-for-NOP into LW skipped a1=N")

# P4IMPLY/A2 already holds an existing file that matches what we need for
# GPARN/M3. Nothing to do.
#
, R(P4IMPLY__E |         N      | P4GPARN_E   | _G(X        ) | _GFP(        X) | LW_X   | P_X , NOP   , "NOP if GDEST matches GDEST")

# Impossible cases
#
# After editing a file, that file must exist. After deleting, it must not.
# These are various permutations that can never happen. Listed here anyway
# just in case we've got a bug somewhere.

# Git action AMT produces a file which must exist in both both Git and Perforce.
#
, R(P4IMPLY_XX | A|M|T          | P4GPARN__   | _G(X        ) | _GFP(        X) | LW_X   | P_X , IMPOSSIBLE, "AMT produces GPARN")

# Cannot re-add a file that already exists.
#
, R(P4IMPLY_XX | X              | P4GPARN_E   | _G(A        ) | _GFP(        X) | LW_X   | P_X , IMPOSSIBLE, "GPARN E cannot be re-Added")
, R(P4IMPLY__E | A              | P4GPARN_X   | _G(X        ) | _GFP(        X) | LW_X   | P_X , IMPOSSIBLE, "P4IMPLY E cannot be re-added")

# Git action D deletes the file, must no longer exist in Git or Perforce.
#
, R(P4IMPLY_XX |       D        | P4GPARN_E   | _G(X        ) | _GFP(        X) | LW_X   | P_X , IMPOSSIBLE, "Deleted file cannot GPARN E")
, R(P4IMPLY_XX | X              | P4GPARN_E   | _G(X        ) | _GFP(      D  ) | LW     | P_X , IMPOSSIBLE, "Deleted GPARFPN->GPARN cannot P4GPARN E")

# Cannot M|T|D a file that is not there.
#
, R(P4IMPLY_XX | X              | P4GPARN__   | _G(  M|T|D  ) | _GFP(        X) | LW_X   | P_X , IMPOSSIBLE, "GPARN not E cannot be MTDed")

# Fully populated P4IMPLY/A2 should have an existing file if git-diff-tree
# thinks P4IMPLY/A2 matches GPARN/M3
#
, R(P4IMPLY___ |         N      | P4GPARN_E   | _G(X        ) | _GFP(        X) | FP     | P_X , IMPOSSIBLE, "FP P4IMPLY not E cannot be Ned to GPARN E")

# If git-diff-tree thinks P4IMPLY/A2 and GPARN/M3 match, and P4IMPLY has a file,
# then GPARN (or its GPARNFP basis) must also have that same file.
#
, R(P4IMPLY__E |         N      | P4GPARN__   | _G(X        ) | _GFP(        X) | LW_X   | P_X , IMPOSSIBLE, "FP P4IMPLY E cannot be Ned to GPARN not E")

]
                        # pylint:enable=C0301

                        # pylint:disable=R0913
                        # Too many arguments
                        # I disagree.
def to_input( ghost_cell
            , gdest_cell
            , gparn_cell
            , gparfpn_cell
            , gdest_column
            , gparn_column ):
    '''
    Return an integer that encodes a single file's input to the
    above decision matrix.
    '''
    row_input = ( _p4imply_p4exists(gdest_cell)
                | _p4imply_to_gparn_git_action(ghost_cell)
                | _gparn_p4exist(gparn_cell, gparfpn_cell)
                | _gparn_to_gdest_git_action(gdest_cell, ghost_cell)
                | _gparfpn_to_gparn_git_action(gparfpn_cell)
                | _lw(gdest_column.branch.is_lightweight)
                | _pop(gdest_column, gparn_column)
                )
    return row_input
                        # pylint:enable=R0913


def find_row(row_input):
    '''
    Search the above table to match the above input.
    '''
    for row in TABLE:
        if (row.input & row_input) == row_input:
            if LOG.isEnabledFor(logging.DEBUG3):
                LOG.debug3('integ matrix input = {} output={}'
                           .format(deb(row_input), row))
            return row

    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3('integ matrix input = {} no match'.format(deb(row_input)))
    return None


def _pop(gdest_column, gparn_column):
    '''
    Are we populating a new depot branch?
    '''
    if _is_pop_new_branch(gdest_column, gparn_column):
        return POP
    else:
        return P__


def _is_pop_new_branch(gdest_column, gparn_column):
    '''
    Are we populating a new depot branch?
    '''
                        # Not if there are already changelists in the depot
                        # branch.
    if gdest_column.change_num:
        return False

                        # Not if creating a lightweight GDEST child of a fully
                        # populated GPARN parent. Never fully populate a
                        # lightweight branch.
    if (        gdest_column.branch.is_lightweight
        and not gparn_column.branch.is_lightweight):
        return False

                        # Yes. We're either populating a new fully populated
                        # GDEST, or we're copying the files from a lightweight
                        # GPARN that contains only files that differ from
                        # lightweight GPARN's fully populated GPARFPN basis.
    return True


                        # Return codes for _exists_how()
_E_EXISTS_AT_REV   = 'exists '  # Not deleted at revision
_E_DELETED_AT_REV  = 'deleted'
_E_NEVER           = 'never  '  # Never added/branched, nothing to delete.

def _exists_how(cell):
    '''
    Does this file exist in this column?
    Existed but deleted at column's revision?
    Never existed?

    Returns an _E_xxx string, NOT A BOOLEAN.
    '''
    if not (     cell
            and  cell.discovered
            and ('depotFile' in cell.discovered)):
        return _E_NEVER

                        # Problem:
                        #     'p4 copy -n' from a lightweight branch will store
                        # 'delete' actions for any file not yet branched into
                        # that lightweight branch. 'p4 files' won't clobber
                        # this 'delete' action because that file doesn't EXIST
                        # in the lightweight branch: it's "inherited" from its
                        # GPARFPN fully populated basis.
                        #
                        # Solution:
                        #     Examine the dict more deeply than just
                        # "('delete' in action)". If the file really _was_
                        # deleted, there'd be a 'change' value telling which
                        # changelist holds the 'p4 delete' file action.
                        #
    if (    ('delete' in cell.discovered.get('action'))
        and ('change' in cell.discovered)):
        return _E_DELETED_AT_REV

    return _E_EXISTS_AT_REV


def _p4imply_p4exists(gdest_cell):
    '''
    Does the file already exist, undeleted at the column's revision, in the Perforce
    depot branch that we're reusing to house the impending Git commit?
    '''
    e = _exists_how(gdest_cell)
    return { _E_EXISTS_AT_REV  : P4IMPLY__E
           , _E_DELETED_AT_REV : P4IMPLY_DL
           , _E_NEVER          : P4IMPLY_NE }[e]


_GIT_ACTION_TO_ENUM = {
      'A'  : A
    , 'M'  : M
    , 'T'  : T
    , 'D'  : D
    , None : N
}


def _p4imply_to_gparn_git_action(ghost_cell):
    '''
    What git-diff-tree action converts this file's current P4IMPLY state to
    what Git expects for GPARN state?
    '''
    git_action = Cell.safe_discovered(ghost_cell, 'git-action')
    return _GIT_ACTION_TO_ENUM.get(git_action)


def _gparn_p4exist(gparn_cell, gparfpn_cell):
    '''
    Does this file exist either in GPARN, or inherited from its GPARFPN?
    '''
    e = _exists_how(gparn_cell)

                        # Exists or deleted from GPARN itself.
                        # Do not inherit.
    if e != _E_NEVER:
        return { _E_EXISTS_AT_REV  : P4GPARN_EP
               , _E_DELETED_AT_REV : P4GPARN__ }[e]

                        # Inherit existence from GPARFPN.
    e = _exists_how(gparfpn_cell)
    return { _E_EXISTS_AT_REV  : P4GPARN_EFP
           , _E_DELETED_AT_REV : P4GPARN__
           , _E_NEVER          : P4GPARN__ }[e]


def _gparn_to_gdest_git_action(gdest_cell, ghost_cell):
    '''
    Convert git-fast-export's git-action to an enum.

    A warning about "git-action" from git-fast-export. git-fast-export never
    reports 'A' for add. It reports 'M' for either "add" or "modify". We have to
    check for file existence in Git first-parent to differentiate. I'm not
    adding Yet Another Column here to handle that case. Easier to sanitize that
    input before we get to this table.
    '''
    enum = _GIT_ACTION_TO_ENUM.get(Cell.safe_discovered(gdest_cell, 'git-action'))
    if (enum == M) and (not Cell.safe_discovered(ghost_cell, 'sha1')):
        enum = A
    return _G(enum)


def _gparfpn_to_gparn_git_action(gparfpn_cell):
    '''
    Convert git-diff-tree GPARFPN GPARN action to an enum.
    '''
    enum = _GIT_ACTION_TO_ENUM.get(Cell.safe_discovered( gparfpn_cell
                                                       , 'git-action'))
    return _GFP(enum)


def _lw(is_lightweight):
    '''
    Lightweight depot branches prohibit some branch-for-no-reason actions that
    fully populated depot branches permit.
    '''
    if is_lightweight:
        return LW
    else:
        return FP


def deb(row_input):
    '''
    Debugging converter for input int.
    '''
    if not isinstance(row_input, int):
        return str(row_input)
    return ' '.join([ _deb_p4imply_p4exists(row_input)
                    , _deb_p4imply_to_gparn_git_action(row_input)
                    , _deb_gparn_p4exist(row_input)
                    , _deb_gparn_to_gdest_git_action(row_input)
                    , _deb_gparfpn_to_gparn_git_action(row_input)
                    , _deb_lw(row_input)
                    , _deb_pop(row_input)
                    #, '{:0b}'.format(row_input)
                    ])


def _masked_bin(x, mask):
    '''0b000111000 ==> "111" '''
    mask_str = NTR('{:b}').format(mask)
    bit_str  = NTR('{:b}').format(mask & x)
    first_1_index = mask_str.find('1')
    last_1_index  = mask_str.find('1')
    return bit_str[first_1_index:last_1_index]


def _deb_p4imply_p4exists(x):
    '''debug dump'''
    mask = P4IMPLY_XX
    return { P4IMPLY__E : 'P4IMPLY__E'
           , P4IMPLY_NE : 'P4IMPLY_NE'
           , P4IMPLY_DL : 'P4IMPLY_DL'
           , P4IMPLY___ : 'P4IMPLY___'
           , P4IMPLY_XX : 'P4IMPLY_XX' }.get(x & mask, _masked_bin(x, mask))

def _deb_gitact(x):
    '''Debugging converter from int to P4S string.'''
    bits = []
    bits.append(NTR('A') if A & x & ~GHOST_BIT else '.')
    bits.append(NTR('M') if M & x & ~GHOST_BIT else '.')
    bits.append(NTR('D') if D & x & ~GHOST_BIT else '.')
    bits.append(NTR('T') if T & x & ~GHOST_BIT else '.')
    bits.append(NTR('N') if N & x & ~GHOST_BIT else '.')
    return ''.join(bits)


def _deb_p4imply_to_gparn_git_action(x):
    '''debug'''
    return _deb_gitact(x)


def _deb_gparn_p4exist(x):
    '''debug dump'''
    mask = P4GPARN_X
    return { P4GPARN_E   : 'P4GPARN_E'
           , P4GPARN_EP  : 'P4GPARN_EP'
           , P4GPARN_EFP : 'P4GPARN_EFP'
           , P4GPARN__   : 'P4GPARN__'
           , P4GPARN_X   : 'P4GPARN_X' }.get(x & mask, _masked_bin(x, mask))


def _deb_gparn_to_gdest_git_action(x):
    '''debug dump'''
    return _deb_gitact(_UN_G(x))


def _deb_gparfpn_to_gparn_git_action(x):
    '''debug dump'''
    return _deb_gitact(_UN_GFP(x))


def _deb_lw(x):
    '''debug dump'''
    mask = LW_X
    return { LW   : 'LW'
           , FP   : 'FP'
           , LW_X : 'LW_X' }.get(x & mask, _masked_bin(x, mask))

def _deb_pop(x):
    '''debug dump'''
    mask = P_X
    return { POP : 'POP'
           , P__ : 'P__'
           , P_X : 'P_X' }.get(x & mask, _masked_bin(x, mask))
