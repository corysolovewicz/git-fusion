#! /usr/bin/env python3.3
'''
Integration action decision matrix
'''

from   collections  import namedtuple
import logging

from   p4gf_l10n    import NTR

                        # pylint:disable=C0301
                        # line too long? Keep tabular code tabular.

# Integration decision matrix
#
# Inputs
# --
# We previously decided to integrate some source revision to a destination.

# The file may or may not yet exist in Perforce at the destination. It may or
# may not be deleted at the source revision or at head in the destination.
# The file may or may not exist in the destination Git commit.
#
# Outputs
# --
# Exactly how shall we integrate this file?
# How shall we resolve it?
# Any post-integ actions? Fallbacks if integ fails.

LOG = logging.getLogger(__name__)

# -- Inputs ---------------------------------------------------------------

                                    # Input is integer bit fields, not string
                                    # or enum constants, so that we can express
                                    # "don't care" as the bitwise bits of what we
                                    # don't care about, and so that we can scan for
                                    # hits with bitwise tests.

                                    # Integ source revision
                                    # GPARN or GPARFPN or P4JITFP
                                    #
P4S_E__ = 0b000000000000000000001   # exists in Perforce, not deleted at src rev
P4S_E_D = 0b000000000000000000010   # exists in Perforce,     deleted at src rev
P4S_DK  = 0b000000000000000000011   # exists in Perforce,
                                    #   don't care if src rev deleted or not

                                    # Integ destination revision  GDEST
                                    #
P4D_E__ = 0b000000000000000000100   #     exists in Perforce, not deleted at dest rev
P4D_E_D = 0b000000000000000001000   #     exists in Perforce,     deleted at dest rev
P4D____ = 0b000000000000000010000   # not exists at all in Perforce
P4D_DK  = 0b000000000000000011100   # don't care if exists in Perforce
                                    #   or deleted at dest rev

                                    # git-ls-tree at GDEST commit
                                    # (what we actually want to happen in Perforce)
                                    #
GD_E    = 0b000000000000000100000   #     exists in git-ls-tree
GD__    = 0b000000000000001000000   # not exists in git-ls-tree
GD_DK   = 0b000000000000001100000   # don't care if exists or not in GDEST's git-ls-tree

                                    # Git action from GDEST's git-fast-export
                                    # or P4IMPLY's git-diff-tree
                                    #
A       = 0b000000000000010000000   # A   create a new file
M       = 0b000000000000100000000   # M   edit an existing file
D       = 0b000000000001000000000   # D   delete an existing file
T       = 0b000000000010000000000   # T   change mode (filetype) of existing file
N       = 0b000000000100000000000   #     no action from Git
DK      = 0b000000000111110000000   # don't care what git action requested
                                    #
                                    # C copy and R rename will not appear because
                                    # we ask not for copy/rename detection.
                                    # U unmerged will not appear because we do
                                    # not git-merge not.
                                    # X unknown will not appear because that would
                                    # be a bug in Git.
                                    #
                                    # Git action does not actually matter in this
                                    # table: we either integ or we don't, based
                                    # solely on file existence. But I'm keeping it
                                    # in the table in case we later discover that
                                    # Git action matters.

                                    # Does GDEST's content sha1 (C) and file mode (T)
                                    # match that of the integ source (S)?
                                    # Destination? (D)
                                    #
ST_EQ   = 0b000000001000000000000   # integ source type (git file mode)
ST_NE   = 0b000000010000000000000
ST_DK   = 0b000000011000000000000

SC_EQ   = 0b000000100000000000000   # integ source file content (git blob sha1)
SC_NE   = 0b000001000000000000000
SC_DK   = 0b000001100000000000000

DT_EQ   = 0b000010000000000000000   # integ dest type (git file mode)
DT_NE   = 0b000100000000000000000
DT_DK   = 0b000110000000000000000

DC_EQ   = 0b001000000000000000000   # integ dest file content (git blob sha1)
DC_NE   = 0b010000000000000000000
DC_DK   = 0b011000000000000000000

SDTC_DK = 0b011111111000000000000   # shorter constant for "don't care at all
                                    # about type/content match unless we're
                                    # merging".


# -- Outputs --------------------------------------------------------------

                            # 'p4 integ' flags
                            #
                            # Omits -i (baseless merge) and -t (propagate
                            # filetype) flags that outer code
                            # unconditionally supplies.
                            #
INTEG     = ''              # integrate content only,
                            # no branch/deleteactions.
                            #
INTEG_T   = '-t'            # integrate content and filetype,
                            # no branch/delete actions.
                            #
RBD     = '-Rbd -t'         # integrate branch/delete actions only, no
                            # content. Usually only -Rb or -Rd required, I'm
                            # squashing 'em down to a single -Rbd out of
                            # laziness and to reduce output range.
                            #
# None                      # Do not run 'p4 integrate' or 'p4 resolve'.

                            # 'p4 resolve' flags
                            #
SRC_AT  = '-at'             # Keep integ source, completely replace integ dest.
                            #
DEST_AY = '-ay'             # Ignore integ source, keep integ dest unchanged.
                            #
CONTENT = NTR('-af -t')     # Merge content, -t as text even if binary.
                            # Leaves conflict markers in file. Later code
                            # will likely need to 'p4 edit' and overwrite
                            # with content from Git.
                            #
# None                      # Do not run 'p4 integrate' or 'p4 resolve'.

                            # fallback: add/edit/delete
                            #
ADD     = 'add'             # Integ actions with a fallback are permitted to
EDIT    = 'edit'            # fail, that's okay. But the fallback had better
DELETE  = 'delete'          # work or we've got a failure. Integ actions
                            # without a fallback must not fail.
                            #
FAIL_OK = ''                # No fallback action, but if integ fails to open
                            # file for integ, that's okay. If resolve fails
                            # to leave file resolved, revert and keep going
                            # (although if there are previous integs from
                            # previous branches, that could be a problem,
                            # probably need to restore those somehow which
                            # bogs us down in single-file actions to
                            # restore. Blech.)
                            #
# None                      # No fallback, integ must open file for integ
                            # and resolve must leave resolved, else raise a
                            # fatal exception.

                            # pylint:disable=C0103
                            # Invalid class name "R"
                            # Intentionally short name here.
                            # Type does not matter. Only content matters.
R = namedtuple('R', ['input', 'integ_flags', 'resolve_flags', 'fallback'])
                            # pylint:enable=C0103

TABLE = [

#            |          | git    |           | integ source/dest            ==>
#    integ   | integ    | dest   | git       | type/content match           ==>
#    src rev | dest rev | exists | action    | git dest                     ==> integ | resolve | fallback
#

# Exists everywhere? It's a normal integ, regardless of Git action.
# May reopen later to change file type or swap in different content.
#
# CONTENT -af -t unsafe when result might be filetype symlink, so we're not
# going to use it anymore. At all. We're not exploding this decision matrix to
# account for "result might be a symlink". Always fully accept theirs -at or
# yours -ay, then do a 'p4 reopen' on top of the result to clobber with whatever
# Git has.
#
# Only if source has the winning filetype, propagate it.
#
  R( P4S_E__ | P4D_E__  | GD_E   | A|M|T     | ST_EQ | SC_DK | DT_NE | DC_DK , INTEG_T, SRC_AT,  EDIT    )
#
# If source matches more than dest, accept it. But don't propagate non-winning filetype
#
, R( P4S_E__ | P4D_E__  | GD_E   | A|M|T     | ST_EQ | SC_EQ | DT_EQ | DC_NE , INTEG,   SRC_AT,  EDIT    )
, R( P4S_E__ | P4D_E__  | GD_E   | A|M|T     | ST_NE | SC_EQ | DT_NE | DC_NE , INTEG,   SRC_AT,  EDIT    )
#
# When in doubt, favor the dest unless source has a reason not to.
#
, R( P4S_E__ | P4D_E__  | GD_E   | A|M|T     | SDTC_DK                       , INTEG,   DEST_AY, EDIT    )

# NOP for NOP
#
, R( P4S_E__ | P4D_E__  | GD_E   |         N | SDTC_DK                       , None,    None,    None    )

# Delete required, integ desired. P4D prohibits integ + delete with
# "can't delete (already opened for integrate)". So delete without integ.
#
, R( P4S_E__ | P4D_E__  | GD__   |       D|N | SDTC_DK                       , None,    None,    DELETE  )

# File once existed in Perforce destination location, but now deleted.
# Require destination exist. Re-branch, or failing that, add.
# May reopen later to change file type or swap in different content.
#
, R( P4S_E__ | P4D_E_D  | GD_E   | A|M|T  |N | SDTC_DK                       , RBD,     SRC_AT,  ADD     )

# Integration source has a file that Git does not want, that already does
# not exist in integration destination. This would be a branch (or integ)
# for re-delete, -Rbd + -ay. No thank you.
#
, R( P4S_E__ | P4D_E_D  | GD__   |       D|N | SDTC_DK                       , None,    None,    None )

# File never existed in Perforce detination. Exists in integration source,
# and Git wants a file there. Integrate for branch.
# May reopen later to change file type or swap in different content.
#
, R( P4S_E__ | P4D____  | GD_E   | A|M|T  |N | SDTC_DK                       , RBD,     SRC_AT,  ADD     )

# File never existed in Perforce destination, nor does Git want a file
# there. But we've got an integration source. Okay to try integ for ignored
# "branch" action here, but also okay if this fails.
#
, R( P4S_E__ | P4D____  | GD__   |       D|N | SDTC_DK                       , RBD,     DEST_AY, FAIL_OK )

# Integration source existed, is now deleted. Integration destination
# exists, and Git wants a file there. Okay to try integ for ignored "delete"
# action here, but also okay if this fails.
#
, R( P4S_E_D | P4D_E__  | GD_E   | A|M|T     | SDTC_DK                       , RBD,     DEST_AY, FAIL_OK )
, R( P4S_E_D | P4D_E__  | GD_E   |         N | SDTC_DK                       , None,    None,    None    )

# Integration source existed but is deleted at head, and Git does not want a
# file there. But integration destination has a file. Try integ-for-delete,
# and if that doesn't actually delete the file, then just p4 delete.
#
, R( P4S_E_D | P4D_E__  | GD__   |       D|N | SDTC_DK                       , RBD,     SRC_AT,  DELETE  )

# File exists but deleted at head in both integration source and
# destination, yet Git requires that there be a file there. Perforce cannot
# integrate for delete and also add/edit at the same time. So just re-add.
#
, R( P4S_E_D | P4D_E_D  | GD_E   | A|M|T  |N | SDTC_DK                       , None,    None,    ADD     )

# File exists but deleted at head in both integration source and
# destination, so Perforce won't let you integ. That's great, because
# Git does not want a file there. NOP.
#
, R( P4S_E_D | P4D_E_D  | GD__   |       D|N | SDTC_DK                       , None,    None,    None    )

# File exists but deleted at head in integration source and doesn't even
# exist in destination, so Perforce won't let you integ. But Git needs a
# file there, and no integ will produce one. Add.
#
, R( P4S_E_D | P4D____  | GD_E   | A|M|T  |N | SDTC_DK                       , None,    None,    ADD     )

# File exists but deleted at head in integration source and doesn't even
# exist in destination, so Perforce won't let you integ. That's great,
# because Git doesn't want a file there. NOP
#
, R( P4S_E_D | P4D____  | GD__   |       D|N | SDTC_DK                       , None,    None,    None    )

# -- Impossible cases (or a bug somewhere) --------------------------------

# GD_E    : exists in destination commit's git-ls-tree
# + D     : git-fast-export or git-diff-tree says that
#           the existing file file is deleted.
#
, R( P4S_DK  | P4D_DK   | GD_E   |       D   | SDTC_DK                       , None,    None,    None    )

# GD__    : does not exist in destination commit's git-ls-tree
# + A|M|T : git-fast-export or git-diff-tree says that
#           the non-existent file is added or modified.
#
, R( P4S_DK  | P4D_DK   | GD__   | A|M|T     | SDTC_DK                       , None,    None,    None    )

]

_GIT_ACTION_TO_INPUT = { 'A' : A
                       , 'M' : M
                       , 'T' : T
                       , 'D' : D
                       }

#def to_input(row, integ_src_cell, git_delta_cell, gdest_cell):
def to_input(row, integ_src_cell, integ_dest_cell, git_delta_cell, gdest_cell):
    '''
    Return the appropriate input to use when searching the above table.
    '''
    assert integ_src_cell              # Required: must actually have a
    assert integ_src_cell.discovered   # source from which to integrate.

    row_input = ( _p4s(integ_src_cell)
                | _p4d(gdest_cell)
                | _gd(row)
                | _git_action(git_delta_cell)
                | _sdtc(gdest_cell, integ_src_cell, integ_dest_cell)
                )
    return row_input

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


def _p4s(integ_src_cell):
    '''Does the source already exist in Perforce? Deleted at head?'''
    if 'delete' in integ_src_cell.discovered['action']:
        return P4S_E_D

    # Is the integration source unavailable as an integration source?
    # Peforce revision string "none" means nope.
    if integ_src_cell.discovered.get('rev') == 'none':
        return P4S_E_D  ### Should this be P4S____, not P4S_E_D?
                        ### Does that matter? Affect decision?
    return P4S_E__


def _p4d(gdest_cell):
    '''Does the destination already exist in Perforce? Deleted at head?'''
    if (    gdest_cell
            and gdest_cell.discovered
            and 'depotFile' in gdest_cell.discovered ):
        if 'delete' in gdest_cell.discovered['action']:
            return P4D_E_D
        else:
            return P4D_E__
    else:
        return P4D____


def _gd(row):
    '''Does the file exist in the Git destination commit?'''

    if row.sha1 and row.mode:
        return GD_E
    else:
        return GD__


def _masked_bin(x, mask):
    '''0b000111000 ==> "111" '''
    mask_str = NTR('{:b}').format(mask)
    bit_str  = NTR('{:b}').format(mask & x)
    first_1_index = mask_str.find('1')
    last_1_index  = mask_str.find('1')
    return bit_str[first_1_index:last_1_index]


def _git_action(git_delta_cell):
    '''What action does git-fast-export or git-diff-tree suggest?'''
    if (    git_delta_cell
            and git_delta_cell.discovered
            and 'git-action' in git_delta_cell.discovered ):
        ga = _GIT_ACTION_TO_INPUT.get(git_delta_cell.discovered['git-action'])
        assert ga != None
        return ga
    else:
        return N


def _git_sha1_and_mode(cell):
    '''
    Return a (sha1, file mode) tuple of cell.discovered's Git sha1 and file
    mode if the exist, or (None, None) if not.
    '''
    if not (cell and cell.discovered):
        return (None, None)
    return (cell.discovered.get('sha1'), cell.discovered.get('git-mode'))


def _sdtc(gdest_cell, integ_src_cell, integ_dest_cell):
    '''
    Does the integ source's git file mode and content sha1
    match the gdest's? Does the integ dest's match?

    Return all 4 bits of integ
    {Source, Dest} match Git DEST {file mode (Type), Content sha1}
    '''
    (gd_sha1, gd_mode) = _git_sha1_and_mode(gdest_cell)
    (s_sha1,  s_mode)  = _git_sha1_and_mode(integ_src_cell)
    (d_sha1,  d_mode)  = _git_sha1_and_mode(integ_dest_cell)

    r = 0
    r |= (ST_EQ if gd_mode == s_mode else ST_NE)
    r |= (SC_EQ if gd_sha1 == s_sha1 else SC_NE)
    r |= (DT_EQ if gd_mode == d_mode else DT_NE)
    r |= (DC_EQ if gd_sha1 == d_sha1 else DC_NE)
    return r


def _deb_p4s(x):
    '''Debugging converter from int to P4S string.'''
    mask = P4S_DK
    return { P4S_E__ : 'P4S_E__'
           , P4S_E_D : 'P4S_E_D'
           , P4S_DK  : 'P4S_DK ' }.get(x & mask, _masked_bin(x, mask))


def _deb_p4d(x):
    '''Debugging converter from int to P4S string.'''
    mask = P4D_DK
    return { P4D_E__ : 'P4D_E__'
           , P4D_E_D : 'P4D_E_D'
           , P4D____ : 'P4D____'
           , P4D_DK  : 'P4D_DK '
           }.get(x & mask, _masked_bin(x, mask))


def _deb_gd(x):
    '''Debugging converter from int to P4S string.'''
    mask = GD_DK
    return { GD_E  : 'GD_E '
           , GD__  : 'GD__ '
           , GD_DK : 'GD_DK'
           }.get(x & mask, _masked_bin(x, mask))


def _deb_st(x):
    '''Debugging converter from int to ST string.'''
    mask = ST_DK
    return { ST_EQ : 'ST_EQ'
           , ST_NE : 'ST_NE'
           , ST_DK : 'ST_DK'
           }.get(x & mask, _masked_bin(x, mask))


def _deb_sc(x):
    '''Debugging converter from int to SC string.'''
    mask = SC_DK
    return { SC_EQ : 'SC_EQ'
           , SC_NE : 'SC_NE'
           , SC_DK : 'SC_DK'
           }.get(x & mask, _masked_bin(x, mask))


def _deb_dt(x):
    '''Debugging converter from int to ST string.'''
    mask = DT_DK
    return { DT_EQ : 'DT_EQ'
           , DT_NE : 'DT_NE'
           , DT_DK : 'DT_DK'
           }.get(x & mask, _masked_bin(x, mask))


def _deb_dc(x):
    '''Debugging converter from int to ST string.'''
    mask = DC_DK
    return { DC_EQ : 'DC_EQ'
           , DC_NE : 'DC_NE'
           , DC_DK : 'DC_DK'
           }.get(x & mask, _masked_bin(x, mask))


def _deb_gitact(x):
    '''Debugging converter from int to P4S string.'''
    bits = []
    bits.append('A' if A & x else '.')
    bits.append('M' if M & x else '.')
    bits.append('D' if D & x else '.')
    bits.append('T' if T & x else '.')
    bits.append('N' if N & x else '.')
    return ''.join(bits)


def deb(x):
    '''Debugging converter for input int.'''
    if not isinstance(x, int):
        return str(x)
    return ' '.join([ _deb_p4s   (x)
                    , _deb_p4d   (x)
                    , _deb_gd    (x)
                    , _deb_gitact(x)
                    ])


# Not listed in table, but implied anywhere GD_E:
#
#  'p4 edit' after integ for branch or merge:
#
#     'p4 integ' produces a branched or merged file, with a copy of that
#     file in the local filesystem. Its content or filetype might not quite
#     match what Git expects for this commit.
#     If content mismatch, 'p4 edit'.
#     If filetype mismatch, 'p4 reopen -k'.
#
#     +++ You do not need to calculate a sha1 for the entire file or compare
#     +++ the entire file: you just care if it differs, which can go much
#     +++ faster with an early exit on first mismatch.

# Duplicate integ actions
#
#     Perforce prohibits integrating a single target file for delete from
#     multiple integration sources. It also prohibits some duplicate branch
#     actions, at least that is what the error message says even though Zig
#     has yet to trigger that error.

# Filetype Actions
#
#     We unconditionally treat any change to file content or file type as
#     either a 'p4 edit -k <dest_type>' or a 'p4 reopen -k <dest_type>'.
#     The input table mashes together 'M' and 'T' Git actions.





# 'p4 help integ' flags through the ages.
#
# All these flags are supported in all versions 11.1-13.2. But deprecated flags
# become undoc in later server versions, and new flags start out as undoc in
# earlier server versions.
#
# Most recent 'p4 help' description listed here so that Zig does not need to
# fire up an older server version just to run 'p4 help integ'.
#
#       appears
#       in help
#       11 12 13
# flag  1  12 12  help
# ----  --------  -------------------------------------------------------------
# -t    1         The -t flag propagates source filetypes instead of scheduling
#                 filetype conflicts to be resolved.
#
# -i    1         The -i flag enables merging between files that have no prior
#                 integration history.  By default, 'p4 integrate' requires a
#                 prior integration in order to identify a base for merging.
#                 The -i flag allows the integration, and schedules the target
#                 file to be resolved using the first source revision as the
#                 merge base.
#
# -b    1  12 13  The -b flag makes 'p4 integrate' use a user- defined branch
#                 view. (See 'p4 help branch'.) The source is the left side of
#                 the branch view and the target is the right side. With -r, the
#                 direction is reversed.
#
# -f    1  12 13  The -f flag forces integrate to ignore integration history and
#                 treat all source revisions as unintegrated. It is meant to be
#                 used with revRange to force reintegration of specific,
#                 previously integrated revisions.
#
# -d    1         The -d flag is a shorthand for all -D flags used together.
#
# -D    1  12 1   The -D flags modify the way deleted files are treated:
#
# -Dt   1  12 1   If the target file has been deleted and the source file has
#                 changed, re-branch the source file on top of the target file
#                 instead of scheduling a resolve.
#
# -Ds   1  12 1   If the source file has been deleted and the target file has
#                 changed, delete the target file instead of scheduling a
#                 resolve.
#
# -Di   1  12 13  The -Di flag modifies the way deleted revisions are treated.
#                 If the source file has been deleted and re-added, revisions
#                 that precede the deletion will be considered to be part of the
#                 same source file. By default, re-added files are considered to
#                 be unrelated to the files of the same name that preceded them.
#
# -R           3  The -R flags modify the way resolves are scheduled:
#
# -Rb          3  Schedules 'branch resolves' instead of branching new target
#                 files automatically.
#
# -Rd          3  Schedules 'delete resolves' instead of deleting target files
#                 automatically.
#
# -Rs          3  Skips cherry-picked revisions already integrated. This can
#                 improve merge results, but can also cause multiple resolves
#                 per file to be scheduled.



