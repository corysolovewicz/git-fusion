#! /usr/bin/env python3.3
'''
Code for dumping the big matrix.

Internally builds a grid of "boxes", each "box" is a list of text lines
that go in that box. Then merge each horizontal row of boxes into a list
of (long!) text lines.
'''
import re

import p4gf_branch
import p4gf_const
import p4gf_depot_branch
import p4gf_g2p_matrix2_integ
import p4gf_g2p_matrix2_ghost
import p4gf_util

# Column widths to keep things sane.
_WIDE             = False
_VAL_MAX_WIDTH_L  = [30,  70]
_CELL_MAX_WIDTH_L = [80, 100]

_VAL_MAX_WIDTH  = _VAL_MAX_WIDTH_L [1 if _WIDE else 0]
_CELL_MAX_WIDTH = _CELL_MAX_WIDTH_L[1 if _WIDE else 0]

# pylint:disable=W9903
# non-gettext-ed string
# This entire file is debugging dump, no L10N required.

def dump (matrix
        , one_row = None
        , wide    = False):
    '''
    Return a long list of long strings suitable for debugging dumps.

    This is what Zig keeps drawing on his whiteboard when debugging.
    '''

    global _VAL_MAX_WIDTH
    global _CELL_MAX_WIDTH
    _VAL_MAX_WIDTH  = _VAL_MAX_WIDTH_L [1 if wide else 0]
    _CELL_MAX_WIDTH = _CELL_MAX_WIDTH_L[1 if wide else 0]

    if one_row:
        rows = [one_row]
    else:
        rows = matrix.rows_sorted if matrix.rows_sorted else matrix.rows.values()

    # 1. Build a row of text output boxes, each box that describes one column.

    # Top left box is the header for the "row itself" column.

    box = ['row', 'ct={}'.format(len(rows))]
    col_boxes = [ box ]

    # One box per column
    col_boxes.extend([_to_column_box(column)
                      for column in matrix.columns])
    if matrix.git_delta_column:
        col_boxes[1+matrix.git_delta_column.index].append('git_delta')

    # 2. Build one row of boxes for each row in our list
    row_boxes_list = [_to_row_boxes(row)
                      for row in rows]

    # 3. How wide is each column (within reason) ?
    col_width_list = _col_width_list(col_boxes, row_boxes_list)

    # 4. Print column header
    l = _box_list_to_lines(col_width_list, col_boxes)
    l.extend(' ')

    # 5. Print each row
    for row_boxes in row_boxes_list:
        l.extend(_box_list_to_lines(col_width_list, row_boxes))
        l.extend(' ')

    return l


def _append_if(line_list, val, fmt='{}'):
    '''
    Append a string to line_list, only if val has something worth reporting.
    '''
    if not val:
        return
    if hasattr(fmt, 'format'):
        line_list.append(fmt.format(val))
    else:
        line_list.append(str(fmt))

def _to_column_box(column):
    '''
    Return a list of strings that describe one column.
    '''
    r = [ '{i}:{col_type}'.format(i=column.index, col_type=column.col_type)
        , 'v:{}'.format(p4gf_branch.abbrev(column.branch))
        , 'd:{}'.format(p4gf_depot_branch.abbrev(column.depot_branch))
        , '@{}' .format(column.change_num)
        ]
    _append_if(r, column.sha1,            p4gf_util.abbrev(column.sha1))
    if column.fp_counterpart:
        r.append('fp_ctr_part:{}'.format(column.fp_counterpart.index))
    _append_if(r, column.is_first_parent, 'first-parent')
    return r


def _to_row_header(row):
    '''
    Return one row's own data as a list of strings.
    '''
    r = [ row.gwt_path ]
    _append_if(r, row.sha1, p4gf_util.abbrev(row.sha1))
    _append_if(r, row.mode, p4gf_util.mode_str(p4gf_util.octal(row.mode)))
    _append_if(r, row.p4_request)
    _append_if(r, row.p4filetype)
    return r


def _to_cell_decided(decided):
    '''
    Return cell.Decided as a list of strings to fill a text cell.
    '''
    if not decided:
        return ['- Decided    - None']

    r = ['- Decided    -']
    if decided.integ_flags != None:
        r.append('integ {}'.format(decided.integ_flags))
    _append_if(r, decided.resolve_flags, 'resolve {}')
    r.append('fb    : {}'.format(decided.on_integ_failure))
    _append_if(r, decided.integ_fallback,       'fb    : {}')
    _append_if(r, decided.p4_request,           'p4_req: {}')
    if decided.add_delete:
        r.append('add_delete')
    _append_if(r, decided.integ_input != None,  'ir    : {}'
                    .format(_integ_input(decided.integ_input)))
    return r


def _integ_input(integ_input):
    '''
    Convert integ input to a string. Knows how to differentiate between normal
    matrix integ input and ghost integ input.
    '''
    if isinstance(integ_input, int):
        if integ_input & p4gf_g2p_matrix2_ghost.GHOST_BIT:
            return p4gf_g2p_matrix2_ghost.deb(integ_input)
        else:
            return p4gf_g2p_matrix2_integ.deb(integ_input)
    else:
        return integ_input


def _fill(ct, msg):
    '''Pad (or truncate) to exactly fill a width.'''
    msg_ct = len(msg)
    if msg_ct < ct:
        return msg + ' ' * (ct - msg_ct)
    return msg[-ct:]


def _to_cell_discovered(discovered):
    '''
    Return cell.Decided as a list of strings to fill a text cell.
    '''
    if not discovered:
        return ['- Discovered - None']

    r = ['- Discovered -']
    keys    = sorted(discovered.keys())
    key_len = max(len(key) for key in keys)
    for key in keys:
        val = discovered[key]
        if key == 'sha1':
            val = p4gf_util.abbrev(discovered[key])
        elif val.startswith('//'):
            val = abbrev_depot_path(val, _VAL_MAX_WIDTH)
        else:
            val = val[-_VAL_MAX_WIDTH:]
        fmt = '{key}: {val}'
        r.append(fmt.format(key=_fill(key_len, key), val=val))
    return r


def _to_cell(cell):
    '''
    Return one cell's data as a list of strings.
    '''
    if not cell:
        return ['-']

    return _to_cell_decided(cell.decided) \
         + _to_cell_discovered(cell.discovered)


def _to_row_boxes(row):
    '''
    Return a list of lists-of-strings, one list-of-strings for each cell
    in row (including one for the row itself)
    '''
    return  [_to_row_header(row)] \
          + [_to_cell(cell)
             for cell in row.cells]


def _box_width(box):
    '''How long is the longest line in this box?'''
    return max(len(l) for l in box)


def _col_width_list(col_boxes, row_boxes_list):
    '''Return a list of column character widths.'''
    # Start with column headers
    max_width_list = [ _box_width(box) for box in col_boxes]

    # Then roll through each row
    for row_boxes in row_boxes_list:
        m = [ _box_width(box) for box in row_boxes]
        max_width_list = [max(mm, mwl) for mm, mwl in zip(m, max_width_list)]

    # Apply sane limits. Nobody wants a 100-char-wide column.
    max_width_list = [min(_CELL_MAX_WIDTH, mwl) for mwl in max_width_list]

    return max_width_list


def _val(box, i):
    '''
    Return a string for line i of box, even if box doesn't have that many lines.
    '''
    return box[i] if i < len(box) else ''


def _box_list_to_lines(col_width_list, box_list):
    '''
    Convert horizontal list of boxes into a single list of text lines.
    '''
    line_ct = max(len(box) for box in box_list)
    lines   = []

    for line_i in range(line_ct):
        segments = [_fill(col_width, _val(box, line_i))
                    for col_width, box in zip(col_width_list, box_list)]
        lines.append('  '.join(segments))
    return lines


_RE_PREFIX = re.compile( '^' +
    p4gf_const.P4GF_DEPOT_BRANCH_ROOT.format(
                                   P4GF_DEPOT = p4gf_const.P4GF_DEPOT
                                 , repo_name  = '[^/]*'
                                 , branch_id  = '(../../[^/]+)'
                                 ) + '/' )

# Don't waste 3 character columns on '...'. Our workstations have had single-
# column ellipsis characters for almost 30 years.
_ELLIPSIS = '\u2026'

def abbrev_depot_path(depot_path, val_max_width=_VAL_MAX_WIDTH):
    '''
    Depot paths often have a long lightweight branch prefix:

    //.git-fusion/branches/p4gf_repo/G9/vx/HmW4TdiHNhpIojtWTg==
        /depot/master/dir/dir2/file.txt

    Chop that off and replae with the abbreviated branch ID.
    If still too long, chop off the middle (depot/master/dir) until fits.
    '''
    m = _RE_PREFIX.search(depot_path)
    if not m:
        return depot_path[-val_max_width:]

    prefix = m.group(0)
    suffix = depot_path[len(prefix):]
    branch_id = p4gf_util.abbrev(m.group(1).replace('/',''))
    prefix = '{ellipsis}{branch_id}/'.format( ellipsis  = _ELLIPSIS
                                            , branch_id = branch_id )
    suf_max = val_max_width - len(prefix)
    if suf_max < len(suffix):
        suffix = _ELLIPSIS + suffix[-(suf_max - 1):]
    return prefix + suffix
