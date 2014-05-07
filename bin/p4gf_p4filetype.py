#! /usr/bin/env python3.3

'''
Perforce filetypes handling.
'''
from   p4gf_l10n      import _, NTR

# See 'p4 help filetypes'
#	       Type        Is Base Type  Plus Modifiers
#	      --------    ------------  --------------
ALIASES = {
	      'ctempobj' : ['binary',    'S', 'w'       ]
	    , 'ctext'    : ['text',      'C'            ]
	    , 'cxtext'   : ['text',      'C', 'x'       ]
	    , 'ktext'    : ['text',      'k'            ]
	    , 'kxtext'   : ['text',      'k', 'x'       ]
	    , 'ltext'    : ['text',      'F'            ]
	    , 'tempobj'  : ['binary',    'F', 'S', 'w'  ]
	    , 'ubinary'  : ['binary',    'F'            ]
	    , 'uresource': ['resource',  'F'            ]
	    , 'uxbinary' : ['binary',    'F', 'x'       ]
	    , 'xbinary'  : ['binary',    'x'            ]
	    , 'xltext'   : ['text',      'F', 'x'       ]
	    , 'xtempobj' : ['binary',    'S', 'w', 'x'  ]
	    , 'xtext'    : ['text',      'x'            ]
	    , 'xunicode' : ['unicode',   'x'            ]
	    , 'xutf16'   : ['utf16',     'x'            ]
        }

def to_base_mods(filetype):
    '''
    Split a string p4filetype like "xtext" into an array of 2+ strings:
    'text'      => ['text', '' ]
    "xtext"     => ['text', 'x']
    "+x"        => ['',     'x']
    "ktext+S10" => ['text', 'k', 'S', '1', '0']

    Invalid filetypes produce undefined results.

    Multi-char filetypes like +S1 become multiple elements in the returned list.
    '''

    # +S<n> works only because we tear down and rebuild our + mod chars in
    # the same sequence. We actually treat +S10 as +S +1 +0, then rebuild
    # that to +S10 and it just works. Phew.

    # Just in case we got 'xtext+k', split off any previous mods.
    base_mod = filetype.split('+')
    mods = base_mod[1:]
    base = base_mod[0]
    if mods:
        # Try again with just the base.
        base_mod = to_base_mods(base)
        if base_mod[1]:
            mods += base_mod[1:]
            base = base_mod[0]

    if base in ALIASES:
        x = ALIASES[base]
        base = x[0]
        if mods:
            mods += x[1:]
        else:
            mods = x[1:]

    if mods:
        return [ base ] + mods
    else:
        return [ base , '' ]


def from_base_mods(base, mods):
    '''
    Return 'text+x', or just '+x' or 'text' or even ''.

    base : string like "text"
    mods : list of modifiers ['x'].
           Ok if empty or if contains empty string ''.
           Order preserved, so OK to split multi-char mods
           like "+S10" into multiple chars ['S', '1', '0']
    '''
    if not mods:
        return base
    if not base:
        return '+' + ''.join(mods)
    return base + '+' + ''.join(mods)


def remove_mod(filetype, mod):
    '''
    Remove a single modifier such as 'x'

    Cannot remove multiple modifiers or +S<n>.
    '''
    if 1 != len(mod):
        raise RuntimeError(_('BUG: Cannot remove multiple modifier chars: {}').format(mod))

    base_mods = to_base_mods(filetype)
    base_str  = base_mods[0]
    mods      = base_mods[1:]
    if '' in mods:
        mods.remove('')
    if mod in mods:
        mods.remove(mod)

    if not mods:
        return base_str
    return NTR('{base}+{mods}').format(base=base_str,
                                  mods=''.join(mods))


def replace_base(filetype, oldbase, newbase):
    '''
    Used to convert unicode or utf16 to binary
    oldbase = array of possible types to convert from
    newbase = new base type
    '''

    if not filetype:
        return filetype
    newtype = filetype
    for obase in oldbase:
        if obase == newbase:
            continue
        elif obase in filetype:
            newtype = filetype.replace(obase, newbase)
            break
    return newtype


