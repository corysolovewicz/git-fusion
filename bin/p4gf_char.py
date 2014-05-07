#! /usr/bin/env python3.3
"""
Functions for dealing with character encoding.
"""
from   p4gf_l10n import NTR


def _encoding_list():
    """
    Return a list of character encodings, in preferred order,
    to use when attempting to read bytes of unknown encoding.
    """
                        ### Zig warns that 'latin_1' can decode ANY byte array.
                        ### 'latin-1' is a straight pass-through of
                        ###     \xXX 8-bit bytes in ==> U+00XX Unicode chars out
                        ###
                        ### Therefore we'll never attempt 'shift_jis', nor
                        ### will decode() ever give up and re-raise the original
                        ### utf8 encoding exception.

    return NTR(['utf8', 'latin_1', 'shift_jis'])


def decode(bites):
    """
    Attempt to decode using one of several code pages.
    """
    for encoding in _encoding_list():
        try:
            s = bites.decode(encoding)
            return s
        except UnicodeDecodeError:
            pass
    # Give up, re-create and raise the first error.
    bites.decode(_encoding_list[0])
