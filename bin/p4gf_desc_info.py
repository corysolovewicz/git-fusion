#! /usr/bin/env python3.3
'''
Storing data in a P4 Changelist description.
'''

import re
import p4gf_const


class DescInfo:
    '''
    Data we write to the bottom of a Perforce changelist description.
    '''
    def __init__(self):
                # Description text WITHOUT our header or noise
        self.clean_desc             = None

                # Our header and noise
        self.suffix                 = None

                # author and committer are dicts.
        self.author                 = None
        self.committer              = None

        self.author_p4              = None  # p4user id, never written to text
        self.pusher                 = None  # p4user id
        self.sha1                   = None
        self.push_state             = None
        self.depot_branch_id        = None
        self.contains_p4_extra      = False

                # list of (sha1, path) tuples defining any submodules
        self.gitlinks               = None
                # list of ordered parent Git commits (SHA1 strings)
        self.parents                = None

                # String "{depot-branch-id}@{change-num}"
                # GPARN[0]'s depot branch id and changelist number
                #
                # Left empty unless GPARN[0] is on a different branch
                # from this changelist.
                #
        self.parent_branch          = None

        self.ghost_of_sha1          = None
        self.ghost_of_change_num    = None
        self.ghost_precedes         = None

    def __setitem__(self, key, value):
        if key == p4gf_const.P4GF_DESC_KEY_DEPOT_BRANCH_ID:
            self.depot_branch_id = value
        elif key == p4gf_const.P4GF_DESC_KEY_PUSH_STATE:
            self.push_state = value
        elif key == p4gf_const.P4GF_DESC_KEY_CONTAINS_P4_EXTRA:
            self.contains_p4_extra = value
        elif key == p4gf_const.P4GF_DESC_KEY_GHOST_OF_SHA1:
            self.ghost_of_sha1 = value
        elif key == p4gf_const.P4GF_DESC_KEY_GHOST_OF_CHANGE_NUM:
            self.ghost_of_change_num = value
        elif key == p4gf_const.P4GF_DESC_KEY_GHOST_PRECEDES_SHA1:
            self.ghost_precedes = value
        elif key == p4gf_const.P4GF_DESC_KEY_PARENT_BRANCH:
            self.parent_branch = value
        else:
            setattr(self, key.lower(), value)

    def __getitem__(self, key):
        if key == p4gf_const.P4GF_DESC_KEY_DEPOT_BRANCH_ID:
            return self.depot_branch_id
        elif key == p4gf_const.P4GF_DESC_KEY_PUSH_STATE:
            return self.push_state
        elif key == p4gf_const.P4GF_DESC_KEY_CONTAINS_P4_EXTRA:
            return self.contains_p4_extra
        elif key == p4gf_const.P4GF_DESC_KEY_GHOST_OF_SHA1:
            return self.ghost_of_sha1
        elif key == p4gf_const.P4GF_DESC_KEY_GHOST_OF_CHANGE_NUM:
            return self.ghost_of_change_num
        elif key == p4gf_const.P4GF_DESC_KEY_GHOST_PRECEDES_SHA1:
            return self.ghost_precedes
        elif key == p4gf_const.P4GF_DESC_KEY_PARENT_BRANCH:
            return self.parent_branch
        else:
            return getattr(self, key.lower())

    def __delitem__(self, key):
        self.__setitem__(key, None)

    def __len__(self): # Just to make pylint happy
        return 8

    def __repr__(self):
        return self.to_text()

    @staticmethod
    def from_text(text):
        '''
        Parse a changelist description into a new DescInfo object.

        Scans for our "Imported from Git:" header and then
        converts the remainder into fields.

        Return None if "Imported from Git" header missing.
        '''

        # Use str.rfind(), not find, just in case a human intentionally used
        # our header's phrase in their own text.
        impidx = text.rfind(p4gf_const.P4GF_IMPORT_HEADER)
        if impidx < 0:
            # No 13.1 header. Is this from Git Fusion 12.x?
            impidx = text.rfind(p4gf_const.P4GF_IMPORT_HEADER_OLD)
            if impidx < 0:
                return None
        suffix = text[impidx:]

        r = DescInfo()
        r.clean_desc = text[:impidx-1]
        r.suffix     = suffix

        # Author/Committer fields require multiple values.
        for key in ( p4gf_const.P4GF_DESC_KEY_AUTHOR
                   , p4gf_const.P4GF_DESC_KEY_COMMITTER ):
            # Allow for the user name to be optional.
            regex = re.compile(key.capitalize() + r':(.+)? (<.*>) (\d+) ([-+\d]+)')
            match = regex.search(suffix)
            if match:
                fullname = match.group(1)
                fullname = fullname.strip() if fullname else ' '
                d = { 'fullname' : fullname
                    , 'email'    : match.group(2)
                    , 'time'     : match.group(3)
                    , 'timezone' : match.group(4) }
                r[key] = d

        # Extract any submodule/gitlink entries as a list of (sha1, path) tuples
        regex = re.compile(r'gitlink: ([^/]+)/(.+)$', re.MULTILINE)
        links = []
        for match in regex.finditer(suffix):
            links.append((match.group(1).strip(), match.group(2).strip()))
        if links:
            r.gitlinks = links

        # Everything else is freeform
        for key in ( p4gf_const.P4GF_DESC_KEY_PUSHER
                   , p4gf_const.P4GF_DESC_KEY_SHA1
                   , p4gf_const.P4GF_DESC_KEY_PUSH_STATE
                   , p4gf_const.P4GF_DESC_KEY_CONTAINS_P4_EXTRA
                   , p4gf_const.P4GF_DESC_KEY_DEPOT_BRANCH_ID
                   , p4gf_const.P4GF_DESC_KEY_PARENTS
                   , p4gf_const.P4GF_DESC_KEY_PARENT_BRANCH
                   , p4gf_const.P4GF_DESC_KEY_GHOST_OF_SHA1
                   , p4gf_const.P4GF_DESC_KEY_GHOST_OF_CHANGE_NUM
                   , p4gf_const.P4GF_DESC_KEY_GHOST_PRECEDES_SHA1
                   ) :
            regex = re.compile(key + r': (.+)')
            match = regex.search(suffix)
            if match:
                r[key] = match.group(1).strip()
        if r.parents and isinstance(r.parents, str):
            # Convert the space-delimited parents to a list of SHA1s
            # pylint:disable=E1103
            r.parents = r.parents.split(' ')
            # pylint:enable=E1103

        return r

    def to_text(self):
        '''
        Return a changelist description with our data.
        '''
        # Build as a list of strings we'll join later.
        parts = [self.clean_desc]
        # Avoid adding anything between the clean_desc and the 'Imported' line
        # below, otherwise be sure to update the add_commit() code in
        # p4gf_fastimport that strips away this audit fluff when re-cloning
        # the changes.
        parts.append(p4gf_const.P4GF_IMPORT_HEADER)

        def _append(key, value):
            '''Append one line.'''
            if value:
                parts.append(" {}: {}".format(key, value))

        # Author and Committer are formatted.
        for key in ( p4gf_const.P4GF_DESC_KEY_AUTHOR
                   , p4gf_const.P4GF_DESC_KEY_COMMITTER ):
            d = self[key]
            if d:
                _append(key, "{0} {1} {2} {3}".format( d['fullname' ]
                                                     , d['email'    ]
                                                     , d['time'     ]
                                                     , d['timezone' ]))

        # Pusher written only if different from Author.
        if self.pusher != self.author_p4:
            _append(p4gf_const.P4GF_DESC_KEY_PUSHER, self.pusher)

        # Git commit sha1 and push state never empty, always written.
        _append(p4gf_const.P4GF_DESC_KEY_SHA1,       self.sha1)
        _append(p4gf_const.P4GF_DESC_KEY_PUSH_STATE, self.push_state)

        # These values somtimes empty/False, only written if non-None/False
        _append( p4gf_const.P4GF_DESC_KEY_DEPOT_BRANCH_ID,     self.depot_branch_id)
        _append( p4gf_const.P4GF_DESC_KEY_CONTAINS_P4_EXTRA,   self.contains_p4_extra)
        _append( p4gf_const.P4GF_DESC_KEY_GHOST_OF_SHA1,       self.ghost_of_sha1)
        _append( p4gf_const.P4GF_DESC_KEY_GHOST_OF_CHANGE_NUM, self.ghost_of_change_num)
        _append( p4gf_const.P4GF_DESC_KEY_GHOST_PRECEDES_SHA1, self.ghost_precedes)
        _append( p4gf_const.P4GF_DESC_KEY_PARENT_BRANCH,       self.parent_branch)

        # Append submodule/gitlink entries as "gitlink: SHA1/path"
        if self.gitlinks:
            for (sha1, path) in self.gitlinks:
                _append(p4gf_const.P4GF_DESC_KEY_GITLINK, "{}/{}".format(sha1, path))
        if self.parents:
            _append(p4gf_const.P4GF_DESC_KEY_PARENTS, ' '.join(self.parents))

        return '\n'.join(parts)
