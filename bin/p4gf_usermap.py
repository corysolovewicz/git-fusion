#! /usr/bin/env python3.3
"""
Script that manages the user map file in Git Fusion. The user map consists
of Perforce user names mapped to the email addresses that appear in the Git
commit logs. This is used to associated Git authors with Perforce users,
for purposes of attribution. The Perforce user accounts are typically mapped
automatically by searching for an account with the same email address as the
Git author. In cases where the email addresses are not the same, the Perforce
administrator may add a mapping to the p4gf_usermap file.
"""

import logging
import os
import re
import sys

import p4gf_env_config    # pylint: disable=W0611
import p4gf_const
import p4gf_create_p4
import p4gf_init
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_log
import p4gf_p4user
import p4gf_util

LOG = logging.getLogger('p4gf_usermap')

# Only sync the user map once per run.
_user_map_synced = False
USERMAP_REGEX = re.compile('([^ \t]+)[ \t]+([^ \t]+)[ \t]+"?([^"]+)"?')


def _find_by_tuple_index(index, find_value, users):
    """
    Return the first matching element of tuple_list that matches
    find_value.

    Return None if not found.
    """
    for usr in users:
        if usr[index] == find_value:
            return usr
    return None

# Because tuple indexing is less work for Zig than converting to NamedTuple
TUPLE_INDEX_P4USER   = 0
TUPLE_INDEX_EMAIL    = 1
TUPLE_INDEX_FULLNAME = 2

# pylint: disable=C0103
# C0103 Invalid name
# The correct type is P4User, not p4user.


def tuple_to_P4User(um_3tuple):
    """
    Convert one of our 3-tuples to a P4User.
    """
    p4user = p4gf_p4user.P4User()
    p4user.name      = um_3tuple[TUPLE_INDEX_P4USER  ]
    p4user.email     = um_3tuple[TUPLE_INDEX_EMAIL   ]
    p4user.full_name = um_3tuple[TUPLE_INDEX_FULLNAME]
    return p4user


class UserMap:
    """
    Mapping of Git authors to Perforce users. Caches the lists of users
    to improve performance when performing repeated searches (e.g. when
    processing a Git push consisting of many commits).
    """

    def __init__(self, p4):
        # List of 3-tuples: first whatever's loaded from p4gf_usermap,
        # then followed by single tuples fetched from 'p4 users' to
        # satisfy later lookup_by_xxx() requests.
        self.users = None

        # List of 3-tuples, filled in only if needed.
        # Complete list of all Perforce user specs, as 3-tuples.
        self.p4users = None

        self.p4 = p4
        self._case_sensitive = None

    def _is_case_sensitive(self):
        """
        Returns True if the server indicates case-handling is 'sensitive',
        and False otherwise.
        """
        if self._case_sensitive is None:
            info = p4gf_util.first_dict(self.p4.run('info'))
            self._case_sensitive = info.get('caseHandling') == 'sensitive'
        return self._case_sensitive

    def _read_user_map(self):
        """
        Reads the user map file from Perforce into a list of tuples,
        consisting of username, email address, and full name. If no
        such file exists, an empty list is returned.

        Returns a list of 3-tuples: (p4user, email, fullname)
        """
        usermap = []
        root = p4gf_util.p4_to_p4gf_dir(self.p4)
        mappath = root + '/users/p4gf_usermap'

        global _user_map_synced
        if not _user_map_synced:
            # don't let a writable usermap file get in our way
            self.p4.run('sync', '-fq', mappath)
            _user_map_synced = True

        if not os.path.exists(mappath):
            return usermap

        with open(mappath) as mf:
            no_folding = self._is_case_sensitive()
            for line in mf:
                if not line:
                    continue
                line = line.strip()
                if not line or line[0] == '#':
                    continue
                m = USERMAP_REGEX.search(line)
                if not m:
                    LOG.debug('No match: {}'.format(line))
                    continue

                p4user   = m.group(1) if no_folding else m.group(1).casefold()
                email    = m.group(2)
                _validate_email(email)
                fullname = p4gf_util.dequote(m.group(3))
                usermap.append((p4user, email, fullname))
        return usermap

    def _get_p4_users(self):
        """
        Retrieve the set of users registered in the Perforce server, in a
        list of tuples consisting of username, email address, and full name.
        If no users exist, an empty list is returned.

        Returns a list of 3-tuples: (p4user, email, fullname)
        """
        users = []
        results = self.p4.run('users')
        if results:
            no_folding = self._is_case_sensitive()
            for r in results:
                name = r['User'] if no_folding else r['User'].casefold()
                users.append((name, r['Email'], r['FullName']))
        return users

    def _lookup_by_tuple_index(self, index, value):
        """
        Return 3-tuple for user whose tuple matches requested value.

        Searches in order:
        * p4gf_usermap (stored in first portion of self.users)
        * previous lookup results (stored in last portion of self.users)
        * 'p4 users' (stored in self.p4users)

        Lazy-fetches p4gf_usermap and 'p4 users' as needed.

        O(n) list scan.
        """
        if not self.users:
            self.users = self._read_user_map()
        # Look for user in existing map. If found return. We're done.
        user = _find_by_tuple_index(index, value, self.users)
        if user:
            return user

        # Look for user in Perforce.
        if not self.p4users:
            self.p4users = self._get_p4_users()
        user = _find_by_tuple_index(index, value, self.p4users)

        if not user:
            # Look for the "unknown git" user, if any.
            user = _find_by_tuple_index(TUPLE_INDEX_P4USER,
                                       p4gf_const.P4GF_UNKNOWN_USER,
                                       self.p4users)

        # Remember this search hit for later so that we don't have to
        # re-scan our p4users list again.
        if user:
            self.users.append(user)

        return user

    def lookup_by_email(self, addr):
        """
        Retrieve details for user by their email address, returning a
        tuple consisting of the user name, email address, and full name.
        First searches the p4gf_usermap file in the .git-fusion workspace,
        then searches the Perforce users. If no match can be found, and a
        Perforce user named 'unknown_git' is present, then a fabricated
        "user" will be returned. Otherwise None is returned.
        """
        return self._lookup_by_tuple_index(TUPLE_INDEX_EMAIL, addr)

    def lookup_by_p4user(self, p4user):
        """
        Return 3-tuple for given Perforce user.
        """
        if not self._is_case_sensitive():
            p4user = p4user.casefold()
        return self._lookup_by_tuple_index(TUPLE_INDEX_P4USER, p4user)

    def p4user_exists(self, p4user):
        """
        Return True if we saw this p4user in 'p4 users' list.
        """
        # Look for user in Perforce.
        if not self.p4users:
            self.p4users = self._get_p4_users()
        if not self._is_case_sensitive():
            p4user = p4user.casefold()
        user = _find_by_tuple_index(TUPLE_INDEX_P4USER, p4user, self.p4users)
        if user:
            return True
        return False


_VALIDATE_EMAIL_ILLEGAL = '<>,'

def _validate_email(email):
    '''
    Raise error upon unwanted <>,
    '''
    LOG.debug('checking email: {}'.format(email))
    for c in _VALIDATE_EMAIL_ILLEGAL:

        if c in email:
            LOG.error('Nope {} in {}'.format(c, email))
            raise RuntimeError(
                _("Unable to read '{usermap}'."
                  " Illegal character '{c}' in email address '{email}'")
                .format( usermap = 'p4gf_usermap'
                       , c       = c
                       , email   = email))


def main():
    """
    Parses the command line arguments and performs a search for the given
    email address in the user map.
    """
    p4gf_util.has_server_id_or_exit()
    log_l10n()

    # Set up argument parsing.
    parser = p4gf_util.create_arg_parser(
        _("Searches for an email address in the user map."))
    parser.add_argument(NTR('email'), metavar='E',
                        help=_('email address to find'))
    args = parser.parse_args()

    # make sure the world is sane
    ec = p4gf_init.main()
    if ec:
        print(_("p4gf_usermap initialization failed"))
        sys.exit(ec)

    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4(client=p4gf_util.get_object_client_name())
        if not p4:
            sys.exit(1)

        usermap = UserMap(p4)
        user = usermap.lookup_by_email(args.email)
        if user:
            print(_("Found user '{}' <{}>").format(user[0], user[2]))
            sys.exit(0)
        else:
            sys.stderr.write(_("No such user found: '{}'\n").format(args.email))
            sys.exit(1)


if __name__ == '__main__':
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)
