#! /usr/bin/env python3.3
'''get list of repos'''

import p4gf_config
import p4gf_group
from   p4gf_l10n      import NTR
import p4gf_util


class RepoList:
    '''build list of repos available to user'''

    def __init__(self):
        '''empty list'''
        self.repos = []

    @staticmethod
    def list_for_user(p4, user):
        '''build list of repos visible to user'''
        result = RepoList()

        for view in p4gf_util.view_list(p4):
            #check user permissions for view
            # PERM_PUSH will avoid checking the repo config file for read-permission-check = user
            view_perm = p4gf_group.ViewPerm.for_user_and_view(p4,
                                                            user,
                                                            view,
                                                            p4gf_group.PERM_PUSH)
            #sys.stderr.write("view: {}, user: {}, perm: {}".format(view, user, view_perm))
            if view_perm.can_push():
                perm = NTR('push')
            elif view_perm.can_pull():
                perm = NTR('pull')
            else:
                continue

            config = p4gf_config.get_repo(p4, view)
            charset = config.get(p4gf_config.SECTION_REPO, p4gf_config.KEY_CHARSET, fallback='')
            desc = config.get(p4gf_config.SECTION_REPO, p4gf_config.KEY_DESCRIPTION, fallback='')
            result.repos.append((view, perm, charset, desc))

        result.repos.sort(key=lambda tup: tup[0])
        return result
