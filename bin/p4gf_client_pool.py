#! /usr/bin/env python3.3
"""
p4gf_client_pool.py

A pool of temporary Perforce client spec objects that we can use when querying
Perforce for files within a defined view, usually a single branch view.

"""
import logging

from   p4gf_access_queue import AccessQueue
import p4gf_branch
import p4gf_const
from   p4gf_l10n import _, NTR
import p4gf_util

LOG = logging.getLogger(__name__)

def _to_key(view_lines):
    '''list of lines is not hashable. Join 'em.'''
    return '\n'.join(view_lines)

# pylint: disable=C0103
# Invalid name
class ClientPool:
    '''
    A pool of temporary Perforce client spec objects that we can use when querying
    Perforce for files within a defined view, usually a single branch view.

    We query the same set of branches over and over: each time we switch a client
    view, that's a write to the db.view table. Rather than switch a single client's
    view over and over, create a pool of clients, each switched to a view, and use
    the appropriate client for the query.

    Deletes clients upon exit.
    '''

    def __init__(self, ctx):
        self.ctx                       = ctx

            # A queue of view_lines, sequenced by access time.
            # When we're full, we pop the least-recently-accessed
            # view_lines off this queue and recycle its associated
            # client spec.
        self.q                         = AccessQueue(maxlen=10)

            # Associate view_lines with the client spec that uses them.
        self.view_lines_to_client_name = {}

    def for_view(self, view_lines):
        '''
        Return the name of a Perforce client spec that has the requested view.
        '''
        # Already have one?
        key = _to_key(view_lines)
        client_name = self.view_lines_to_client_name.get(key)
        if client_name:
            self.q.access(key)
            return client_name

        # Do we have room to create a new client?
        if not self.q.is_full():
            client_name = self._create_client_for_view_lines(view_lines)

        # No room. Recycle an old client.
        else:
            old_view_lines = self.q.pop_oldest()
            client_name    = self.view_lines_to_client_name[old_view_lines]
            del self.view_lines_to_client_name[old_view_lines]
            self._set_view_lines(client_name, view_lines)

        self.q.access(key)
        self.view_lines_to_client_name[key] = client_name
        return client_name

    def cleanup(self):
        '''
        Delete any temp client specs created on this server for this repo.

        Don't limit this to clients created by this pool instance.
        '''
        pattern = p4gf_const.P4GF_REPO_TEMP_CLIENT.format( server_id = p4gf_util.get_server_id()
                                                         , repo_name = self.ctx.config.view_name
                                                         , n         = '*')
        clients = [client['client'] for client
                   in self.ctx.p4gfrun(NTR(["clients", "-e", pattern]))]
        for client in clients:
            self.ctx.p4gfrun(NTR(["client", "-d", client]))
        # also clear this instance of references to deleted clients
        self.q.clear()
        self.view_lines_to_client_name.clear()

    def _create_client_for_view_lines(self, view_lines):
        '''
        Create a new client spec with the requested view_lines.

        Return its name.
        '''
        client_name = self._generate_client_name()

        # Let client root match the real ctx.p4's client so that server-
        # calculated local paths match. Might come in handy.
        client_root = self.ctx.contentlocalroot

        desc = (_("Created by Perforce Git Fusion for queries in '{view}'.")
                .format(view=self.ctx.config.view_name))

        # Replae RHS lines with new client name.
        new_view_map = p4gf_branch.replace_client_name(
                            view_lines, self.ctx.config.p4client, client_name)

        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('_create_client_for_view_lines() name={} view={}'
                       .format(client_name, new_view_map.as_array()))
        else:
            LOG.debug2('_create_client_for_view_lines() name={}'

                     .format(client_name))
        p4gf_util.set_spec( self.ctx.p4gf
                          , 'client', spec_id=client_name
                          , values={'Owner'         : p4gf_const.P4GF_USER
                                   , 'LineEnd'      : NTR('unix')
                                   , 'View'         : new_view_map.as_array()
                                   , 'Root'         : client_root
                                   , 'Host'         : None
                                   , 'Description'  : desc})
        return client_name

    def _set_view_lines(self, client_name, view_lines):
        '''
        Change an existing client's view.
        '''
        new_view_map = p4gf_branch.replace_client_name(
                            view_lines, self.ctx.config.p4client, client_name)

        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('_set_view_lines() name={} view={}'
                       .format(client_name, new_view_map.as_array()))
        else:
            LOG.debug2('_set_view_lines() name={}'

                     .format(client_name))
        p4gf_util.ensure_spec_values( self.ctx.p4gf
                                    , 'client', client_name
                                    , { 'View' : new_view_map.as_array() })

    def _generate_client_name(self):
        '''
        Return "git-fusion-{server_id}-{repo_name}-temp-{n}", suitable for use as a
        name for a temporary client
        '''
        return p4gf_const.P4GF_REPO_TEMP_CLIENT.format( server_id = p4gf_util.get_server_id()
                                                      , repo_name = self.ctx.config.view_name
                                                      , n         = len(self.q.q))

