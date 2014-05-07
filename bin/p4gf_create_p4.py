#! /usr/bin/env python3.3
"""Create a new P4.P4() instance."""

import sys
import os

# Imports for really annoying but occasionally useful debug3 traceback + delay:
#import logging
#import time
#import traceback

import P4
import p4gf_const
from   p4gf_l10n import _
import p4gf_log
import p4gf_version

LOG = p4gf_log.for_module()
# debug3 = create/connect/destroy tracking for leaked connections.
#          If enabled, p4_connect() sleeps for a few seconds after
#          each p4.connect(), to make it easiser to line up timestamps between
#          Git Fusion debug logs with Perforce server or proxy logs.
#          Also dumps stack traces to each call to p4_connect() so you can
#          see who is responsible.

# Every known connection we've created. So that we can close them when done.
_CONNECTION_LIST = []

def create_p4(port=None, user=None, client=None, connect=True):
    """Return a new P4.P4() instance with its prog set to
    'P4GF/2012.1.PREP-TEST_ONLY/415678 (2012/04/14)'

    By default the P4 is connected; call with connect=False to skip connection.

    There should be NO bare calls to P4.P4().

    """
    if 'P4PORT' in os.environ:
        LOG.debug("os.environment['P4PORT'] {0}".format(os.environ['P4PORT']))
    p4 = P4.P4()
    LOG.debug("default p4.port = {0}".format(p4.port))

    p4.prog = p4gf_version.as_single_line()
    p4.exception_level = P4.P4.RAISE_ERRORS

    if port:
        p4.port = port
    if user:
        p4.user = user
    else:
        p4.user = p4gf_const.P4GF_USER
    if client:
        p4.client = client

    _CONNECTION_LIST.append(p4)

    if connect:
        try:
            LOG.debug("p4_connect(): u={} {}".format(user, p4))
            p4_connect(p4)

        except P4.P4Exception as e:
            sys.stderr.write(_('error: cannot connect, p4d not running?\n'))
            sys.stderr.write(_('Failed P4 connect: {}'.format(str(e))))
            return None
        p4gf_version.p4d_version_check(p4)

    return p4


def p4_connect(p4):
    '''
    Route ALL calls to p4.connect() through this function so we track who
    connected from where. Who created the leaking connection?
    '''
    cm = p4.connect()
    #if LOG.isEnabledFor(logging.DEBUG3):
    #    LOG.debug3('p4_connect        id(p4)={}\n{}'
    #           .format(id(p4), ''.join(traceback.format_stack())))
    #    time.sleep(4)
    return cm

def p4_disconnect(p4):
    '''
    Route ALL calls to p4.disconnect() through this function so we track who
    disconnected from where. Who created the leaking connection?
    '''
    cm = p4.disconnect()
    #if LOG.isEnabledFor(logging.DEBUG3):
    #    LOG.debug3('p4_disconnect        id(p4)={}\n{}'
    #           .format(id(p4), ''.join(traceback.format_stack())))
    #    time.sleep(4)
    return cm

def close_all():
    '''
    Close every connection we created.
    '''
    for p4 in _CONNECTION_LIST:
        try:
            LOG.debug3('close_all()       id(p4)={} connected={}'
                       .format(id(p4), p4.connected()))
            if p4.connected():
                p4_disconnect(p4)

            # pylint:disable=W0703
            # Catching too general exception Exception
            # This is cleanup code. If we fail, that's okay. At worst we
            # leave a connection around for a few more seconds.
        except Exception as e:
            LOG.error(e)

    del _CONNECTION_LIST[:]

def destroy(p4):
    '''
    Disconnect and unregister and delete.
    '''
    LOG.debug3('destroy()         id(p4)={} connected={}'
               .format(id(p4), p4.connected()))
    if p4.connected():
        p4_disconnect(p4)
    unregister(p4)
    del p4

def unregister(p4):
    '''
    Some code is smart enough to close and destroy its own P4 connection.
    Let go of the object so that it can leave our heap.
    '''
    LOG.debug3('unregister()      id(p4)={} connected={}'
               .format(id(p4), p4.connected()))
    assert not p4.connected()    # Require that the caller really did disconnect.
    if p4 in _CONNECTION_LIST:
        _CONNECTION_LIST.remove(p4)


class Connector:
    '''
    RAII object that connects and disconnects a P4 connection.
    '''
    def __init__(self, p4):
        self.p4 = p4

    def __enter__(self):
        p4_connect(self.p4)
        return self.p4

    def __exit__(self, _exc_type, _exc_value, _traceback):
        p4_disconnect(self.p4)
        return False  # False == do not squelch any current exception

class Closer:
    '''
    RAII object that closes all P4 connections on exit.
    '''

    def __init__(self):
        pass

    def __enter__(self):
        return None

    def __exit__(self, _exc_type, _exc_value, _traceback):
        '''
        Close all registered connections.
        '''
        close_all()
        return False  # False == do not squelch any current exception

