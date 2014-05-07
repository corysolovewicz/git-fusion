#! /usr/bin/env python3.3

'''
Path conversion utilities to go between depot, client, Git work tree, and
local filesystem formats.


X.to_depot()  : return a single path in depot syntax     : '//depot/file'
X.to_client() : return a single path in client syntax    : '//myclient/file'
X.to_gwt()    : return a single path in Git work tree    : 'file'
X.to_local()  : return a single path in local filesystem : '/User/bob/file'

Paths in depot and client syntax are escaped: The evil chars @#%* are
converted to %-escape sequences.

Paths in Git work tree and local syntax are unescaped: %-escaped sequences
are converted to the evil chars @#%*.

Depot/client conversion requires a P4.Map (aka MapApi) object.
gwt/local conversion requires a client root.

Usually you'll use Context to create the object, then convert. Something like this:

    depot_file = ctx.gwt_path(blob['file']).to_depot()

'''
import os

from p4gf_util import escape_path, unescape_path

class BasePath:
    '''
    Base class to hold what we need and cover some common conversions.

    You MUST override either to_depot() or to_client(). Failure to do so
    will result in infinite loops. Even on really fast CPUs.
    '''

    def __init__(self, p4map, client_name, client_root, path):
        self.p4map       = p4map
        self.client_name = client_name
        self.client_root = client_root  # Must not end in trailing delimiter / .
        self.path        = path         # Syntax unknown by Base,
                                        # known by derived class.

    def to_depot(self):
        '''
        Return path in depot syntax, escaped.

        Suitable for use with Perforce commands
        except for 'p4 add' of evil @#%* chars.
        '''
        ### Zig knows it's possible to have a single RHS map to multiple LHS.
        ### The P4 C API returns a collection of strings here.
        ### Why does the P4Python API return only a single string?
        return self.p4map.translate(self.to_client(), self.p4map.RIGHT2LEFT)

    def to_client(self):
        '''
        Return path in client syntax, escaped.

        Suitable for use with Perforce commands
        except for 'p4 add' of evil @#%* chars.
        '''
        return self.p4map.translate(self.to_depot())

    def to_gwt(self):
        '''
        Return path relative to client root, unescaped.

        Suitable for use with Git and some filesystem operations
        as long as current working directory is GIT_WORK_TREE.
        '''
        c = self.to_client()
        if not c:
            return None
        c_rel_esc = c[3+len(self.client_name):]
        return unescape_path(c_rel_esc)

    def to_local(self):
        '''
        Return absolute path in local filesystem syntax, unescaped.

        Suitable for use in all filesystem operations.
        '''
        gwt = self.to_gwt()
        if not gwt:
            return None
        return os.path.join(self.client_root, gwt)


class ClientPath(BasePath):
    '''A path in client syntax: //myclient/foo'''

    def __init__(         self, p4map, client_name, client_root, path):
        BasePath.__init__(self, p4map, client_name, client_root, path)

    def to_client(self):
        return self.path


class DepotPath(BasePath):
    '''A path in depot syntax: //depot/foo'''

    def __init__(         self, p4map, client_name, client_root, path):
        BasePath.__init__(self, p4map, client_name, client_root, path)

    def to_depot(self):
        return self.path


class GWTPath(BasePath):
    '''A path in Git Work Tree syntax.'''
    def __init__(         self, p4map, client_name, client_root, path):
        BasePath.__init__(self, p4map, client_name, client_root, path)

    def to_gwt(self):
        return self.path

    def to_client(self):
        gwt_esc = escape_path(self.path)
        return '//{}/'.format(self.client_name) + gwt_esc

class LocalPath(BasePath):
    '''An absolute path in local filesystem syntax.'''
    def __init__(         self, p4map, client_name, client_root, path):
        BasePath.__init__(self, p4map, client_name, client_root, path)

    def to_gwt(self):
        return self.path[1 + len(self.client_root):]

    def to_local(self):
        return self.path

    def to_client(self):
        gwt_esc = escape_path(self.to_gwt())
        return '//{}/'.format(self.client_name) + gwt_esc
