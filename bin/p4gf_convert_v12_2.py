#! /usr/bin/env python3.3
"""Converts version 2012.2 Git Fusion Perforce servers for use with 2013.1 Git Fusion

This script must be run as part of the upgrade process
from Git Fusion 2012.2 to 2013.1.

As with any upgrade, a checkpoint prior to running is recommended

See the release notes for details on how to upgrade.

This script will:

* delete client git-fusion-<space> workspace files
* create the Git Fusion config files for the previously created repos

This script will also:

* delete object client workspace files
* OPTIONAL: obliterate //.git-fusion/objects/...

Invoke with -h for usage information.

"""

import os
import shutil
import sys
import getpass


import P4
import p4gf_const
import p4gf_context
import p4gf_create_p4
import p4gf_lock
import p4gf_util
from   p4gf_ensure_dir import ensure_dir
from p4gf_config import create_from_12x_gf_client_name
import p4gf_super_init

LOG_FILE = None

# pylint:disable=W9903
# non-gettext-ed string
# Do not translate this script to other human languages. 12.2 was US English-only,
# so shall be the upgrade script from 12.2.

def create_server_id(localroot, server_id, p4):
    """ Create the server-id file and counter
    using method in p4gf_super_init.
    """
    ensure_dir(localroot)
    if server_id:
        p4gf_super_init.ID_FROM_ARGV = server_id[0]
    p4gf_super_init.p4 = p4
    p4gf_super_init.ensure_server_id()
    return p4gf_util.read_server_id_from_file()

# pylint: disable=R0912
# Too many branches: fix would result in many arguments to separate functions
def convert_client(args, p4, client_name):
    """Convert the named Perforce client and its workspace. Raises
    P4Exception if the client is not present, or the client configuration is
    not set up as expected.

    Keyword arguments:
    args        -- parsed command line arguments
    p4          -- Git user's Perforce client
    client_name -- name of client to be deleted

    """
    group_list = [p4gf_const.P4GF_GROUP_VIEW_PULL, p4gf_const.P4GF_GROUP_VIEW_PUSH]
    p4.user = p4gf_const.P4GF_USER
    old_client = p4.client
    p4.client = client_name

    print("  Processing client {}...".format(client_name))
    if not p4gf_util.spec_exists(p4, 'client', client_name):
        raise P4.P4Exception('No such client "{}" defined'
                             .format(client_name))

    view_name = client_name[len(p4gf_const.P4GF_CLIENT_PREFIX):]
    client = p4.fetch_client()
    command_path = client["Root"]

    if not args.convert:
        print("    Removing client files for {}...".format(client_name))
        print("      p4 sync -fqk {}/...#none".format(command_path))
        print("    Create config for client {}...".format(client_name))
        p4.client = old_client
        for group_template in group_list:
            group = group_template.format(view=view_name)
            print("    Leaving existing group {}".format(group))
        print("    Remove client lock counter")
        print("      p4 counter -u -d {}".format(p4gf_lock.view_lock_name(view_name)))

    else:
        LOG_FILE.write("Processing client {}\n".format(client_name))
        print("    Removing client files for {}...".format(client_name))
        print("      p4 sync -fqk {}/...#none".format(command_path))
        LOG_FILE.write("p4 sync -fqk {}/...#none\n".format(command_path))
        p4.run('sync', '-fqk', command_path + '/...#none')
        print("    Creating config for client {}...".format(client_name))
        LOG_FILE.write("Creating config for client {}...\n".format(client_name))
        p4.client = old_client
        create_from_12x_gf_client_name(p4, client_name)
        for group_template in group_list:
            group = group_template.format(view=view_name)
            print("    Leaving existing group {}".format(group))
        print("    Remove client lock counter")
        print("      p4 counter -u -d {}".format(p4gf_lock.view_lock_name(view_name)))
        LOG_FILE.write("p4 counter -u -d {}\n".format(p4gf_lock.view_lock_name(view_name)))
        _delete_counter(p4, p4gf_lock.view_lock_name(view_name))
# pylint: enable=R0912


def _delete_counter(p4, name):
    """Attempt to delete counter. Report and continue on error."""
    try:
        p4.run('counter', '-u', '-d', name)
    except P4.P4Exception as e:
        if str(e).find("No such counter") < 0:
            print('ERROR: Failed to delete counter {ctr}: {e}'.
                     format(ctr=name, e=str(e)))


def get_p4gf_localroot(p4):
    """Calculate the local root for the object client."""
    if p4.client != p4gf_util.get_12_2_object_client_name():
        raise RuntimeError('incorrect p4 client')
    client = p4.fetch_client()
    rootdir = client["Root"]
    if rootdir.endswith("/"):
        rootdir = rootdir[:len(rootdir) - 1]
    client_map = P4.Map(client["View"])
    lhs = client_map.lhs()
    if len(lhs) > 1:
        # not a conforming Git Fusion client, ignore it
        return None
    rpath = client_map.translate(lhs[0])
    localpath = p4gf_context.client_path_to_local(rpath, p4.client, rootdir)
    localroot = p4gf_context.strip_wild(localpath)
    return localroot


def convert_clients(args, p4, client_name):
    """Convert all of the Git Fusion clients
    """
    print("Converting clients...")
    r = p4.run('clients', '-e', p4gf_const.P4GF_CLIENT_PREFIX + '*')
    if not r:
        print("  No Git Fusion clients found.")
        return
    for spec in r:
        # Skip all object cache clients, not just the one for this host.
        if spec['client'].startswith(p4gf_const.P4GF_OBJECT_CLIENT_PREFIX):
            if spec['client'] != client_name:
                print("  Warning: ignoring client {}".format(spec['client']))
        # ignore client 'git-fusion-' if present
        elif spec['client'] == p4gf_const.P4GF_CLIENT_PREFIX:
            print("  Warning: ignoring client {}".format(spec['client']))
        # convert repo clients
        else:
            try:
                convert_client(args, p4, spec['client'])
            except P4.P4Exception as e:
                sys.stderr.write(str(e) + '\n')
                sys.exit(1)


# pylint: disable=R0912
# pylint: disable=R0915
def convert(args, p4):
    """Find all git-fusion-* clients and convert them, as well as delete
    the entire object cache (//.git-fusion/objects/...).

    Keyword arguments:
    args -- parsed command line arguments
    p4   -- Git user's Perforce client

    """
    print("Connected to {}".format(p4.port))
    p4.user = p4gf_const.P4GF_USER

    # Sanity check system
    counters = []
    r = p4.run('counters', '-u', '-e', 'git_fusion_auth_server_lock*')
    for spec in r:
        counters.append(spec['counter'])
    r = p4.run('counters', '-u', '-e', 'git_fusion_view_*_lock')
    for spec in r:
        counters.append(spec['counter'])
    if counters:
        print("All Git Fusion servers connecting to this server must be disabled.")
        print("See release notes for instructions on how to proceed.")
        print("The following counters indicate on-going activity:")
        print(", ".join(counters))
        sys.exit(1)

    r = p4.run('counters', '-u', '-e', 'p4gf_auth_keys_last_changenum-*')
    for spec in r:
        counters.append(spec['counter'])
    if not counters:
        print("Does not look like a Git Fusion 2012.2 installation")
        print("Cannot find the counter for p4gf_auth_keys_last_changenum")
        print("See release notes for instructions on how to proceed.")
        sys.exit(1)

    # Retrieve host-specific initialization counters.
    r = p4.run('counters', '-u', '-e', 'git-fusion*-init-started')
    for spec in r:
        counters.append(spec['counter'])
    r = p4.run('counters', '-u', '-e', 'git-fusion*-init-complete')
    for spec in r:
        counters.append(spec['counter'])

    # we require the server_id before we convert the clients
    localroot = get_p4gf_localroot(p4)
    server_id = create_server_id(localroot, args.id, p4)
    client_name = p4gf_util.get_12_2_object_client_name()
    convert_clients(args, p4, client_name)
    group_list = [p4gf_const.P4GF_GROUP_PULL, p4gf_const.P4GF_GROUP_PUSH]

    if not args.convert:
        if localroot:
            print("Removing client files for {}...".format(client_name))
            print("  p4 sync -fqk {}...#none".format(localroot))
            print("Deleting client {}...".format(client_name))
            print("  p4 client -f -d {}".format(client_name))
            print("Deleting client {}'s workspace...".format(client_name))
            print("  rm -rf {}".format(localroot))
        print("Obliterating object cache...")
        if not args.delete:
            print("  p4 obliterate -y //.git-fusion/objects/...")
        else:
            print("  Skipping obliterate")
        print("Removing initialization counters...")
        for counter in counters:
            print("  p4 counter -u -d {}".format(counter))
        for group in group_list:
            print("Leaving existing group {}".format(group))
    else:
        if localroot:
            print("Removing client files for {}...".format(client_name))
            print("  p4 sync -fqk {}...#none".format(localroot))
            LOG_FILE.write("p4 sync -fqk {}...#none\n".format(localroot))
            p4.run('sync', '-fqk', localroot + '...#none')
            print("Deleting client {}...".format(client_name))
            print("  p4 client -f -d {}".format(client_name))
            LOG_FILE.write("p4 client -f -d {}\n".format(client_name))
            p4.run('client', '-df', client_name)
            print("Deleting client {}'s workspace...".format(client_name))
            print("  rm -rf {}".format(localroot))
            LOG_FILE.write("rm -rf {}\n".format(localroot))
            shutil.rmtree(localroot)
            # after removing the GF localroot
            # recreate the GF localroot and re-write the server-id
            # the serverid - counter has been already set above
            # in create_server_id
            ensure_dir(localroot)
            p4gf_util.write_server_id_to_file(server_id)
        if not args.delete:
            print("Obliterating object cache...")
            print("  p4 obliterate -y //.git-fusion/objects/...")
            LOG_FILE.write("p4 obliterate -y //.git-fusion/objects/...\n")
            p4.run('obliterate', '-y', '//.git-fusion/objects/...')
        else:
            print("  Run: p4 delete //.git-fusion/objects/...")
            print("       p4 submit")
            LOG_FILE.write("Need to run: p4 delete //.git-fusion/objects/...\n")
            LOG_FILE.write("             p4 submit\n")
        print("Removing initialization counters...")
        for counter in counters:
            print("  p4 counter -u -d {}".format(counter))
            LOG_FILE.write("  p4 counter -u -d {}\n".format(counter))
            _delete_counter(p4, counter)
        for group in group_list:
            print("Leaving existing group {}".format(group))
# pylint: enable=R0915
# pylint: enable=R0912


def main():
    """Process command line arguments and call functions to do the real
    work of cleaning up the Git mirror and Perforce workspaces.
    """
    # Set up argument parsing.
    parser = p4gf_util.create_arg_parser(
        "Convert 2012.2 Git Fusion configured Perforce Server for use with"
        " 2013.1 Git Fusion.\nThis is not reversable.")
    parser.add_argument("-y", "--convert", action="store_true",
                        help="perform the conversion")
    parser.add_argument("-d", "--delete", action="store_true",
                        help="skip obliterate and show delete command")
    parser.add_argument('--id',                              nargs=1,
            help="Set this Git Fusion server's unique id. Default is hostname.")
    args = parser.parse_args()

    # Do not run as root, this is very git account specific
    user = getpass.getuser()
    if user == "root":
        print("This script should be run using the Git dedicated account")
        return 2

    if args.convert:
        try:
            global LOG_FILE
                                # pylint:disable=W1501
                                # "x" is not a valid mode for open.
                                # Yes it is. Pylint 1.0.0 is incorrect here.
            LOG_FILE = open("p4gf_convert_v12_2.log", "x")
                                # pylint:enable=W1501
            print("Logging to p4gf_convert_v12_2.log")
        except IOError:
            print("Please remove or rename p4gf_convert_v12_2.log")
            sys.exit(1)

    client_name = p4gf_const.P4GF_OBJECT_CLIENT_PREFIX + p4gf_util.get_hostname()
    try:
        p4 = p4gf_create_p4.create_p4(client=client_name)
    except RuntimeError as e:
        sys.stderr.write("{}\n".format(e))
        sys.exit(1)
    if not p4:
        return 2

    # Sanity check the connection (e.g. user logged in?) before proceeding.
    try:
        p4.fetch_client()
        view = ['//{depot}/... //{client}/...'.format(
            depot=p4gf_const.P4GF_DEPOT, client=client_name)]
        spec = {'Host': '',
                'Root': os.path.join(os.environ.get("HOME"),
                p4gf_const.P4GF_DIR),
                'View': view}
        p4gf_util.ensure_spec_values(p4, 'client', client_name, spec)
    except P4.P4Exception as e:
        sys.stderr.write("P4 exception occurred: {}".format(e))
        sys.exit(1)

    try:
        convert(args, p4)
    except P4.P4Exception as e:
        sys.stderr.write("{}\n".format(e))
        sys.exit(1)

    if not args.convert:
        print("This was report mode. Use -y to make changes.")
    else:
        print("Commands run were logged to p4gf_convert_v12_2.log.")
        if args.delete:
            print("You must now run: p4 delete //.git-fusion/objects/...")
            print("                  p4 submit")
            print("    Use a client which has this location in its view")
            LOG_FILE.write("Need to run: p4 delete //.git-fusion/objects/...\n")
            LOG_FILE.write("             p4 submit\n")
        LOG_FILE.close()

if __name__ == "__main__":
    with p4gf_create_p4.Closer():
        main()
