#! /usr/bin/env python3.3
'''
Command-line script to copy from Perforce to Git Fusion's internal repo.

Invokes code from the same script (p4gf_auth_server.py) that normal Git clients
invoke when they connect to Git Fusion over sshd, but passes "poll_only=True"
to suppress 'git pull' permission check or call to original git-upload-pack.
'''
import os
import sys

import p4gf_env_config    # pylint: disable=W0611
import p4gf_auth_server
import p4gf_const
import p4gf_create_p4
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_log
import p4gf_util
import p4gf_version
import p4gf_view_dirs

LOG = p4gf_log.for_module()

def _list_for_server():
    '''
    Return list of repos that have been copied to the given Git Fusion
    server.

    "have been copied" here means "has a .git-fusion/views/<view_name>/
    directory on this server."
    '''
    p4 = p4gf_create_p4.create_p4(client=p4gf_util.get_object_client_name())
    result = []
    p4gf_dir = p4gf_util.p4_to_p4gf_dir(p4)

    for view_name in p4gf_util.view_list(p4):
        view_dirs = p4gf_view_dirs.from_p4gf_dir(p4gf_dir, view_name)
        if os.path.exists(view_dirs.GIT_DIR):
            result.append(view_name)
    p4gf_create_p4.destroy(p4)
    return result

def main():
    '''
    Invoke p4gf_auth_server as if we're responding to a 'git pull'.
    '''
    # Set up argument parsing.
    parser = p4gf_util.create_arg_parser(
        _("Update Git Fusion's internal repo(s) with recent changes from Perforce."))
    parser.add_argument('-a', '--all', action=NTR('store_true'),
                        help=_('Update all repos'))
    parser.add_argument(NTR('views'), metavar=NTR('view'), nargs='*',
                        help=_('name of view to update'))
    args = parser.parse_args()

    # Check that either --all, --gc, or 'views' was specified.
    if not args.all and len(args.views) == 0:
        sys.stderr.write(_('Missing view names; try adding --all option.\n'))
        sys.exit(2)

    view_list = _list_for_server()
    if not args.all:
        bad_views = [x for x in args.views if x not in view_list]
        if bad_views:
            sys.stderr.write(_('One or more views are not defined on this server:\n\t'))
            sys.stderr.write('\n\t'.join(bad_views))
            sys.stderr.write('\n')
            sys.stderr.write(_('Defined views:\n\t'))
            sys.stderr.write('\n\t'.join(view_list))
            sys.stderr.write('\n')
            sys.exit(2)
        view_list = args.views

    for view_name in view_list:
        sys.argv = [ 'p4gf_auth_server.py'
                   , '--user={}'.format(p4gf_const.P4GF_USER)
                   , 'git-upload-pack'
                   , view_name]
        p4gf_auth_server.main(poll_only=True)

if __name__ == "__main__":
    # Ensure any errors occurring in the setup are sent to stderr, while the
    # code below directs them to stderr once rather than twice.
    try:
        # thwart pylint identical code detection with two empty lines below
        with p4gf_log.ExceptionLogger(squelch=False, write_to_stderr_=True):
            p4gf_log.record_argv()
            p4gf_version.log_version()
            log_l10n()
            p4gf_version.version_check()
    # pylint: disable=W0702
    except:
        # Cannot continue if above code failed.
        exit(1)

    # main() already writes errors to stderr, so don't let logger do it again
    p4gf_log.run_with_exception_logger(main, write_to_stderr=False)
