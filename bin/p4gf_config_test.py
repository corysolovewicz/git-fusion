#! /usr/bin/env python3.3
'''
Test hooks for Git Fusion configuration files. Invoked from config_file.t
and used to modify individual options and test shadowing of options
between global and repo configs.

Split out to break cyclic import p4gf_config <-> p4gf_context
'''
import sys

import p4gf_env_config    # pylint: disable=W0611
import p4gf_config
import p4gf_context
from   p4gf_l10n import NTR     # All NTR, no _. No need to translate this test-only file.


def _test_read(repo_name, section, key):
    '''
    Unit test hook to see if we actually read the correct values from
    the correct files.
    '''
    with p4gf_context.create_context(NTR('p4gf_repo'), None) as ctx:
        if repo_name == 'global':
            config = p4gf_config.get_global(ctx.p4gf)
        else:
            config = p4gf_config.get_repo(ctx.p4gf, repo_name)
    if not config.has_section(section):
        print(NTR('section not found: {}').format(section))
    elif not config.has_option(section, key):
        print(NTR('option not found: [{section}] {key}')
             .format(section=section, key=key))
    else:
        value = config.get(section, key)
        print(value)


def _test_read_branch(repo_name, branch, key):
    '''
    Unit test hook to see if we actually read the correct values from
    the correct files.
    '''
    with p4gf_context.create_context(NTR('p4gf_repo'), None) as ctx:
        config = p4gf_config.get_repo(ctx.p4gf, repo_name)
    for section in config.sections():
        if config.has_option(section, p4gf_config.KEY_GIT_BRANCH_NAME) and \
           config.get(section, p4gf_config.KEY_GIT_BRANCH_NAME) == branch:
            _test_read(repo_name, section, key)
            return
    print(NTR('branch not found: {}').format(branch))


def _test_write(repo_name, section, key, value):
    '''
    Unit test hook to see if we actually write the correct values to
    the correct files.
    '''
    if repo_name == 'global':
        print(NTR('write to global config not implemented.'))
        return
    with p4gf_context.create_context(NTR('p4gf_repo'), None) as ctx:
        config = p4gf_config.get_repo(ctx.p4gf, repo_name)
        if not config.has_section(section):
            config.add_section(section)
        config.set(section, key, value)
        p4gf_config.write_repo_if(ctx.p4gf, ctx.client_spec_gf, repo_name, config)


def _test_write_branch(repo_name, branch, key, value):
    '''
    Unit test hook to see if we actually write the correct values to
    the correct files.
    '''
    with p4gf_context.create_context(NTR('p4gf_repo'), None) as ctx:
        config = p4gf_config.get_repo(ctx.p4gf, repo_name)
    for section in config.sections():
        if config.has_option(section, p4gf_config.KEY_GIT_BRANCH_NAME) and \
           config.get(section, p4gf_config.KEY_GIT_BRANCH_NAME) == branch:
            _test_write(repo_name, section, key, value)
            return
    print(NTR('branch not found: {}').format(branch))


def main():
    '''
    Parse the command-line arguments and perform the desired function.
    '''
    # Remove the name of the Python script from the arguments.
    sys.argv.pop(0)
    command   = sys.argv.pop(0)
    repo_name = sys.argv.pop(0)
    section   = sys.argv.pop(0)
    key       = sys.argv.pop(0)
    value     = sys.argv.pop(0) if sys.argv else None

    if command == 'read':
        _test_read(repo_name, section, key)
    elif command == 'write':
        _test_write(repo_name, section, key, value)
    # for read/write-branch, section is actually git-branch-name
    elif command == 'read-branch':
        _test_read_branch(repo_name, section, key)
    elif command == 'write-branch':
        _test_write_branch(repo_name, section, key, value)


if __name__ == "__main__":
    main()
