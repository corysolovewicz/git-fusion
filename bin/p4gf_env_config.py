#! /usr/bin/env python3.3
"""Set Git Fusion environment from optional environment config file named in P4GF_ENV."""
import sys
import os
import configparser
from subprocess import check_output, STDOUT, CalledProcessError
import p4gf_bootstrap  # pylint: disable=W0611
import p4gf_const
from   p4gf_l10n    import _, NTR
import p4gf_log
from p4gf_missing_config_path import MissingConfigPath

# This module is intended to execute prior to any main() statements.
# This is necessary to configure the process environment at
# the earliest opportunity from the P4GF_ENV named config file.
# We accomplish this by including this module at the top of all
# python scripts which run as main().
# For example:
#        import p4gf_env_config  # pylint: disable=W0611
# The including module will make no references to this module,
# thus we disable W0611 - 'Unused import'

# list of prohibited vars - will raise error
# rather than be ignored to prevent failing to enforce user's expected behavior
Prohibited_vars = [NTR('PATH'), NTR('LANG'), NTR('P4CONFIG')]
Required_vars = [NTR('P4GF_HOME'), NTR('P4PORT')] # list of required config items

Unset = NTR('unset')   # value to cause ENV key to be unset (case insensitive test)
# from p4 help environment
P4_vars = [
    NTR('P4CHARSET'),                 # Client's local character set
    NTR('P4COMMANDCHARSET'),          # Client's local character set (for command line operations)
    NTR('P4CLIENT'),                  # Name of client workspace
    NTR('P4CLIENTPATH'),              # Directories client can access
    NTR('P4CONFIG'),                  # Name of configuration file
    NTR('P4DIFF'),                    # Diff program to use on client
    NTR('P4DIFFUNICODE'),             # Diff program to use on client
    NTR('P4EDITOR'),                  # Editor invoked by p4 commands
    NTR('P4HOST'),                    # Name of host computer
    NTR('P4IGNORE'),                  # Name of ignore file
    NTR('P4LANGUAGE'),                # Language for text messages
    NTR('P4LOGINSSO'),                # Client side credentials script
    NTR('P4MERGE'),                   # Merge program to use on client
    NTR('P4MERGEUNICODE'),            # Merge program to use on client
    NTR('P4PAGER'),                   # Pager for 'p4 resolve' output
    NTR('P4PASSWD'),                  # User password passed to server
    NTR('P4PORT'),                    # Port to which client connects
    NTR('P4SSLDIR'),                  # SSL server credential director
    NTR('P4TICKETS'),                 # Location of tickets file
    NTR('P4TRUST'),                   # Location of ssl trust file
    NTR('P4USER')]                    # Perforce user name

# We wish to log config file processing during load of this module.
# This is before __ main __ instantiates the ExceptionLogger
# So explicitly configure logging
# pylint: disable=W0212
# Access to a protected member
p4gf_log._lazy_init()
# pylint: enable=W0212

LOG                    = p4gf_log.for_module()
_configured            = False  # python loads only once, but nevertheless set on load

# pylint: disable=R0201
# method could be a function  .. yeah .. but let's place these in the class
class EnvironmentConfig:
    '''Set the os.environ from a config file named in P4GF_ENV.
    '''
    def __init__(self):
        self.p4_vars = []                               # P4vars explicitly set byconfigfile
        if p4gf_const.P4GF_ENV_NAME in os.environ:      # GF's copy - defaults to None
            p4gf_const.P4GF_ENV = os.environ[p4gf_const.P4GF_ENV_NAME]

    def log_gf_env(self):
        '''Log the resulting Git Fusion environment.'''
        LOG.info("Git Fusion P4GF_HOME = {0}".format(p4gf_const.P4GF_HOME))
        LOG.info("Git Fusion GIT_BIN = {0}".format(p4gf_const.GIT_BIN))
        # Log GIT_BIN
        try:
            git_path = check_output(['which', p4gf_const.GIT_BIN], stderr=STDOUT)
        except CalledProcessError:
            msg = "Cannot find git at {0}".format(p4gf_const.GIT_BIN)
            LOG.error(msg)
            raise RuntimeError(_(msg))

        git_path = git_path.decode().strip()
        git_version = check_output(
                [git_path, '--version'], stderr=STDOUT).decode().strip()
        LOG.info("Git Fusion is configured for git: " + git_path  + "  "  + git_version)

        for var in P4_vars:
            if var in os.environ:
                LOG.info("Git Fusion P4 vars in environment: {0} = {1}".
                        format(var, os.environ[var]))

    def unset_environment(self, env_vars):
        '''Unset the environment variables named in the string/list.'''
        if isinstance(env_vars, str):
            evars = [env_vars]
        else:
            evars = env_vars

        for var in evars:
            if var in os.environ:
                del os.environ[var]
                LOG.info("Unsetting environment var {0}".format(var))

    def set_gf_environment(self):
        '''Set the os.environ from a config file named in P4GF_ENV.
        If the var is set but the value is not a file, raise error.
        '''
        global _configured
        if not _configured:
            if not p4gf_const.P4GF_ENV:
                self.set_gf_environ_from_environment()
            else:
                raise_if_not_absolute_file(p4gf_const.P4GF_ENV, p4gf_const.P4GF_ENV_NAME)
                self.set_gf_environ_from_config()
                self.version_p4gf_env_config()
            self.log_gf_env()
            _configured = True

    def check_required(self, key):
        '''Remove eligible key from required list .'''
        # pylint: disable=W0602
        # Using global for 'Required_vars' but no assignment is done
        # pylint fails to detect the remove as causing a modification
        global Required_vars
        if key in Required_vars:
            Required_vars.remove(key)


    def check_prohibited(self, key):
        '''Raise error if key is in prohibited list.'''
        if key in Prohibited_vars:
            msg = "Git Fusion environment: config_file {0} :" + \
                  " {1} may not be set in this config file." \
                      .format(p4gf_const.P4GF_ENV, key)
            LOG.error(msg)
            raise RuntimeError(_(msg))


    def set_gf_environ_from_environment(self):
        '''Use the inherited environment and the Git Fusion default GFHOME.'''
        # Default behavior
        LOG.info("P4GF_ENV not set. Using default environment.")

    def set_gf_environ_from_config(self):
        '''Load the Git Fusion environment config file and
        set the os.environ from its values.'''
        # pylint: disable=R0915, R0912
        #  Too many statements
        #  Too many branches
        p4_vars_in_config = []
        config_path = p4gf_const.P4GF_ENV
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        try:
            config.read(config_path)
        except configparser.Error as e:
            msg = "Unable to read Git Fusion environment config file '{0}'.\n{1}" \
                  .format(config_path, e)
            LOG.error(msg)
            raise RuntimeError(_(msg))

        if config.has_section(p4gf_const.SECTION_ENVIRONMENT):
            p4gf_config_dict = dict(config.items(p4gf_const.SECTION_ENVIRONMENT))
            for key, val in p4gf_config_dict.items():
                value = val
                if NTR('#') in value:
                    value = value[:value.index(NTR('#'))].rstrip()
                value = value.strip(NTR("'")).strip(NTR('"')).rstrip(NTR('/'))
                if key == p4gf_const.P4GF_HOME_NAME:
                    p4gf_const.P4GF_HOME = value
                    p4gf_const.P4GF_DIR  = os.path.basename(p4gf_const.P4GF_HOME)
                    LOG.info("setting P4GF_HOME {0}  P4GF_DIR {1}".
                            format(p4gf_const.P4GF_HOME, p4gf_const.P4GF_DIR))
                    raise_if_not_absolute_dir(p4gf_const.P4GF_HOME, p4gf_const.P4GF_HOME_NAME)
                    self.check_required(key)
                    continue
                if key == p4gf_const.GIT_BIN_NAME:
                    p4gf_const.GIT_BIN = value
                    if value != p4gf_const.GIT_BIN_DEFAULT:
                        raise_if_not_exe_path(p4gf_const.GIT_BIN, p4gf_const.GIT_BIN_NAME)
                    self.check_required(key)
                    continue
                self.check_prohibited(key)
                if value.lower() == Unset :     # permit unset
                    del os.environ[key]
                    LOG.info("unsetting shell var {0}".format(key))
                    continue
                os.environ[key] = value
                LOG.info("setting shell var {0} = {1}".format(key, value))
                if key.startswith(NTR('P4')):
                    p4_vars_in_config.append(key)
                self.check_required(key)
            LOG.info("P4GF_ENV setting P4 vars: {0}   :\n  any missing required items? {1}".
                    format(p4_vars_in_config, Required_vars))
            # Unset any P4 vars not set in the config file.
            # This will always delete P4CONFIG
            self.unset_environment(set(P4_vars) - set(p4_vars_in_config))
            if len(Required_vars):
                msg = "Git Fusion environment: config file {0} is missing required item(s): {1}." \
                          .format(config_path, Required_vars)
                LOG.error(msg)
                raise RuntimeError(_(msg))
            LOG.info("Setting environment from config file {0}.".format(config_path))
            LOG.info("        config file settings:  {0}".format(p4gf_config_dict))
        else:
            msg = "Git Fusion environment: config file '{0}' has no 'environment' section." \
                      .format(config_path)
            LOG.error(msg)
            raise RuntimeError(_(msg))

    def version_p4gf_env_config(self):
        '''If the user defined P4GF_ENV file has changed then save it to Perforce.'''
        pass


def raise_if_not_exe_path(fpath, pseudonym):
    '''Raise if not absolute, exists, and executable.'''
    if (os.path.isabs(fpath)
             and os.path.isfile(fpath)
             and os.access(fpath, os.X_OK)):
        return
    msg = "Git Fusion environment: config_file {0} :\n".format(p4gf_const.P4GF_ENV) + \
          " '{0}' path is relative, missing, or not executable: {1}." \
              .format(pseudonym, fpath)
    LOG.error(msg)
    raise RuntimeError(_(msg))

def raise_if_not_absolute_file(fpath, pseudonym):
    '''Raise if not absolute and exists.'''
    # Raise this named Exception for use only by p4gf_super_init.py
    if not os.path.exists(fpath):
        raise  MissingConfigPath(fpath)
    if os.path.isabs(fpath) and os.path.isfile(fpath):
        return
    msg = "Git Fusion environment: invalid path {0} in {1}:\n" + \
           "is not an existing absolute file.".format(fpath, pseudonym)
    LOG.error(msg)
    raise RuntimeError(_(msg))

def raise_if_not_absolute_dir(dpath, pseudonym):
    '''Raise if not absolute, exists, and is dir.'''
    if os.path.isabs(dpath) and os.path.isdir(dpath):
        return
    msg = "Git Fusion environment: config_file {0} :\n".format(p4gf_const.P4GF_ENV) + \
          " {0} path is not an existing absolute directory path: {1}." \
              .format(pseudonym, dpath)
    LOG.error(msg)
    raise RuntimeError(_(msg))

def get_main_module():
    '''Return the name of the module which defined the __main__ module'''
    module = str(sys.modules[NTR('__main__')])
    idx = module.find(NTR('from')) + 4
    return module[idx:]

LOG.info('p4gf_env_config imported by {0}'.format(get_main_module()))
Env_config = EnvironmentConfig()
Env_config.set_gf_environment()

