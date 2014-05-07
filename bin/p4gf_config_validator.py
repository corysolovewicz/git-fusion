#! /usr/bin/env python3.3
'''
Validation of Git Fusion configuration files.
'''
import copy
import re
import sys

import P4

import p4gf_branch
import p4gf_config
import p4gf_const
import p4gf_create_p4
from   p4gf_l10n import _, NTR
import p4gf_log

LOG = p4gf_log.for_module()

REMOVE_HYPEN_REGEX = re.compile(r'^("?)-(.*)')
def view_lines_define_empty_view(view_lines):
    """
    Determine whether the views lines define an empty view.
    Returns True if empty.

    It is assumed that the view_lines have passed
    through P4.Map and so have been disambiuated.

    [//depot/a/..., -//depot/...] disambiguates to
    [//depot/a/..., -//depot/a/...]
    Remove matching pairs across inclusions and exclusions.
    Return True if no inclusions remain.
    """

    inclusions = set([])
    exclusions = set([])
    # collect the sets of inclusions and exclusions
    # while stripping the '-' from the exclusions
    for v in view_lines:
        if v.startswith('-') or v.startswith('"-'):
            exclusions.add(re.sub(REMOVE_HYPEN_REGEX, r'\1\2', v))
        else:
            inclusions.add(v)
    # subtract the matching exclusions from inclusions
    # and check if inclusions count > 0
    return  not len(inclusions - exclusions) > 0

def depot_from_view_lhs(lhs):
    """extract depot name from lhs of view line"""
    return re.search('^\"?[+-]?//([^/]+)/.*', lhs).group(1)


class Validator:
    '''A validator for Git Fusion configuration files. It should be used
    as a context manager, using the Python 'with' statement. This avoids
    leaking ConfigParser instances without the need for explicitly invoking
    del or hoping that __del__ will actually work.'''

    def __init__(self):
        self.view_name = None
        self.config_file_path = None
        self.config = None              # Can be None if empty config file.
        self.config_merged = None       # Can be None.
        self.p4 = None
        self.report_count = 0

    @staticmethod
    def from_local_file(view_name, p4, config_file_path):
        '''initialize from local config file'''
        v = Validator()
        v.view_name = view_name
        v.config_file_path = config_file_path
        v.config = p4gf_config.read_file(config_file_path)
        v.config_merged = Validator._combine_repo_with_global(p4, v.config)
        v.p4 = p4
        return v

    @staticmethod
    def from_depot_p4gf_config(view_name, p4):
        ''' initialize from config file stored in depot'''
        v = Validator()
        v.view_name = view_name
        v.config_file_path = p4gf_config.depot_path_repo(view_name)
        # Do _not_ use a shared instance, since we explicitly release it later;
        # instead, get a fresh, unmerged copy for validation purposes.
        v.config = p4gf_config.read_repo(p4, view_name)
        v.config_merged = Validator._combine_repo_with_global(p4, v.config)
        v.p4 = p4
        return v

    @staticmethod
    def from_template_client(view_name, p4, template_client_name):
        ''' initialize from config file stored in depot'''
        v = Validator()
        v.view_name = view_name

        client = p4.run('client', '-o', template_client_name)[0]
        view = client.get('View')
        v.config_file_path = view_name
        v.config = p4gf_config.default_config_repo_for_view(p4, view_name, view)
        v.config_merged = Validator._combine_repo_with_global(p4, v.config)
        v.p4 = p4
        return v

    @staticmethod
    def from_stream(view_name, p4, stream_name):
        ''' initialize from config file stored in depot'''
        v = Validator()
        v.view_name = view_name

        v.config_file_path = view_name
        v.config = p4gf_config.default_config_repo_for_stream(p4, view_name, stream_name)
        v.config_merged = Validator._combine_repo_with_global(p4, v.config)
        v.p4 = p4
        return v

    @staticmethod
    def _combine_repo_with_global(p4, config_repo):
        """Return a new ConfigParser instance that is a copy of config_repo,
        with global repo config values merged in.

        Returns None if config_repo is already None.
        """
        if not config_repo:
            return None
        config_merged = copy.copy(config_repo)
        p4gf_config.combine_repo_with_global(p4, config_repo)
        return config_merged

    def __enter__(self):
        """Enter the runtime context."""
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        """Exit the runtime context."""
        if self.config:
            p4gf_config.clean_up_parser(self.config)
        if self.config_merged:
            p4gf_config.clean_up_parser(self.config_merged)
        return False

    def is_valid(self, enable_mismatched_rhs):
        '''check if config file is valid'''

        # Reject empty config.
        if not self.config:
            self._report_error(_('empty config\n'))
            return False

        # reject sections starting with @ except for @repo
        # like if they put @repos or @Repo instead of @repo
        at_sections = [section for section in self.config.sections()
                       if     section.startswith('@')
                          and section not in p4gf_config.AT_SECTIONS]
        if at_sections:
            self._report_error(_("unexpected section(s): '{}'\n").format("', '".join(at_sections)))
            return False
        # Make sure if a charset specified that it's valid
        if self.config.has_option(p4gf_config.SECTION_REPO, p4gf_config.KEY_CHARSET):
            charset = self.config.get(p4gf_config.SECTION_REPO, p4gf_config.KEY_CHARSET)
            if not self.valid_charset(charset):
                self._report_error(_("invalid charset: '{}'\n").format(charset))
                return False
        # Ensure the change-owner setting is correctly defined
        if self.config.has_option(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                  p4gf_config.KEY_CHANGE_OWNER):
            value = self.config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                    p4gf_config.KEY_CHANGE_OWNER)
            if value != 'author' and value != 'pusher':
                self._report_error(_("repository configuration option '{}' has illegal value\n")
                                   .format(p4gf_config.KEY_CHANGE_OWNER))
                return False
        # Make sure branches are present and properly configured
        if not self._valid_branches(enable_mismatched_rhs):
            return False
        if not self._valid_depots():
            return False
        return True

    @staticmethod
    def valid_charset(charset):
        '''Return True for a valid charset, False for an invalid charset'''
        p4 = p4gf_create_p4.create_p4(connect=False)
        try:
            # setting invalid charset will raise an exception from p4python
            p4.charset = charset
        except P4.P4Exception:
            return False
        return True

    def _report_error(self, msg):
        '''Report error message, including path to offending file'''
        if not self.report_count:
            sys.stderr.write(_("error: invalid configuration file: '{}'\n")
                             .format(self.config_file_path))
            contents = p4gf_config.to_text('', self.config) if self.config else ''
            LOG.debug('config {} contents: ' + contents)
        self.report_count += 1
        LOG.error("Config {} has error: {}".format(self.config_file_path, msg))
        sys.stderr.write(_('error: {}').format(msg))

    #pylint:disable=R0912
    def _valid_branches(self, enable_mismatched_rhs):
        '''
        Check if branch definitions in config file are valid
        '''
        # validation requires use of some settings merged in from the global config
        # for example [@features]
        config = self.config
        config_merged = self.config_merged
        # Does the config contain any branch sections?
        sections = p4gf_config.branch_section_list(config)
        if not sections:
            self._report_error(_('repository configuration missing branch ID\n'))
            return False

        # check branch creation option
        try:
            if config.has_option(p4gf_config.SECTION_REPO,
                                 p4gf_config.KEY_ENABLE_BRANCH_CREATION):
                config.getboolean(p4gf_config.SECTION_REPO,
                                  p4gf_config.KEY_ENABLE_BRANCH_CREATION)
        except ValueError:
            self._report_error(_("repository configuration option '{}' has illegal value\n")
                               .format(p4gf_config.KEY_ENABLE_BRANCH_CREATION))

        # check merge commits option
        try:
            if config.has_option(p4gf_config.SECTION_REPO,
                                 p4gf_config.KEY_ENABLE_MERGE_COMMITS):
                config.getboolean(p4gf_config.SECTION_REPO,
                                  p4gf_config.KEY_ENABLE_MERGE_COMMITS)
        except ValueError:
            self._report_error(_("repository configuration option '{}' has illegal value\n")
                               .format(p4gf_config.KEY_ENABLE_MERGE_COMMITS))

        # Examine them and confirm they have branch views and all RHS match
        enable_mismatched_rhs |= \
            config.has_option(p4gf_config.SECTION_REPO,
                              p4gf_config.KEY_ENABLE_MISMATCHED_RHS) and \
            config.getboolean(p4gf_config.SECTION_REPO,
                              p4gf_config.KEY_ENABLE_MISMATCHED_RHS)
        first_branch = None
        for section in sections:
            try:
                branch = p4gf_branch.Branch.from_config(config_merged, section, self.p4)
            except RuntimeError as e:
                self._report_error("{}\n".format(e))
                return False

            if enable_mismatched_rhs:
                continue

            # check branch for set of view lines which describe an empty view
            # we get the views after passsing through P4.Map's disambiuator
            if view_lines_define_empty_view(branch.view_p4map.lhs()):
                msg = p4gf_const.EMPTY_VIEWS_MSG_TEMPLATE.format(view_name=self.view_name
                    ,view_name_p4client=self.view_name)
                self._report_error(msg)
                return False

            if not first_branch:
                first_branch = branch
            else:
                if not branch.view_p4map.rhs() == first_branch.view_p4map.rhs():
                    msg = _("branch views do not have same right hand sides\n") \
                        + _("view for branch '{}':\n{}\n").format(first_branch.branch_id,
                                                                  first_branch.view_lines) \
                        + _("view for branch '{}':\n{}\n").format(branch.branch_id,
                                                                  branch.view_lines)
                    self._report_error(msg)
                    return False
        return True
    #pylint:enable=R0912

    def _valid_depots(self):
        '''Prohibit remote, spec, and other changelist-impaired depot types.'''
        # Fetch all known Perforce depots.
        depot_list = {depot['name']: depot for depot in self.p4.run('depots')}

        # Scan all configured branches for prohibited depots.
        # use merged config for this to pick up [@features]
        branch_dict     = p4gf_branch.dict_from_config(self.config_merged, self.p4)
        valid           = True
        for branch in branch_dict.values():
            if not branch.view_p4map:
                continue
            v = self._view_valid_depots( depot_list
                                       , branch.branch_id
                                       , branch.view_p4map)
            valid = valid and v
        return valid

    def _view_valid_depots(self, depot_list, branch_id, view_p4map):
        '''Prohibit remote, spec, and other changelist-impaired depot types.'''
        valid = True

        # Extract unique list of referenced depots. Only want to warn about
        # each depot once per branch, even if referred to over and over.
        lhs = view_p4map.lhs()
        referenced_depot_name_list = []
        for line in lhs:
            if line.startswith('-'):
                continue
            depot_name = depot_from_view_lhs(line)
            if not depot_name in referenced_depot_name_list:
                referenced_depot_name_list.append(depot_name)

        # check each referenced depot for problems
        for depot_name in referenced_depot_name_list:
            if depot_name == p4gf_const.P4GF_DEPOT:
                self._report_error(
                    _("branch '{branch_id}':"
                      " Git Fusion internal depot '{depot_name}' not permitted.\n'")
                    .format( branch_id  = branch_id
                           , depot_name = depot_name))
                valid = False
                continue

            if not depot_name in depot_list:
                self._report_error(
                    _("branch '{branch_id}':"
                      " undefined depot '{depot_name}' not permitted.\n'")
                    .format( branch_id  = branch_id
                           , depot_name = depot_name))
                valid = False
                continue

            depot = depot_list[depot_name]
            if depot['type'] not in [NTR('local'), NTR('stream')]:
                self._report_error(
                    _("branch '{branch_id}':"
                      " depot '{depot_name}' type '{depot_type}' not permitted.\n'")
                    .format( branch_id  = branch_id
                           , depot_name = depot_name
                           , depot_type = depot['type']))
                valid = False
                continue

        return valid
