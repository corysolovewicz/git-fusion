#! /usr/bin/env python3.3
'''
A Git<->Perforce branch association.

Mostly just a wrapper for one section of a repo config file.
'''
import copy
import logging
import P4

import p4gf_config
from   p4gf_l10n import _, NTR
import p4gf_path
import p4gf_util

LOG = logging.getLogger(__name__)

class Branch:
    '''
    A Git<->Perforce branch association.

    Mostly just a wrapper for one section of a repo config file.

    There are two types of branches: classic and stream

    For a classic branch, stream_name and writable_stream_name will be None
    and view_lines and view_p4map will always be set.

    For a stream branch, stream_name and writable_stream_name will be set but
    view_lines and view_p4map will initially be unset, as that requires
    running 'p4 stream -ov' to get the stream's view.

    view_lines and view_p4map may list their RHS as either relative to some
    root ('//depot/... ...') or when used in p4gf_context, already associated
    with a client ('//depot/... //p4gf_myrepo/...').
    '''
    def __init__(self):
        self.branch_id          = None  # Section name.
                                        # Must be unique for this repo.
                                        #
                                        # Can be None: from_branch() creates
                                        # temporary branch view instances that
                                        # never go into ctx.branch_dict() or any
                                        # config file.

        self.git_branch_name    = None  # Git ref name, minus "refs/heads/".
                                        # None for anonymous branches.

                                        # This branch's view into the
                                        # P4 depot hierarchy.
                                        #
                                        # Both can be None for new lightweight
                                        # branches for which we've not yet
                                        # created a view.
                                        #
        self.view_lines         = None  # as list of strings
        self.view_p4map         = None  # view_lines as P4.Map
        self.stream_name        = None  # for stream branches, the stream this
                                        # branch is connected to
        self.original_view_lines = None # snapshot of stream's view at the time
                                        # the branch was created
        self.writable_stream_name = None# for stream branches, the name of the
                                        # stream that is writable on this branch
                                        # either the stream itself or its
                                        # baseParent for virtual streams
        self.depot_branch       = None  # DepotBranchInfo where we store files
                                        # if we're lightweight.
                                        #
                                        # None if fully populated OR if not
                                        # yet set.
                                        #
                                        # Reading from config? Starts out as
                                        # just a str depot_branch_id. Someone
                                        # else (Context) must convert to full
                                        # DepotBranchInfo pointer once that list
                                        # has been loaded.

        self.deleted            = False # Latches true when git deletes a lightweight task branch

        # -- begin data not stored in config files ----------------------------

        self.is_lightweight     = False # Is this a branch that stores only
                                        # changed files in Perforce?

        self.is_new             = None  # Was this branch view created during
                                        # the current pull/push?
                                        # None  ==> Not sure yet
                                        # False ==> We know it's not new
                                        # True  ==> We know it's new

        self.is_dbi_partial     = False # Set True when we set when defining a
                                        # new depot branch, if we suspect our
                                        # parent or fully populated basis is
                                        # empty because it is not yet submitted
                                        # to Perforce.
                                        #
                                        # Cleared back to False when we set
                                        # dbi's parent, or know for sure that it
                                        # doesn't have one.

        self.populated          = None  # Latches True or False during is_populated()
                                        ### (was latched in p4gf_jit
                                        ### .populate_first_commit_on_current_branch() )

        self.more_equal         = False # Is this the first branch view listed
                                        # in p4gf_config? This is the view that
                                        # we use for HEAD. Usually this is
                                        # 'master', but not always.

    #pylint:disable=R0912,R0915
    @staticmethod
    def from_config(config, branch_id, p4=None):
        '''
        Factory to seed from a config file.

        Returns None if config file lacks a complete and correct branch
        definition.
        '''

        is_deleted = False
        if config.has_option(branch_id,
                p4gf_config.KEY_GIT_BRANCH_DELETED):
            is_deleted = config.getboolean(branch_id,
                p4gf_config.KEY_GIT_BRANCH_DELETED)

        result = Branch()
        result.branch_id = branch_id
        branch_config = config[branch_id]
        result.git_branch_name = branch_config.get(p4gf_config.KEY_GIT_BRANCH_NAME)
        result.depot_branch = branch_config.get(p4gf_config.KEY_DEPOT_BRANCH_ID)
        result.deleted = is_deleted
        if p4gf_config.KEY_STREAM in branch_config and p4gf_config.KEY_VIEW in branch_config:
            f = _("repository configuration section [{}] may not contain both"
                  " 'view' and 'stream'")
            raise RuntimeError(f.format(branch_id))
        if p4gf_config.KEY_STREAM in branch_config:
            result.stream_name = branch_config.get(p4gf_config.KEY_STREAM)
            stream = p4gf_util.p4run_logged(p4, ['stream', '-ov', result.stream_name])
            LOG.debug("stream for branch:\n{}\n".format(stream))
            if not 'View' in stream[0]:
                f = _("repository configuration section [{}] '{}' does not refer"
                      " to a valid stream")
                raise RuntimeError(f.format(branch_id, result.stream_name))
            if stream[0]['Type'] == 'task':
                f = _("repository configuration section [{}] '{}' refers to a task stream")
                raise RuntimeError(f.format(branch_id, result.stream_name))
            if stream[0]['Type'] == 'virtual':
                result.writable_stream_name = stream[0]['baseParent']
            else:
                result.writable_stream_name = result.stream_name
            view_lines = stream[0]['View']
            LOG.debug("View lines:\n{}\n".format(view_lines))
            # if this is a config2, stream branches will have stored
            # a snapshot of the stream's view at branch create time
            if p4gf_config.KEY_ORIGINAL_VIEW in branch_config:
                original_view_lines = branch_config.get(p4gf_config.KEY_ORIGINAL_VIEW)
                if isinstance(original_view_lines, str):
                    original_view_lines = original_view_lines.splitlines()
                # Common: first line blank, view starts on second line.
                if original_view_lines and not len(original_view_lines[0].strip()):
                    del original_view_lines[0]
                result.original_view_lines = original_view_lines
        else:
            view_lines = branch_config.get(p4gf_config.KEY_VIEW)
            if isinstance(view_lines, str):
                view_lines = view_lines.splitlines()
            # Common: first line blank, view starts on second line.
            if view_lines and not len(view_lines[0].strip()):
                del view_lines[0]


        LOG.debug2("view_lines={}".format(view_lines))
        if not view_lines:
            return None

        if isinstance(view_lines, str):
            view_lines = view_lines.replace('\t', ' ')
        elif isinstance(view_lines, list):
            view_lines = [ln.replace('\t', ' ') for ln in view_lines]
        result.view_p4map = P4.Map(view_lines)
        result.view_lines = view_lines

        return result
    #pylint:enable=R0912

    @staticmethod
    def from_branch(branch, new_branch_id):
        '''
        Return a new Branch instance, with values copied from another.
        '''
        r = copy.copy(branch)
        r.branch_id = new_branch_id
        return r

    def add_to_config(self, config):
        '''
        Create a section with this Branch object's data.

        This is used to add lightweight and stream-based branches to
        the p4gf_config2 file.
        '''
        section = self.branch_id
        config.add_section(section)
        if self.git_branch_name:
            config[section][p4gf_config.KEY_GIT_BRANCH_NAME] = self.git_branch_name
        if self.stream_name:
            config[section][p4gf_config.KEY_STREAM] = self.stream_name
        if self.view_lines:
            stripped_lines = p4gf_config.convert_view_to_no_client_name(self.view_lines)
            # for stream-based branches, we're saving a snapshot of the view
            # at the time of branch creation to enable mutation detection
            # later on.
            if self.stream_name:
                config[section][p4gf_config.KEY_ORIGINAL_VIEW] = stripped_lines
            else:
                config[section][p4gf_config.KEY_VIEW] = stripped_lines
        # Set this only if deleted is True
        if self.deleted:
            config[section][p4gf_config.KEY_GIT_BRANCH_DELETED] = NTR('True')
        if self.depot_branch:
            if isinstance(self.depot_branch, str):
                config[section][p4gf_config.KEY_DEPOT_BRANCH_ID] = self.depot_branch
            else:
                config[section][p4gf_config.KEY_DEPOT_BRANCH_ID] = self.depot_branch.depot_branch_id

    def intersects_p4changelist(self, p4changelist):
        '''
        Does any file in the given P4Changelist object intersect our branch's
        view into the Perforce depot hierarchy?
        '''
        if LOG.isEnabledFor(logging.DEBUG3):
            def _loggit(intersects, path):
                '''Noisy logging dumpage.'''
                LOG.debug3('branch_id={br} intersect={i} change={cl} path={pa} view={vw}'
                           .format( br = self.branch_id[:7]
                                  , i  = intersects
                                  , cl = p4changelist.change
                                  , pa = path
                                  , vw = '\n'.join(self.view_lines)))
            loggit = _loggit
        else:
            loggit = None

        # Do NOT optimize by checking p4changelist.path against view and
        # early-returning False if path is not in our view. Path might be
        # something really high up like //... or //depot/... for changelists
        # that straddle multiple branches, and False here would miss that
        # changelist's intersection with our view. Thank you
        # push_multi_branch.t for catching this.

        for p4file in p4changelist.files:
            if self.view_p4map.includes(p4file.depot_path):
                if loggit:
                    loggit(True, p4file.depot_path)
                return True

        if loggit:
            loggit(False, NTR('any depot_path'))
        return False

    def intersects_depot_file_list(self, depot_file_list):
        '''
        Does any depotFile in the given list intersect our branch view?
        '''
        for depot_file in depot_file_list:
            if self.view_p4map.includes(depot_file):
                return True
        return False

    def intersects_depot_path(self, depot_path):
        '''
        Does a depot path intersect our branch view?
        '''
        if self.view_p4map.includes(depot_path):
            return True
        return False

    def __repr__(self):
        lines = [ '[{}]'.format(self.branch_id)
                , 'git-branch-name = {}'.format(self.git_branch_name)
                , 'is_lightweight = {}'.format(self.is_lightweight)
                , 'deleted = {}'.format(self.deleted)
                , 'view =\n\t{}'.format(self.view_lines) ]
        if self.stream_name:
            lines.append('stream = {}'.format(self.stream_name))
            lines.append('writable_stream = {}'.format(self.writable_stream_name))
        if self.view_p4map:
            lines.append('p4map = {}'.format(self.view_p4map))
        return '\n'.join(lines)

    def to_log(self, logger=LOG):
        '''
        Return a representation suitable for the logger's level.

        If DEBUG  or less, return only our branch_id, abbreviated to 7 chars.
        If DEBUG2 or more, dump a full representation.
        '''
        if logger.isEnabledFor(logging.DEBUG2):
            return self.__repr__()
        return self.branch_id[:7]

    def set_rhs_client(self, client_name):
        '''
        Convert a view mapping's right-hand-side from its original client
        name to a new client name:

            //depot/dir/...  dir/...
            //depot/durr/... durr/...

        becomes

            //depot/dir/...  //client/dir/...
            //depot/durr/... //client/durr/...
        '''
        self.view_p4map = convert_view_from_no_client_name( self.view_p4map
                                                          , client_name )
        self.view_lines = self.view_p4map.as_array()

    def set_depot_branch(self, new_depot_branch):
        '''
        Replace any previous depot_branch with new_depot_branch (None okay).

        Calculate a new view mapping with our old branch root replaced by
        the new branch root.
        '''
        old_root = _depot_root(self.depot_branch)
        new_root = _depot_root(new_depot_branch)

        if self.view_p4map:
            new_p4map = p4gf_util.create_p4map_replace_lhs_root( self.view_p4map
                                                               , old_root
                                                               , new_root )
            new_lines = new_p4map.as_array()

        else:   # Nothing to change.
            new_p4map = self.view_p4map
            new_lines = self.view_lines

        self.depot_branch = new_depot_branch
        self.view_p4map   = new_p4map
        self.view_lines   = new_lines

    def fully_populated_view_p4map(self):
        '''
        Return a P4.Map instance that lists our view onto the fully populated depot.

        If we're already a fully populated branch, returns or own P4.Map
        instance.

        If we're lightweight, returns a new P4.Map that looks like our
        lightweight view, re-rooted to //.
        '''
        if not self.is_lightweight:
            return self.view_p4map

        old_root = _depot_root(self.depot_branch)
        new_root = '//'

        return p4gf_util.create_p4map_replace_lhs_root( self.view_p4map
                                                      , old_root
                                                      , new_root )

    def find_fully_populated_change_num(self, ctx):
        '''
        Return the changelist number from which this branch first diverged
        from fully populated Perforce.

        If this branch IS fully populated Perforce, return None.
        '''
        if not self.is_lightweight:
            return None

        return ctx.depot_branch_info_index()\
            .find_fully_populated_change_num(self.get_or_find_depot_branch(ctx))

    def is_populated(self, ctx):
        '''
        Does this branch have at least one changelist?
        '''
        if self.populated == None:
            with ctx.switched_to_branch(self):
                r = ctx.p4run(['changes', '-m1', ctx.client_view_path()])
            if r:
                self.populated = True
            else:
                self.populated = False
        return self.populated

    def find_depot_branch(self, ctx):
        '''
        Scan through known DepotBranchInfo until we find one whose root
        contains the first line of our view.
        '''
        lhs0 = p4gf_path.dequote(self.view_p4map.lhs()[0])
        depot_branch = ctx.depot_branch_info_index().find_depot_path(lhs0)
        return depot_branch

    def get_or_find_depot_branch(self, ctx):
        '''
        If we already know our depot branch, return it.
        If not, go find it, remember it, return it.
        '''
        if not self.depot_branch:
            self.depot_branch = self.find_depot_branch(ctx)
        return self.depot_branch

    def is_ancestor_of_lt(self, ctx, child_branch):
        '''
        Is our depot_branch an ancestor of lightweight
        child_branch's depot_branch?

        Strict: X is not its own ancestor.
        '''
        if not child_branch.is_lightweight:
            return False

        our_depot_branch   = self.get_or_find_depot_branch(ctx)
        child_depot_branch = child_branch.get_or_find_depot_branch(ctx)
        # Strict: X is not its own ancestor.
        if our_depot_branch == child_depot_branch:
            return False
        # +++ None ==> Fully populated Perforce,
        # +++ always ancestor of any lightweight branch.
        if not our_depot_branch:
            return True

        # Must walk lightweight child's ancestry tree,
        # looking for our depot branch.
        cl_num = ctx.depot_branch_info_index().find_ancestor_change_num(
                                        child_depot_branch, our_depot_branch)
        return True if cl_num else False

    def p4_files(self, ctx, at_change = NTR('now')):
        '''
        Run 'p4 files //client/...@change' and return the result.

        If this is a lightweight branch run the command TWICE, once for our
        lightweight view, and then again for fully populated Perforce at
        whatever changelist we diverged from fully populated Perforce.
        Return the merged results.

        Inserts 'clientFile' values for each p4 file dict because we have it
        handy and we understand "inherited from fully populated Perforce" better
        than code that calls us.

        Switches the client view to ancestor and back.
        '''
        our_files = []
        with ctx.switched_to_branch(self):
            path_at = ctx.client_view_path(at_change)
            if at_change != 'now':
                r = ctx.branch_files_cache.files_at( ctx        = ctx
                                                   , branch     = self
                                                   , change_num = at_change )
            else:
                r = ctx.p4run(['files', path_at])
            for rr in r:
                if isinstance(rr, dict) and 'depotFile' in rr:
                    rr['clientFile'] = self.view_p4map.translate(rr['depotFile'])
                    our_files.append(rr)

        if not self.is_lightweight:
            return our_files

        fp_change_num   = self.find_fully_populated_change_num(ctx)
        fp_files        = []
        if fp_change_num:
            fp_p4map        = self.fully_populated_view_p4map()
            with ctx.switched_to_view_lines(fp_p4map.as_array()):
                path_at         = ctx.client_view_path(fp_change_num)
                fp_files_result = ctx.p4run(['files', path_at])

            # Keep only the fully populated paths not replaced by
            # a file in our own branch.
            # +++ Hash by client path.
            our_client_path_list = [x['clientFile'] for x in our_files]
            for rr in fp_files_result:
                if isinstance(rr, dict) and 'depotFile' in rr:
                    client_path = fp_p4map.translate(rr['depotFile'])
                    if not client_path in our_client_path_list:
                        rr['clientFile'] = client_path
                        fp_files.append(rr)

        return our_files + fp_files

    def copy_rerooted(self, new_depot_info):
        '''
        Return a new Branch object with a view like source_branch's, but with
        source_branch's LHS depot path roots changed from old_depot_info
        to new_depot_info.

        Assigns no branch_id. Do this yourself if you deem this branch worthy.
        '''
        r = Branch.from_branch(self, None)
        r.set_depot_branch(new_depot_info)
        r.more_equal = False
        return r

    def sha1_for_branch(self):
        '''
        Convenience wrapper for branch_name -> sha1
        '''
        if not self.git_branch_name:
            return None
        return p4gf_util.sha1_for_branch(self.git_branch_name)


# -- end class Branch ---------------------------------------------------------

def _depot_root(depot_branch_info):
    '''
    Return the root portion of the depot paths in a Branch view's RHS.

    Include trailing delimiter for easier str.startswith()/replace() work.
    '''
    if depot_branch_info:
        return depot_branch_info.root_depot_path + '/'
    else:
        return '//'

def depot_branch_to_branch_view_list(branch_dict, depot_branch):
    '''
    Return all known Branch view instances that use depot_branch to store
    their data.
    '''
    return [branch for branch in branch_dict.values()
            if branch.depot_branch == depot_branch]

def dict_from_config(config, p4=None):
    '''
    Factory to return a new dict of branch_id ==> Branch instance,
    one for each branch defined in config.

    Have a Context handy? Then use Context.branch_dict():
    you already have this branch dict and do not need a new one.
    '''
    result = {}
    for branch_id in p4gf_config.branch_section_list(config):
        branch = Branch.from_config(config, branch_id, p4)
        if branch:
            result[branch_id] = branch
    return result


def _lhs_to_relative_rhs(lhs):
    '''
    Turn "//depot/..." into "depot/...".

    Honors quotes '"' and + (as long as the + prece

    Does not honor - because YAGNI.
    '''
    rhs = lhs

    quoted = False
    if rhs.startswith('"'):     # Temporarily remove leading quotes.
        quoted = True
        rhs = rhs[1:]
    if rhs.startswith('+'):     # Omit overlay/plus-mapping marker.
        rhs = rhs[1:]
        if rhs.startswith('"'):     # Temporarily remove quotes after +
            quoted = True
            rhs = rhs[1:]
    if rhs.startswith('//'):    # Absolute // becomes relative ''
        rhs = rhs[2:]

    if quoted:                  # Restore leading quotes.
        rhs = '"' + rhs
    return rhs


def _branch_view_union_p4map_one(p4map, branch):
    '''
    Accumulate one branch's view in the map
    '''
    if not branch.view_lines:
        return
    branch_p4map = P4.Map()
    for line in branch.view_lines:
        # Skip exclusion lines.
        if line.startswith('-') or line.startswith('"-'):
            continue
        # Flatten overlay lines, remove leading +
        if line.startswith('+'):
            line = line[1:]
        elif line.startswith('"+'):
            line = '"' + line[2:]

        branch_p4map.insert(line)

    # Replace branch view's RHS (client side) with a copy of its LHS
    # (depot side) so that each depot path "//depot/foo" maps to a client
    # path "depot/foo". This new RHS allows us un-exclude
    # P4.Map-generated minus/exclusion lines that P4.Map had to insert
    # into branch_p4map when multiple LHS collided on the same RHS.
    lhs = branch_p4map.lhs()
    rhs = [_lhs_to_relative_rhs(l) for l in lhs]

    for (ll, rr) in zip(lhs, rhs):
        if ll.startswith('-') or ll.startswith('"-'):
            continue
        p4map.insert(ll, rr)


def _branch_view_union_p4map(client_name, branch_dict):
    '''
    Return a P4.Map object that contains the union of ALL branch views defined
    in branch_dict.

    Exclusion lines from the config file are NOT included in this P4.Map: you
    cannot easily add those to a multi-view P4.Map without unintentionally
    excluding valid files from previous views.

    RHS of view map is programmatically generated nonsense.

    Returned P4.Map _will_ include exclusion lines. These are inserted by
    P4.Map itself as overlapping views are layered on top of each other.
    That's okay.
    '''
    p4map = P4.Map()
    for br in branch_dict.values():
        _branch_view_union_p4map_one(p4map, br)

    # _branch_view_union_p4map_one() generates a NEW RHS that lacks
    # a //client/ prefix. Insert one now.
    p4map_cvted = convert_view_from_no_client_name(p4map, client_name)
    return p4map_cvted


def calc_branch_union_client_view(client_name, branch_dict):
    '''
    Do most of the prep work for loading a "union of all branches" into a
    client view map, without actually changing the client spec or anything
    else.

    Calculate a view that maps in all of the branches in branch_dict,
    and set that view's RHS to use the given client spec name.

    Return result as a P4.Map
    '''
    return _branch_view_union_p4map(client_name, branch_dict)


def convert_view_from_no_client_name(view, new_client_name):
    '''
    Convert a view mapping's right-hand-side from its original client
    name to a new client name:

        //depot/dir/...  dir/...
        //depot/durr/... durr/...

    becomes

        //depot/dir/...  //client/dir/...
        //depot/durr/... //client/durr/...

    Accepts view as P4.Map, str. or list.
    Returns view as P4.Map().
    '''
    if isinstance(view, P4.Map):
        old_map = view
    elif isinstance(view, str):
        view_lines = view.splitlines()
        old_map = P4.Map(view_lines)
    else:
        view_lines = view
        old_map = P4.Map(view_lines)

    lhs = old_map.lhs()
    new_prefix = '//{}/'.format(new_client_name)
    rhs = [new_prefix + p4gf_path.dequote(r) for r in old_map.rhs()]
    new_map = P4.Map()
    for (l, r) in zip(lhs, rhs):
        new_map.insert(l, r)

    return new_map


def replace_client_name(view, old_client_name, new_client_name):
    '''
    Convert "//depot/... //old_client/..." to "//depot/... //new_client"

    Accepts view as P4.Map, str. or list.
    Returns view as P4.Map().
    '''
    if isinstance(view, P4.Map):
        old_map = view
    elif isinstance(view, str):
        view_lines = view.splitlines()
        old_map = P4.Map(view_lines)
    else:
        view_lines = view
        old_map = P4.Map(view_lines)

    lhs = old_map.lhs()
    new_prefix = '//{}/'.format(new_client_name)
    old_prefix = '//{}/'.format(old_client_name)
    old_len    = len(old_prefix)
    rhs = [new_prefix + p4gf_path.dequote(r)[old_len:] for r in old_map.rhs()]
    new_map = P4.Map()
    for (l, r) in zip(lhs, rhs):
        new_map.insert(l, r)

    return new_map


def iter_fp(branch_dict):
    '''
    Iterate through all the fully populated branch definitions.
    '''
    for branch in branch_dict.values():
        if not branch.is_lightweight:
            yield branch


def define_branch_views_for(ctx, depot_branch):
    '''
    Given a depot branch that is not yet mapped into any known Branch view,
    create zero or more Branch views that map this depot_branch into the repo.

    Returns up to one Branch view per fully populated branch. Typically returns
    only one Branch view total unless you have overlapping fully populated
    branch views, or the Depot branch's first changelist holds files that
    straddle multiple locations in the depot.

    Can return empty list if unable to map this Depot branch into the repo,  in
    which case you should shun this Depot branch. Shun this depot branch. Shun
    the mapping of this depot branch. Shun everything, and then shun shunning.

    Returns with any new Branches already assigned branch_ids and inserted into
    ctx.branch_dict().
    '''

    # What files does this branch hold? We'll use them
    # to find intersecting fully populated branches.
    depot_root = depot_branch.root_depot_path
    r = ctx.p4run(['files', '{}/...'.format(depot_root)])
    depot_file_list = [x['depotFile'] for x in r
                       if isinstance(x, dict) and 'depotFile' in x]

    fully_populated_branch_list = [br for br in ctx.branch_dict().values()
                                   if not br.is_lightweight]

    result_list = []
    for br in fully_populated_branch_list:
        br_rerooted = br.copy_rerooted(depot_branch)
        if br_rerooted.intersects_depot_file_list(depot_file_list):
            br_rerooted.branch_id        = p4gf_util.uuid(ctx.p4gf)
            br_rerooted.git_branch_name  = None
            br_rerooted.is_lightweight   = True
            br_rerooted.populated        = True
            br_rerooted.depot_branch     = depot_branch
            ctx.branch_dict()[br_rerooted.branch_id] = br_rerooted
            result_list.append(br_rerooted)

    return result_list


def abbrev(branch):
    '''
    Return first 7 char of branch ID, or "None" if None.
    '''
    if isinstance(branch, Branch):
        return p4gf_util.abbrev(branch.branch_id)
    return p4gf_util.abbrev(branch)


def most_equal(branch_dict):
    '''
    Return the Branch definition that was listed first in p4gf_config.
    '''
    for b in branch_dict.values():
        if b.more_equal:
            return b
    return None

