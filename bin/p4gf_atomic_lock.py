#! /usr/bin/env python3.3
"""Use Reviews feature per gf-instance service user accounts to enforce atomic view locks"""

import p4gf_bootstrap  # pylint: disable=W0611
import p4gf_const
import p4gf_create_p4
import p4gf_l10n
import p4gf_lock
import p4gf_log
import p4gf_util
import p4gf_version
import re
from P4 import P4Exception, Map

P4D_VERSION_NO_NON_GF_CLEANUP  = 2014.1
LOG = p4gf_log.for_module()
_   = p4gf_l10n._
NTR = p4gf_l10n.NTR

SEPARATOR = '...'
INTERSECT = True
NO_INTERSECT = False

REMOVE = NTR('remove')
ADD    = NTR('add')

class LockConflict(Exception):
    """
    Raised when the reviews user acquires a conflicting lock, and the caller
    should abandon the operation it was about to begin.
    """
    pass

def remove_exclusionary_maps(viewlist):
    """Remove exlcusionary maps from lh map list"""
    cleaned = []
    for view in viewlist:
        if view.startswith('-'):
            continue
        cleaned.append(view)
    return cleaned


def opened_locked_files(p4, change):
    """Return list of locked files in changelist."""
    ofiles = []
    try:
        data = p4.run('opened', '-c', change)
        for file_ in data:
            dfile = file_['depotFile']
            if ' ' in dfile and not dfile.startswith('"'):
                dfile = '"' + dfile + '"'
            if 'ourLock' in file_:
                ofiles.append(dfile)
    except P4Exception as e:
        LOG.debug("Error getting open files: {}  {}".format(change, e))
    return ofiles


def get_local_stream_depots(p4):
    """Get list of local depots"""
    depot_pattern = re.compile(r"^" + re.escape(p4gf_const.P4GF_DEPOT))
    data = p4.run('depots')
    depots = []
    for depot in data:
        if (    (depot['type'] == 'local' or depot['type'] == 'stream')
            and not depot_pattern.search(depot['name'])):
            depots.append(depot['name'])
    LOG.debug("get_local_stream_depots: {0}".format(depots))
    return depots

def enquote_if_space(path):
    """Wrap path is double-quotes if SPACE in path."""
    if ' ' in path and not path.startswith('"'):
        path = '"' + path + '"'
    return path

def p4_files_at_change(p4, change):
    """Get list of files in changelist
    """
    depot_files = []
    depots = get_local_stream_depots(p4)
    for depot in depots:
        cmd = ['files']
        cmd.append("//{0}/...@={1}".format(depot, change))
        r = p4.run(cmd)
        for rr in r:
            if not isinstance(rr, dict):
                continue
            df = rr.get('depotFile')
            if isinstance(df, list):
                depot_files.extend(df)
            else:
                depot_files.append(df)

    return depot_files

def can_cleanup_change(p4, change):
    '''Determine whether the Reviews may be cleaned
    from a non-longer pending changelist'''

    try:
        int(change)
    except ValueError:
        return False

    result = p4.run('describe', '-s', str(change))
    vardict = p4gf_util.first_dict_with_key(result, 'change')
    if not vardict:
        LOG.debug("can_cleanup_change: change {0} does not exist : return True".format(change))
        return True

    LOG.debug("can_cleanup_change  describe on {0}: status={1} shelved={2} depotFile={3}".format(
        change, vardict['status'], 'shelved' in vardict, 'depotFile' in vardict))
    if 'code' in vardict and vardict['code'] == 'error' and 'data' in vardict:
        if re.search('no such changelist', vardict['data']):
            return True
        else:
            raise RuntimeError(_("Git Fusion: error in describe for change '{0}': '{1}'")
                .format(change, vardict))

    submitted = False
    pending = False
    no_files = True

    shelved = 'shelved' in vardict
    if 'status' in vardict:
        pending   = vardict['status'] == 'pending'
        submitted = vardict['status'] == 'submitted'
    if not shelved and pending:
        if 'depotFile' in vardict:
            no_files = False
        else:
            no_files = len(p4_files_at_change(p4, change)) == 0


    if pending and shelved:
        return False
    if pending and no_files:
        return True
    if submitted:
        return True
    return False


def cleanup_non_gf_reviews(p4, p4_reviews_non_gf):
    """Remove non-Git Fusion unlocked files from Reviews."""
    with p4.at_exception_level(p4.RAISE_NONE):
        submit_counters = p4.run('counters', '-u', '-e', p4gf_const.P4GF_REVIEWS_NON_GF_RESET + '*')
        LOG.debug3("non-gf_ counters {}".format(str(submit_counters)))
    if submit_counters:
        with p4gf_lock.user_spec_lock(p4, p4gf_const.P4GF_REVIEWS__NON_GF):
            for counter in submit_counters:
                if isinstance(counter, dict) and 'counter' in counter:
                    value = counter['counter']
                    change = value.replace(p4gf_const.P4GF_REVIEWS_NON_GF_SUBMIT, '')
                    if can_cleanup_change(p4, change):
                        remove_non_gf_reviews(p4, p4_reviews_non_gf,
                                          counter['counter'],
                                          counter['value'].split(SEPARATOR), change)

def remove_non_gf_reviews(p4, p4_reviews_non_gf, counter, data, change):
    """ Remove non-Git Fusion Reviews which are now unlocked"""
    LOG.debug3("counter {}  change {}".format(counter, change))
    LOG.debug3("non_gf submit data  {}".format(data))
    filecount = 0
    if len(data) >= 3:
        try:
            filecount = int(data[2])
        except ValueError:
            LOG.debug("Cannot convert non_gf submit_counter filecount - skipping remove for : {0}".
                format(counter))
    if filecount:
        update_repo_reviews(p4_reviews_non_gf, p4gf_const.P4GF_REVIEWS__NON_GF,
                            None, action=REMOVE, change=change)
        p4.run('counter', '-u', '-d',  counter)

CHANGE_FOUND_BEGIN = False
CHANGE_FOUND_END  = False
GF_BEGIN_MARKER   = False
GF_END_MARKER   = False

def review_path_in_changelist(path):
    """ Return True if path lies between (inclusive) the GF change markers.
    The path argument is passed in the list sequence from Reviews.
    """

    global CHANGE_FOUND_BEGIN, CHANGE_FOUND_END
    if not CHANGE_FOUND_BEGIN:
        if path == GF_BEGIN_MARKER:
            CHANGE_FOUND_BEGIN =True
            return True
        else:
            return False
    else:
        if CHANGE_FOUND_END:
            return False
        else:
            if path == GF_END_MARKER:
                CHANGE_FOUND_END  = True
            return True

def remove_non_gf_changelist_files(change, current_reviews):
    """Changelist files in the non-gf user Reviews are bounded
    by changelist markers. Remove that set of files."""
    global GF_BEGIN_MARKER,  GF_END_MARKER, CHANGE_FOUND_BEGIN, CHANGE_FOUND_END
    CHANGE_FOUND_BEGIN = False
    CHANGE_FOUND_END = False
    GF_BEGIN_MARKER = p4gf_const.NON_GF_REVIEWS_BEGIN_MARKER_PATTERN.format(change)
    GF_END_MARKER = p4gf_const.NON_GF_REVIEWS_END_MARKER_PATTERN.format(change)
    current_reviews = [x for x in current_reviews if not review_path_in_changelist(x)]
    return current_reviews


def update_repo_reviews(p4_reviews, user, clientmap, action=None, change=None):
    """Add or remove view left maps to the review user Reviews.
    Using Map.join, check for a conflict with self - this gf_reviews user.
    This check handles the case of overlapping views pushed to the same GF server.
    If conflict, return INTERSECT and do not update the user reviews
    """

    if clientmap:
        repo_views = clientmap.lhs()
    LOG.debug3("clientmap  = {}".format(clientmap))

    args_ = ['-o', user]
    r = p4_reviews.run('user', args_)
    vardict = p4gf_util.first_dict(r)
    current_reviews = []
    if "Reviews" in vardict:
        current_reviews = vardict["Reviews"]
        if action == ADD:
            if has_intersecting_views(current_reviews, clientmap):
                return INTERSECT

    if action == ADD:
        reviews = current_reviews + repo_views
    elif action == REMOVE:
        if user == p4gf_const.P4GF_REVIEWS__NON_GF:
            reviews = remove_non_gf_changelist_files(change, current_reviews)
        else:  # for Git Fusion reviews
            reviews = list(current_reviews)  # make a copy
            for path in repo_views:
                try:
                    reviews.remove(path)
                except ValueError:
                    pass
    else:
        raise RuntimeError(_("Git Fusion: update_repo_reviews incorrect action '{}'")
                           .format(action))
    LOG.debug3("for user {} setting reviews {}".format(user, reviews))
    p4gf_util.set_spec(p4_reviews, 'user', user, values={"Reviews": reviews})
    return NO_INTERSECT


def lock_update_repo_reviews(ctx, repo, clientmap, action=None):
    """Lock on this gf-instance counter lock then add the repo views to the
    service user account. Use 'p4 reviews' to check whether views are locked.
    Cleanup reviews on rejection.
    """
    p4 = ctx.p4gf
    user = p4gf_util.gf_reviews_user_name()
    if not p4gf_util.service_user_exists(p4, user):
        raise RuntimeError(_("Git Fusion: GF instance reviews user '{}' does not exist")
                           .format(user))
    LOG.debug3("user:{} repo:{} action:{}".format(user, repo, action))
    if p4gf_version.p4d_version(p4) <  P4D_VERSION_NO_NON_GF_CLEANUP:
        with p4gf_create_p4.Connector(ctx.p4gf_reviews_non_gf) as p4_reviews_non_gf:
            cleanup_non_gf_reviews(p4, p4_reviews_non_gf)

    with p4gf_create_p4.Connector(ctx.p4gf_reviews) as p4_reviews:
        with p4gf_lock.user_spec_lock(p4, user):
            intersects = update_repo_reviews(p4_reviews, user, clientmap, action=action)

        # When action == ADD, before updating reviews
        # update_repo_reviews checks for a conflict with self - this p4_reviews user
        # this check handles the case of overlapping views pushed to the same GF server
        # if it detects a conflict it returns INTERSECT and does not update the user reviews
        if intersects == INTERSECT:
            msg = p4gf_const.P4GF_LOCKED_BY_MSG.format(user=user)
            LOG.error(msg)
            LOG.error("clientmap: {}".format(clientmap))
            raise LockConflict(msg)

        if action == ADD:
            # Here we check if another P4GF_REVIEWS_GF service user already has locked the view
            # After we have released the lock
            is_locked, by_user = is_locked_by_review(p4, clientmap)
            if is_locked:
                # get the lock again for cleanup and remove the views we just added
                with p4gf_lock.user_spec_lock(p4, user):
                    update_repo_reviews(p4_reviews, user, clientmap, action=REMOVE)
                msg = p4gf_const.P4GF_LOCKED_BY_MSG.format(user=by_user)
                LOG.error(msg)
                LOG.error("clientmap: {}".format(clientmap))
                raise LockConflict(msg)


def is_locked_by_review(p4, clientmap, check_for_self=False):
    """Check whether any other GF/submit users have my views under Review"""
    gf_user = p4gf_util.gf_reviews_user_name()
    repo_views = remove_exclusionary_maps(clientmap.lhs())
    cmd = [NTR('reviews')] + [p4gf_util.dequote(l) for l in repo_views]
    reviewers = p4gf_util.p4run_logged(p4, cmd)
    for user in reviewers:
        _user = user['user']
        if _user.startswith(p4gf_const.P4GF_REVIEWS_GF):
            if _user == p4gf_const.P4GF_REVIEWS__ALL_GF:
                continue        # skip the union Reviews - used only by trigger
            if check_for_self:
                if _user == gf_user:
                    return True, gf_user
            if _user != gf_user:    # always check if another user has this view locked
                return True, _user
    return False, None


def has_intersecting_views(current_reviews, clientmap):
    """Determine whether the clientmap intersects the
    current set of reviews for this GF reviews user.
    """

    reviews_map = Map()
    for v in current_reviews:
        reviews_map.insert(v)

    repo_map = Map()
    for l in clientmap.lhs():
        repo_map.insert(l)

    joined = Map.join(reviews_map, repo_map)

    #for l in joined.lhs():
    #    if not l.startswith('-'):
    #        return INTERSECT

    return NO_INTERSECT
