#! /usr/bin/env python3.3
'''
Git-Swarm integration. Push a Git branch reference to
"review/{git-branch-name}" to create a new Swarm review.

Main entry points are:

    GSReviewCollection.from_prl() to instantiate

    GSReviewCollection.unhandled_review_list() to find a single commit's
        list of reviews that need attention.

    GSReviewCollection.to_file()/from_file() for transferring knowledge from
        pre-receive-hook time to auth_server time.

    GSReviewCollection.pre_copy_to_p4() for pre-receive-hook's chance to
        modify the Git repo with additional merge commits, update
        PreReceiveTuple new_sha1s to point to those new merge commits.

    GSReviewCollection.post_push() for auth_server's post-receive renaming
        Git references to include newly assigned review IDs

    GSReviewCollection.delete_refs_for_closed_reviews() to remove Git branch
        references to any Git Swarm reviews that are no longer pending. Keeps
        the branch list from expanding unbounded and swamping some Git user's
        'git branch -a' report.

'''
from   collections import namedtuple
import logging
import os
import re
#import sys

#import P4

#from   p4gf_branch_id    import PreReceiveTuple
import p4gf_const
from   p4gf_l10n    import _, NTR
import p4gf_proc
import p4gf_util


LOG = logging.getLogger(__name__)

                        # Any Git reference that starts with this string is
                        # a request to create or amend a Git Swarm review.
REF_PREFIX = 'refs/heads/review/'


# -----------------------------------------------------------------------------

class GSReviewCollection:
    '''
    Git Swarm review collection

    If this 'git push' contains Git references to new or existing Git Swarm
    reviews, collect those Git Swarm review identifiers in a dict for fast
    lookup later.

    This is the main entry point for other classes calling into Git Swarm.
    '''
    def __init__(self, ctx, sha1_to_review_list = None):
        '''
        Create a GSReview instance for each requested Git Swarm review.

        Raise exception on malformed or prohibited request.
        '''
        self.ctx                  = ctx
        self._sha1_to_review_list = {}

        if sha1_to_review_list:
            self._sha1_to_review_list = sha1_to_review_list

    @staticmethod
    def from_prl(ctx, prl):
        '''
        Create and populate a new GSReviewCollection from a pushed
        PreReceiveTuple list.
        '''
        sha1_to_review_list = _review_dict(ctx, prl)
        return GSReviewCollection( ctx
                                 , sha1_to_review_list = sha1_to_review_list)

    def review_list(self, sha1):
        '''
        If a commit's sha1 has any Reviews, return 'em.
        If not, return None.
        '''
        return self._sha1_to_review_list.get(sha1, [])

    def unhandled_review_list(self, sha1):
        '''
        If a commit's sha1 has any Reviews that we've not yet
        handled, return 'em.
        '''
        return [r for r in self.review_list(sha1)
                if not r.handled]

    def all_review_list(self):
        '''
        Iterator generator to return all Review instances.
        '''
        for rl in self._sha1_to_review_list.values():
            for r in rl:
                yield r

    def to_file(self):
        '''
        Persistenly store the current push's list of Git references
        being pushed.
        '''
        lines = [r.to_line() for r in self.all_review_list()]
        if not lines:
            return

        filename = _filename(self.ctx)
        with open(filename, 'w') as out:
            LOG.debug('to_file() writing file {}'.format(filename))
            LOG.debug2('to_file() content:\n{}'.format('\n'.join(lines)))
            out.write('\n'.join(lines))

    @staticmethod
    def from_file(ctx):
        '''
        If there's a file with pre-receive tuples, read it and return
        it as a list of PreReceivetTuple items.

        Destructive read.
        '''
        filename = _filename(ctx)
        if not os.path.exists(filename):
            LOG.debug('from_file() no file.')
            return None

        with open(filename, 'r') as fin:
            lines = fin.readlines()

        os.unlink(filename)

        LOG.debug('from_file() read file {}'.format(filename))
        LOG.debug2('from_file() content:\n{}'.format('\n'.join(lines)))

        d = {}
        for review in [GSReview.from_line(line) for line in lines]:
            if not review:
                continue
            d.setdefault(review.sha1, []).append(review)

        return GSReviewCollection( ctx
                                 , sha1_to_review_list = d)

    def pre_copy_to_p4(self, prl):
        '''
        Merge all review heads to their destination Git branches,
        move the branch reference forward to point to the merge
        commit, not the pushed head.

        This is a NEW commit that we create, not one pushed by the Git user.
        Git user will not have this commit, must pull to see it.

        Was:
                   dest
                     v
             ... --- D1

             ... --- R1
                     ^
              review/dest/new

        Becomes:
                   dest
                     v
             ... --- D1 == RM
                           /
             ... --- R1 ---
                           ^
                   review/dest/new

        Modifies prl in-place, replacing any review PreReceiveTuple with a new
        tuple that points to the merge commit as its head.
        '''
                        # Remember our changes for later.
        sha1_changed = False
        for review in self.all_review_list():
            review.sha1 = self._create_merge_commit(review, prl)
            review.prt.new_sha1 = review.sha1
            sha1_changed = True

        for x in prl:
            LOG.debug('pre_copy_to_p4() prt={}'.format(x))

                        # Rebuild index by sha1, since we just changed sha1s.
        if sha1_changed:
            self._sha1_to_review_list = _review_dict(self.ctx, prl)

    @staticmethod
    def post_push(ctx):
        '''
        Main entry point called after any 'git push' returns from both our
        pre-receive hook and Git itself.

        Now it's safe to rename each Git references to a new review, assign
        its review_id.
        '''
        rc = GSReviewCollection.from_file(ctx)
        if not rc:
            return

        rc.rename_git_refs()
        rc.move_git_refs()

    def rename_git_refs(self):
        '''
        Change Git reference "review/master" to "review/master/1234"
        now that we have an assigned review ID 1234, AND we're done with
        pre-receive and git push work. Safe to change references.
        '''
        for review in self.all_review_list():
            review.rename_git_ref()

    def move_git_refs(self):
        '''
        Move branch refs from their pushed heads to the merge commits that
        Git Fusion created to hold their merge into their destination
        branch.
        '''
        for review in self.all_review_list():
            review.move_git_ref()

    @staticmethod
    def delete_refs_for_closed_reviews(ctx):
        '''
        Remove Git branch references to any Git Swarm reviews that are no longer
        pending.

        Keeps the branch list from expanding unbounded and swamping some Git
        user's 'git branch -a' report.
        '''
                        # Just in case pygit2.listall_references() behaves
                        # poorly if we modify references out from under it
                        # during an iteration, collect all the doomed refs
                        # before deleting them.
                        #
        closed_ref_list = [
            ref_review_id.ref
            for ref_review_id in _ref_review_id_list(ctx)
            if not GSReviewCollection.is_review_open(ctx, ref_review_id.review_id)]

        LOG.debug('delete_refs_for_closed_reviews() {}'.format(closed_ref_list))
        if not closed_ref_list:
            return

        p4gf_proc.popen_no_throw(['git', 'branch', '-D'] + closed_ref_list)

    @staticmethod
    def is_review_open(ctx, review_id):
        '''
        Is this review submitted or deleted?
        '''
        status = _review_id_status(ctx, review_id)
        LOG.debug2('is_review_open() review_id={} status={}'
                   .format(review_id, status))
        return status == 'pending'

    @staticmethod
    def _create_merge_commit(review, prl):
        '''
        Create a new merge commit, merging the pushed commit into
        its destination branch. Return new commit's sha1.

        Leaves all references untouched.

        Knows to scan pushed PreReceiveTuple list for any pushed changes
        to destination branch, use (what will eventually be) post-push head,
        not pre-push, as first-parent of new new merge commit.

        Raises exception if unable to create the merge commit (usually due to
        Git merge conflict, error would be from 'git merge'.
        '''
        LOG.debug('_create_merge_commit() {}'.format(review))

                        # Is the destination branch also being modified as part
                        # of this push? If so, use its eventual post-push head,
                        # not current head, for this merge.
        dest_ref_name = 'refs/heads/' + review.git_branch_name
        LOG.debug3('dest_ref_name={}'.format(dest_ref_name))
        first_parent_sha1     = None
        for prt in prl:
            if prt.ref == dest_ref_name:
                first_parent_sha1 = prt.new_sha1
                LOG.debug3('dest branch part of push, pushed head={}'
                           .format(p4gf_util.abbrev(first_parent_sha1)))
                break
        else:
            first_parent_sha1 = p4gf_util.git_rev_list_1(dest_ref_name)
            LOG.debug3('dest branch not part of push, head={}'
                       .format(p4gf_util.abbrev(first_parent_sha1)))

                        # Check out the raw commit, no branch ref.
                        # That way we don't have to put anything back when
                        # we're done (or if we fail).
        p4gf_util.git_checkout(first_parent_sha1)

                        # Merge in the review head.
                        #
        cmd = [ 'git', NTR('merge')
              , '--no-ff'       # Force a new merge commit, don't just
                                #   fast-forward into the review branch.
              , '--no-commit'   # So that we can set its message via file content.
              , review.sha1]
        p4gf_proc.popen(cmd)

                        # Commit the merge, reusing original commit's message
                        # and authorship.
        cmd = [ 'git', NTR('commit')
              , '--reuse-message', review.sha1]
        p4gf_proc.popen(cmd)

                        # The newly commit is under the HEAD. Use its sha1
                        # as the review's sha1.
        merge_sha1 = p4gf_util.git_rev_list_1('HEAD')
        LOG.debug('Merge commit {sha1} created for review {review}'
                  .format( sha1   = p4gf_util.abbrev(merge_sha1)
                         , review = review ))
        return merge_sha1

# -----------------------------------------------------------------------------

class GSReview:
    '''
    A single pushed Git Swarm review reference, associated Git commit sha1,
    whether we've copied that pushed commit into that review.
    '''
    def __init__(self, git_branch_name, review_id, sha1, prt):
                        # Target/destination Git branch name: "master"
                        # of "review/master/new".
                        #
                        # sha1 can be None (such as when created
                        # from_git_branch_refs())
                        #
        self.git_branch_name = git_branch_name
        self.review_id       = review_id
        self.sha1            = sha1

                        # The PreReceiveTuple that caused us to be created.
                        # Can be None (such as when created from_line().
        self.prt             = prt
        self.handled         = False
        self.needs_rename    = False

    def __repr__(self):
        fmt = '{sha1} {review_id:<7} h={handled:1}'             \
              ' nr={needs_rename:1} {git_branch_name:<30}'
        h  = 1 if self.handled      else 0
        nr = 1 if self.needs_rename else 0
        return fmt.format( fmt
                         , git_branch_name = self.git_branch_name
                         , review_id       = self.review_id
                         , handled         = h
                         , needs_rename    = nr
                         , sha1            = self.sha1
                         )

    @staticmethod
    def from_prt(ctx, prt):
        '''
        If this PreReceiveTuple has a reference to a Git Swarm review,
        create and return that GSReview. If not, return None.

        Raise exception on malformed request.
        '''
        git_reference = prt.ref
        if not git_reference.startswith(REF_PREFIX):
            return None

        trid = _to_target_review_id(ctx, git_reference[len(REF_PREFIX):])
        _fail_if_bad_reference(ctx, prt, trid)

        return GSReview( git_branch_name = trid.git_branch_name
                     , review_id       = trid.review_id
                     , sha1            = prt.new_sha1
                     , prt             = prt
                     )

    def old_ref_name(self):
        '''
        Return the review reference name originally pushed.

        review/xxx for new reviews, review/xxx/nnn for amending existing reviews.
        '''
        return NTR('review/{gbn}/new')                          \
               .format( gbn       = self.git_branch_name )

    def new_ref_name(self):
        '''
        Return the complete review reference name, including review_id,
        whose name we want to store.

        review/xxx/nnn
        '''
        return NTR('review/{gbn}/{review_id}')                   \
               .format( gbn       = self.git_branch_name
                      , review_id = self.review_id )

    def rename_git_ref(self):
        '''
        Rename a Git branch reference.

        Old reference lacked a review id. Give it one by renaming.
        '''
        if not self.needs_rename:
            return

        old_rn = self.old_ref_name()
        new_rn = self.new_ref_name()
        cmd = ['git', 'branch', '-m', old_rn, new_rn]
        p4gf_proc.popen(cmd)
        LOG.debug('rename_git_ref() {} ==> {}'
                  .format(old_rn, new_rn))

    def move_git_ref(self):
        '''
        Move a Git branch reference to point to the merge commit
        that Git Fusion created, not pushed commit.
        '''
        ref = self.new_ref_name()
        cmd = ['git', 'branch', '-f', ref, self.sha1]
        p4gf_proc.popen(cmd)
        LOG.debug('move_git_ref() {} ==> {}'
                  .format(ref, self.sha1))

    def to_line(self):
        '''
        File I/O: Write self to a line of text for later reading back in.

        Probably easier to just pickle the thing at this point, but
        I'm fond of simpler text I/O.
        '''
        arr = [ self.git_branch_name
              , self.review_id
              , self.sha1
              , self.handled
              , self.needs_rename
              ]
        return '\t'.join([str(s) for s in arr])

    @staticmethod
    def from_line(line):
        '''
        Counterpart to to_line().
        '''
        arr = line.split('\t')
        git_branch_name = arr[0]
        review_id       = arr[1]
        sha1            = arr[2]
        handled         = arr[3] == str(True)
        needs_rename    = arr[4] == str(True)

        r = GSReview( git_branch_name = git_branch_name
                    , review_id       = review_id
                    , sha1            = sha1
                    , prt             = None )
        r.handled       = handled
        r.needs_rename  = needs_rename
        return r


# -- module-level -------------------------------------------------------------

TargetReviewID = namedtuple( 'TargetReviewID'
                           , [ 'git_branch_name', 'review_id' ] )


RefReviewID = namedtuple( 'RefReviewID'
                        , [ 'ref', 'review_id' ] )

def _to_target_review_id(ctx, git_branch_name):
    '''
    Convert "mybranch/1234" to TargetReviewID('mybranch', '1234').
    If no review-id, return    TargetReviewID('mybranch', None  ).
    '''

                        # First look for push of new review.
                        # Intentionally will match a git-branch-name
                        # that ends in slash-numbers, so that we can
                        # permit Git users to name their branches
                        # with slashes and numbers:
                        #    myjobs/job/01234
    branch = ctx.git_branch_name_to_branch(git_branch_name)

                        # If no match, strip off any trailing /NNNN and
                        # try again.
    if not branch:
        m = re.match( r'^(.*)/(\d+|new)$', git_branch_name )
        if m:
            if m.group(2) != 'new':
                return TargetReviewID( git_branch_name = m.group(1)
                                     , review_id       = m.group(2) )
            else:
                return TargetReviewID( git_branch_name = m.group(1)
                                     , review_id       = None )

    return TargetReviewID( git_branch_name = git_branch_name
                         , review_id       = None )


def _fail_if_bad_reference(ctx, prt, trid):
    '''
    Raise exception if destination not valid.
    '''
    if not prt.ref.startswith(REF_PREFIX):
        return

    trid            = _to_target_review_id(ctx, prt.ref[len(REF_PREFIX):])
    git_branch_name = trid.git_branch_name
    review_id       = trid.review_id
    branch          = ctx.git_branch_name_to_branch(git_branch_name)

    if not branch:
        raise RuntimeError(_("Cannot create Swarm review for '{ref}'."
                             " No such branch: '{gbn}'.")
                           .format( ref = prt.ref
                                  , gbn = git_branch_name ))

    if branch.is_lightweight:
        raise RuntimeError(_("Cannot create Swarm review for '{ref}'."
                             " Not a fully populated branch in Perforce: '{gbn}'.")
                           .format( ref = prt.ref
                                  , gbn = git_branch_name ))

                        # Amending a non-existent review?
    if review_id:
        status = _review_id_status(ctx, review_id)

        if not status:
            raise RuntimeError(_("Cannot amend Swarm review for '{ref}'."
                                 " No such review: '{review_id}'.")
                               .format( ref       = prt.ref
                                      , review_id = review_id ))
        if status != 'pending':
            raise RuntimeError(_("Cannot amend Swarm review for '{ref}'."
                                 " GSReview no longer pending: '{review_id}'.")
                               .format( ref       = prt.ref
                                      , review_id = review_id ))


def _review_id_status(ctx, review_id):
    '''
    Run 'p4 change -o' on the review's changelist and return its status.

    Return None if not found.
    '''
    with ctx.p4.at_exception_level(ctx.p4.RAISE_NONE):
        r = ctx.p4run(['change', '-o', review_id])
    d = [rr for rr in r
         if     rr
            and hasattr(rr, '__getitem__')
            and rr.get('Status')]

    if d:
        return d[0]['Status']
    else:
        return None


def _review_dict(ctx, prl):
    '''
    Return a dict of sha1 => list of GSReview.
    '''
    r = {}
    for prt in prl:
        review = GSReview.from_prt(ctx, prt)
        if review:
            r.setdefault(review.sha1, []).append(review)
    return r


def _filename(ctx):
    '''
    Return path to temp file where we carry our pre-receive tuple list from
    pre-receive hook process to ancestor p4gf_auth_server process.
    '''
    return os.path.join(ctx.view_dirs.view_container, p4gf_const.P4GF_SWARM_PRT)

def _ref_review_id_list(ctx):
    '''
    Iterator/generator to produce a sequence of RefReviewID tuples,
    one for each existing Git branch reference that looks like a Git Swarm
    review reference.
    '''
    regex = re.compile(r'^refs/heads/review/(.*)/(\d+)')
    for ref in ctx.get_view_repo().listall_references():
        m = regex.match(ref)
        if m:
            yield RefReviewID( ref       = ref[len('refs/heads/'):]
                             , review_id = m.group(2) )

