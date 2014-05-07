#! /usr/bin/env python3.3
'''
When copying from Perforce to Git, what Git commits should
the new Git commit use as parents?
'''

import copy
import logging

from   p4gf_desc_info import DescInfo
from   p4gf_object_type import ObjectType
import p4gf_util

LOG = logging.getLogger(__name__)

    # zignotes
    #
    # Depot Branch space
    # --
    # For each file that is a possible integration source, you eventually get
    # its depot_branch_id @ changelist number. If that change is at or before
    # the depot_branch_id@nnn mentioned in our or an ancestor DescInfo, then
    # this integ action is a JIT-branch action and does NOT earn a parent
    # commit.
    #
    # This list must NOT include fully populated Perforce @ some change if that
    # serves as an integ source solely as the basis for some lightweight branch
    # parent.
    #
    # Brach view space
    # --
    # If we've not yet copied any commits into this branch view, then this is
    # the first commit in the branch view, and we DO need to parent to the Git
    # commit that corresponds with each of the (depot) parent-branch-id@change
    # listed in the current changelist's DescInfo.

class ParentCommitList:
    '''
    Code and state for calculating a list of Git commits
    to use as a new Git commit's parents.
    '''

                        # pylint:disable=R0913
                        # Too many arguments
                        # Tough.
    def __init__( self
                , p2g
                , p4change
                , current_branch        # Branch
                , is_first_commit_on_branch
                , filelog_results = None ):
        self.ctx            = p2g.ctx
        self.p2g            = p2g
        self.p4change       = p4change
        self.current_branch = current_branch
        self.is_first_commit_on_branch = is_first_commit_on_branch
        self.desc_info      = DescInfo.from_text(p4change.description)

        # begin output, calculated by calc()

        self.parent_commit_list = []    # str(sha1)/int(mark) list
        self.first_parent_branch_id = None

                # dict of branch_id ==> int(changelist number)
                # set in _calc_branch_to_cl()
        self.branch_id_to_changelist_num = {}

                # Results of 'p4 filelog -m1 -c NNNN //path'
                # p4run result []
                # Used only for memcapped mode (aka not used at all right now)
        self.filelog_results = filelog_results
                        # pylint:enable=R0913

                        # pylint:disable=R0912
                        # Too many branches
                        # Tough.
    def calc(self):
        '''
        Calculate correct parents.
        '''
        if self._copy_parent_list_from_git_object():
            return

                        # Ghost handling:
                        #
                        # If the previous changelist on this branch is a ghost,
                        # don't use that ghost changelist as a parent: it's not
                        # copied to Git, can't use it. Instead, use the non-
                        # ghost changelist that the ghost is attempting to
                        # reproduce.
                        #
                        # If the ghost reproduces the previous changelist in the
                        # current branch, then it's a JIT branch-for-delete
                        # ghost and we should that previous changelist as the
                        # first-parent, effectively skipping over the ghost.
                        #
                        # If the ghost reproduces a change from some other
                        # branch, then it's a rearrange-for-reuse or populate-
                        # new-branch ghost, and we'll need to follow integ links
                        # to find the appropriate parent.
                        #
        skipped_ghost          = self.p2g.ghost_for_branch_id(
                                                  self.current_branch.branch_id)
        branch_head_change_num = self._current_branch_head_change_num()
        if (   (not skipped_ghost)
            or (skipped_ghost.of_change_num == branch_head_change_num)):
            self.parent_commit_list = self._current_branch_head_as_list()
        else:
            self.parent_commit_list = []

                        # Shallow copy? The graft commit is always
                        # orphan/parentless, regardless of whether its Perforce
                        # file revisions are integrated from other branches or
                        # not.
        if self._is_graft_change():
            return

                        # About to create the first commit from a lightweight
                        # branch? Force a parent commit from the parent branch.
        if self.is_first_commit_on_branch:
            p = self._parent_commit_for_first_lt_child()
            if p:
                self.parent_commit_list.append(p)

                        # Any integrations? We need integration source depotFile
                        # paths so we can find source branch(es), and revision
                        # numbers so we can find source changelists.
                        # Expensive call #1:
                        #   p4 filelog -m1 -c {change_num} //...
        if not self.filelog_results:
            (from_list, erev_list) = self.p2g.filelog_to_integ_source_list(
                                                           self.p4change.change)
        else:
            (from_list, erev_list, _ct) = \
                    self.p2g.calc_filelog_to_integ_source_list_mc(
                                                          self.p4change.change
                                                        , self.filelog_results )

                        # If current changelist follows a ghost changelist, then
                        # that ghost might also have some integration links that
                        # we should follow.
        if skipped_ghost and skipped_ghost.change_num:
            (ghost_from_list, ghost_erev_list) = \
                self.p2g.filelog_to_integ_source_list(skipped_ghost.change_num)
            from_list.extend(ghost_from_list)
            erev_list.extend(ghost_erev_list)

        if not from_list:
            LOG.debug('calc() no integ actions, returning'
                      ' current branch head (if any): {}'
                      .format(self.parent_commit_list))
            return

                        # Which changelists contain these source path#revisions?
                        # Expensive call #2:
                        #    p4 fstat -TdepotFile,headChange [path_list]
        cl_to_dfrl = self._fstat_file_rev_to_change_dict(from_list, erev_list)
                        # Avoid repeatedly stripping #rev off of depotFile#rev
                        # strings. Do it once.
        cl_to_dfl = {cl : [p4gf_util.strip_rev(dfr) for dfr in dfrl]
                     for cl, dfrl in cl_to_dfrl.items()}

        self._calc_branch_to_cl(cl_to_dfl)
        min_branch_to_cl    = self._calc_min_branch_to_cl()

                        # Remove integs that are really just JIT-branch actions
                        # or populating this commit from some lightweight
                        # parent's fully populated basis.
        self._remove_jit_integs(min_branch_to_cl)
        if not self.branch_id_to_changelist_num:
                        # Drained the world. No merge parents for you.
            return

        (parent_commit_list, parent_branch_id_list) \
                                               = self._calc_merge_parent_list()
        self.parent_commit_list.extend(parent_commit_list)

        self.parent_commit_list \
                          = p4gf_util.remove_duplicates(self.parent_commit_list)
        self._honor_original_git_parent_sequence()

        if parent_branch_id_list:
            self.first_parent_branch_id = parent_branch_id_list[0]
        else:
            self.first_parent_branch_id = None

                        # Do not use current branch head as first-parent
                        # if this was originally pushed as a non-merge commit.
        if (    2 <= len(self.parent_commit_list)
            and not self.p2g.desc_info_permits_merge(self.p4change.change)):
            no_use = self._current_branch_head_as_list()
            if (    no_use
                and len(no_use)
                and no_use[0] == self.parent_commit_list[0]):
                self.parent_commit_list.pop(0)

                        # pylint:enable=R0912

    def _parent_commit_for_first_lt_child(self):
        '''
        If "change" is going to be the first commit on a new Git branch,
        and if current_branch is a view into a lightweight depot branch
        based on some other branch at some other changelist number, return
        that other branch@change as a sha1/commit that should be a
        commit parent of "change".

        If not, return None.

        Required when the first commit in a lightweight branh is an add,
        not edit, and thus has zero integ actions to connect it to the
        parent branch. We must connect manually. Here.
        '''

        # Not copying from a lightweight branch?
        if not self.current_branch.depot_branch:
            return None

        # Lightweight branch lacks a parent?
        if not self.current_branch.depot_branch.parent_depot_branch_id_list:
            return None

        # Find a commit to go with the parent branch @ changelist
        # upon which dest_db is based.
        dest_db = self.current_branch.depot_branch
        for par_cl in dest_db.parent_changelist_list:
            # Mark for a change/commit we're about to copy to this repo?
            ml = self.p2g.mark_list.cl_to_mark_list(par_cl)
            if ml:
                return ml[0]
            # sha1 we've already copied to this repo?
            commit = ObjectType.commit_for_change(self.ctx, par_cl)
            if commit:
                return commit.sha1

    def _fully_populated_basis_set(self):
        '''
        Return a list of changelist numbers that serve as the fully
        populated basis for current_branch's parent branches.
        '''
        result = set()
        for branch_id in self.branch_id_to_changelist_num.keys():
            branch = self.ctx.branch_dict()[branch_id]
            cl     = branch.find_fully_populated_change_num(self.ctx)
            if cl:
                result.add(int(cl))
        return result

    def _git_object_parent_list(self):
        '''
        Fetch the list of parent commits from our git object mirror of the
        original Git commit that created our current Perforce changelist.

        Return None if no such commit found.

        Return empty list [] if commit found but it lacked parents
        (orphan/first commit in a chain of commits).
        '''
        # Find corresponding Git commit object for this changelist. Ignore the
        # sha1 in the changelist DescInfo: it's only there for Git-to-Perforce
        # changelists, not changelists that originated in Perforce.
        commit = ObjectType.commit_for_change(self.ctx, self.p4change.change)
        if not commit:
            # Cached object is missing, see if the change description has
            # what we need (in the 'parents' field of the Git desc info).
            if self.desc_info and self.desc_info.parents:
                LOG.debug2('_git_object_parent_list() parents from change: {}'.format(
                    self.desc_info.parents))
                return self.desc_info.parents
            else:
                return None

        depot_path  = commit.to_depot_path()
        commit_text = p4gf_util.depot_path_to_git_object( self.ctx.p4gf
                                                        , depot_path )
        # Parse out the parent list.
        parent_list = []
        for line in commit_text.splitlines():
            if line.startswith(b'parent '):
                sha1 = line[len(b'parent '):].decode().strip()
                parent_list.append(sha1)
            elif not len(line.strip()):
                # Done with header. Stop scanning.
                break
        return parent_list

    def _honor_original_git_parent_sequence(self):
        '''
        Reorder self.parent_commit_list to match the order its commits appear in
        the original Git commit.

        Any parents not listed in the original Git commit are appended to the
        back of the list in preserved order.
        '''

        # +++ Cannot resequence a list without multiple elements.
        if len(self.parent_commit_list) < 2:
            return

        want_seq = self._git_object_parent_list()
        if not want_seq:
            return

        # Build the result in order.
        keep_seq = []
        rem_parent_list = copy.copy(self.parent_commit_list)
        for w in want_seq:
            if w in rem_parent_list:
                keep_seq.append(w)
                rem_parent_list.remove(w)
            elif w in self.p2g.sha1_to_mark:
                # try converting the SHA1 to a mark...
                m = self.p2g.sha1_to_mark[w]
                if m in rem_parent_list:
                    keep_seq.append(m)
                    rem_parent_list.remove(m)

            # Convert sha1 to mark number(s), in case parent is part of this
            # pull and not yet stored in git with a sha1.
            commits = ObjectType.commits_for_sha1(self.ctx, w)
            for commit in commits:
                for mark in self.p2g.mark_list.cl_to_mark_list(commit.details.changelist):
                    if mark in rem_parent_list:
                        keep_seq.append(mark)
                        rem_parent_list.remove(mark)
        # Add any remainder to the end of the result.
        keep_seq.extend(rem_parent_list)
        self.parent_commit_list = keep_seq

    def _fstat_file_rev_to_change_dict(self, file_list, rev_list):
        '''
        Given a list of file paths and corresponding revision numbers,
        figure out which changes contain which files.

        Return a dict of int(changelist number) to [list of depotFile#rev].
        '''
        file_rev_list = p4gf_util.to_path_rev_list(file_list, rev_list)
        result = {}
        r = self.ctx.p4run(['fstat', '-TdepotFile,headRev,headChange'
                           , file_rev_list])
        for rr in r:
            if not isinstance(rr, dict):
                continue
            depot_file    = rr.get('depotFile')
            rev_number    = rr.get('headRev')
            head_change   = rr.get('headChange')
            if (not depot_file) or (not rev_number) or (not head_change):
                continue
            change_number = int(head_change)
            depot_file_list = result.get(change_number)
            if not depot_file_list:
                depot_file_list = []
                result[change_number] = depot_file_list
            depot_file_list.append(p4gf_util.to_path_rev(
                                                    depot_file, rev_number))
        return result

    def _calc_branch_to_cl(self, cl_to_depot_file_list):
        '''
        Return
           dict of branch_id ==> int(changelist number)

        where changelist number is which changelist to use as a Git merge
        parent from that branch.

        cl_to_depot_file_list : dict of
            int(changelist number) ==> list[depotFile integ source]

        Git usually has only one merge parent per parent branch.
        Use only the last (highest numbered) source changelist
        from each branch.

        Yes this is lossy and not entirely correct:
        * Lose which files go with which changelist
        * Lose which (if not all) files in this last changelist were
          actually merged at this merge commit.
        '''
        parent_branch_to_cl = {}
        for branch in self.ctx.branch_dict().values():
            if branch == self.current_branch:
                continue

            # Find the highest changelist number that intersects this branch
            # +++ Walk backwards through history on the assumption that the
            #     hit will be closer to the recent end of history
            #     than the far past.
            #     Reduce calls to Branch.intersects_depot_file_list()
            for cl in sorted(cl_to_depot_file_list.keys(), reverse=True):
                dfl = cl_to_depot_file_list[cl]
                if branch.intersects_depot_file_list(dfl):
                    parent_branch_to_cl[branch.branch_id] = cl
                    break
        self.branch_id_to_changelist_num = parent_branch_to_cl

    def _calc_ancestor_dbid_to_cl(self):
        '''
        For each ancestor Depot branch, what's the changelist on that branch
        from which we diverged?

        Return a dict { depot branch id : int(changelist) }

        +++ Wow we're calculating this per changelist, over and over?
        '''
        cdb = self.current_branch.get_or_find_depot_branch(self.ctx)
        if not cdb:
            return {}

        result     = {}
        work_queue = [cdb]
        seen       = {cdb.depot_branch_id}
        while work_queue:
            cdb = work_queue.pop(0)
            for dbid, cl in zip( cdb.parent_depot_branch_id_list
                               , cdb.parent_changelist_list):
                cl_int = int(cl)
                if not dbid in result:
                    result[dbid] = cl_int
                else:
                    # Multiple paths to this ancestor, keep the highest
                    # changelist number as our basis.
                    if result[dbid] < cl_int:
                        result[dbid] = cl_int

                if not dbid in seen:
                    other_db = self.ctx.depot_branch_info_index() \
                                                .find_depot_branch_id(dbid)
                    if other_db:
                        work_queue.append(other_db)
                        seen.add(dbid)
        return result

    def _calc_min_branch_to_cl(self):
        '''
        Return dict of branch_id ==> int(changelist number)

        Where changelist number is the lowest acceptable changelist number
        for each branch, if any, due to JIT-branch actions into
        current_branch.

        Return empty dict if current branch not lightweight.
        '''
        if not self.current_branch.is_lightweight:
            return {}

        # Creating a new branch? Then we want the changelist where we diverge
        # from parent. If not, then we only want changes from AFTER that
        # divergence: integration from divergence is JIT-branching.
        is_first_commit_on_branch = self.p2g.is_branch_empty(
                                                  self.current_branch.branch_id)
        if is_first_commit_on_branch:
            equ_avoider = 0     # Keep integ from branch point
        else:
            equ_avoider = 1     # Skip integ from branch point

        # Find our immediate parent depot branches, at which changelist.
        parent_dbid_to_cl  = {}
        parent_dbid_to_cl  = self._calc_ancestor_dbid_to_cl()
        branch_to_cl = {}
        for branch_id in self.branch_id_to_changelist_num.keys():
            branch = self.ctx.branch_dict().get(branch_id)
            db     = branch.get_or_find_depot_branch(self.ctx)
            dbid   = db.depot_branch_id if db else 'None'
            if dbid in parent_dbid_to_cl:
                branch_to_cl[branch_id] = parent_dbid_to_cl[dbid] + equ_avoider
        return branch_to_cl

    def _calc_merge_parent_list(self):
        '''
        Return a list of marks/sha1s to use as parents for a Git merge commit.

        Returns a 2-tuple of ( [mark/commit parent], [parent branch id] ).
        '''
        parent_commit_id = []
        parent_branch_id = []
        for branch_id, cl in self.branch_id_to_changelist_num.items():
            # Changelists from before the start of history cannot be
            # parents. No merging from beyond the event horizon.
            if cl < self.p2g.rev_range.begin_change_num:
                continue

            # Each Perforce commit maps to zero or more Git commits, one per
            # branch that intersects the integ source files from that commit.

            # Do we have any pending commits for this changelist?
            ml = self.p2g.mark_list.cl_to_mark_list(str(cl))
            for mark in ml:
                ### Must only include mark if mark associated with branch.
                ### Must only include one mark here.
                ### But that requires adding more branch/mark tracking than we
                ### want to add until I have a test that proves we need it.
                if not mark in parent_commit_id:
                    parent_commit_id.append(mark)
                    parent_branch_id.append(branch_id)

            # Do we have any existing commits for this changelist?
            commit = ObjectType.commit_for_change(self.ctx, cl)

            # Does this Git commit occur in our Git repo at this
            # changelist number?
            if (commit and
                (not commit.sha1 in parent_commit_id) and
                p4gf_util.sha1_exists(commit.sha1) ):
                parent_commit_id.append(commit.sha1)
                parent_branch_id.append(branch_id)

        return (parent_commit_id, parent_branch_id)

    def _current_branch_head_as_list(self):
        '''
        Return a list with 1 or 0 elements: the current branch's head commit
        sha1/mark, if any, as a 1-element list.
        '''
        p = self.p2g.branch_head_mark_or_sha1(self.current_branch.branch_id)
        if p:
            return [p]
        return []

    def _current_branch_head_change_num(self):
        '''
        Return the integer changelist number of the most recent changelist
        copied to Git on the current branch.
        '''
        return self.p2g.branch_head_to_change_num(self.current_branch.branch_id)

    def _is_graft_change(self):
        '''
        Is our current p4change the graft change?
        Those never have parents.
        '''
        if self.p2g.branch_id_to_graft_change:
            graft_change = self.p2g.branch_id_to_graft_change.get(
                                                  self.current_branch.branch_id)
            if graft_change and (self.p4change.change == graft_change.change):
                return True
        return False

    def _remove_jit_integs(self, min_branch_to_cl):
        '''
        Remove from branch_id_to_changelist_num any integs that are really just
        JIT-branch actions or populating this first-commit-on-current-branch
        from a lightweight parent's fully populated basis.
        '''

        # Ignore integs from fully populated Perforce if those integs are
        # solely to act as the basis for some lightweight parent.
        if self.is_first_commit_on_branch:
            fp_basis_set = self._fully_populated_basis_set()
        else:
            fp_basis_set = set()

        del_keys = []
        branch_dict = self.ctx.branch_dict()    # For less typing.
        for branch_id, cl in self.branch_id_to_changelist_num.items():
            if cl in fp_basis_set:
                del_keys.append(branch_id)
                continue

            branch = branch_dict[branch_id]
            for min_branch_id, min_cl in min_branch_to_cl.items():
                # If this integ source is, or is an ancestor of,
                # this <min_branch_id, min_cl> tuple's branch.
                min_applies = (
                          branch_id == min_branch_id
                       or branch.is_ancestor_of_lt( self.ctx
                                                  , branch_dict[min_branch_id]))

                # Then check min_cl against the cl we have for this branch_id.
                if (    min_applies
                    and min_cl and cl < min_cl):
                    del_keys.append(branch_id)
                    break

        for k in del_keys:
            del self.branch_id_to_changelist_num[k]

    def _can_use_as_parent(self, sha1):
        '''
        If sha1 already copied to Git, return sha1.

        If sha1 not yet copied, but its corresponding Perforce changelist is in
        our MarkList of impending copies, return the mark for that changelist.

        If not, return None
        '''
        # Already stored in Git from some previous push or pull?
        if p4gf_util.sha1_exists(sha1):
            return sha1

        # Any of this commit's corresponding changelists
        # about to be copied to Git in this pull?
        for commit in ObjectType.commits_for_sha1(self.ctx, sha1):
            ml = self.p2g.mark_list.cl_to_mark_list(commit.details.changelist)
            if ml:
                return ml[0]

        return None

    def _copy_parent_list_from_git_object(self):
        '''
        If this Perforce changelist contains special Perforce-only file actions
        that affect our usual parent calculations, skip those normal
        calculations and copy the parent list out of this changelist's
        corresponding commit.

        Only works if we're not rebuilding after a repo refactor.
        '''
        if not self.desc_info or not self.desc_info.contains_p4_extra:
            return False

        want_seq = self._git_object_parent_list()
        if None == want_seq:
            return False

        # Do all these parents exist as either copied commits (sha1) or soon-to-
        # be-fast-imported commits (changelists with corresponding marks)? If
        # not, then something has changed (Repo Refactor, at thee I shake my
        # Fists of Impotent Rage!)
        parent_list = []
        for par_sha1 in want_seq:
            par = self._can_use_as_parent(par_sha1)
            if not par:
                return False
            parent_list.append(par)

        self.parent_commit_list = parent_list
        return True
