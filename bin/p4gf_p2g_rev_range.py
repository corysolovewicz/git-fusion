#! /usr/bin/env python3.3
'''RevRange'''

import logging

from p4gf_l10n                  import _, NTR

LOG = logging.getLogger('p4gf_copy_to_git').getChild('rev_range')


class RevRange:
    """Which Perforce changelists should we copy from Perforce to Git?

    If this is the first copy from Perforce to Git, identify snapshot(s) of
    history prior to our copy that we'll use as starting point(s): the "graft"
    commit before our real copied commits start.
    """
    def __init__(self):

        # string. Perforce revision specifier for first thing to copy from
        # Perforce to Git. If a changelist number "@NNN", NNN might not
        # actually BE a real changelist number or a changelist that touches
        # anything in our view of the depot, and that's okay. Someone else
        # will run 'p4 describe //view/...{@begin},{end} to figure out the
        # TRUE changelist numbers.
        self.begin_rev_spec   = None

        # Integer changelist number of the first changelist we'll copy.
        # Set in _new_repo_from_perforce_range() via 'p4 changes'.
        # Left as 0 for all other creation paths. Used only when grafting.
        self.begin_change_num = 0

        # string.  Perforce revision specifier for last thing to copy from
        # Perforce to Git. Usually "#head" to copy everything up to current
        # time.
        self.end_rev_spec     = None

        # boolean. Is this the first copy into a new git repo? If so, then
        # caller must honor branch_id_to_graft_num if set.
        self.new_repo         = False

        # branch_id ==> integer
        # Last Perforce changelist before this branch's history starts.
        # Can be None for some branches and defined for others: not every
        # branch needs a graft.
        # Defined only if new_repo is True AND begin_rev_spec points to a
        # second-or-later changelist within our view.

    def __str__(self):
        return (( "b,e={begin_rev_spec},{end_rev_spec} new_repo={new_repo}")
                 .format( begin_rev_spec        = self.begin_rev_spec
                        , end_rev_spec          = self.end_rev_spec
                        , new_repo              = self.new_repo))

    def as_range_string(self):
        """Return 'begin,end'."""
        return NTR('{begin},{end}').format(begin=self.begin_rev_spec,
                                           end=self.end_rev_spec)

    @classmethod
    def from_start_stop(cls,
                        ctx,
                        start_at="@1",
                        stop_at="#head"):
        """Factory: create and return a new RevRange object that has Perforce
           revision specifiers for begin and end.

        start_at: Accepts either Perforce revision specifier
                  OR a git sha1 for an existing git commit, which is then
                  mapped to a Perforce changelist number, and then we add 1 to
                  start copying ONE AFTER that sha1's corresponding Perforce
                  changelist.
        stop_at:  Usually "#head".
        """
        if start_at.startswith("@"):
            return RevRange._new_repo_from_perforce_range(start_at,
                                                          stop_at)
        else:
            return RevRange._existing_repo_after_commit(ctx,
                                                        start_at,
                                                        stop_at)

    @classmethod
    def _new_repo_from_perforce_range(cls,
                                      start_at, # "@NNN" Perforce changelist num
                                      stop_at):
        """We're seeding a brand new repo that has no git commits yet.
        """
        result = RevRange()

        result.begin_rev_spec = start_at
        result.end_rev_spec   = stop_at
        result.new_repo       = True
        result.begin_change_num = int(start_at[1:])

        return result



    @classmethod
    def _existing_repo_after_commit(cls,
                                    ctx,
                                    start_at, # some git sha1, maybe partial
                                    stop_at):
        """We're adding to an existing git repo with an existing head.

        Find the Perforce submitted changelist that goes with start_at's Git
        commit sha1, then start at one after that.
        """
        last_commit = _expand_sha1(ctx, start_at)
        last_changelist_number = ctx.mirror.get_last_change_for_commit(last_commit, ctx)
        if not last_changelist_number:
            raise RuntimeError(_( 'Invalid startAt={}: no commit sha1 with a'
                                 ' corresponding Perforce changelist number.')
                               .format(start_at))

        result = RevRange()
        result.begin_rev_spec   = "@{}".format(1 + int(last_changelist_number))
        result.end_rev_spec     = stop_at
        result.new_repo         = False
        return result

def _expand_sha1(ctx, partial_sha1):
    """Given partial SHA1 of a git object, return complete SHA1.

    If there is no match, returns None.
    """
    try:
        obj = ctx.view_repo.git_object_lookup_prefix(partial_sha1)
        return obj.hex if obj else None
    except ValueError:
        return None

