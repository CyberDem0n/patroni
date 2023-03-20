import logging

from collections.abc import MutableSet
from typing import Iterable, Iterator, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class CaseInsensitiveSet(MutableSet):

    def __init__(self, values: Optional[Iterable[str]] = None) -> None:
        self._values = {}
        for v in values or ():
            self.add(v)

    def __repr__(self) -> str:
        return '<{0}{1} at {2:x}>'.format(type(self).__name__, tuple(self._values.values()), id(self))

    def __str__(self) -> str:
        return str(set(self._values.values()))

    def __contains__(self, value: str) -> bool:
        return value.casefold() in self._values

    def __iter__(self) -> Iterator[str]:
        return iter(self._values.values())

    def __len__(self) -> int:
        return len(self._values)

    def add(self, value: str) -> None:
        self._values[value.casefold()] = value

    def discard(self, value: str) -> None:
        self._values.pop(value.casefold(), None)


class QuorumError(Exception):
    pass


class QuorumStateResolver(object):
    """Calculates a list of state transition tuples of the form `('sync'/'quorum'/'restart',leader,number,set_of_names)`

    Synchronous replication state is set in two places. PostgreSQL configuration sets how many and which nodes are
    needed for a commit to succeed, abbreviated as `numsync` and `sync` set here. DCS contains information about how
    many and which nodes need to be interrogated to be sure to see an xlog position containing latest confirmed commit,
    abbreviated as `quorum` and `voters` set. Both pairs have the meaning "ANY n OF set".

    The number of nodes needed for commit to succeed, `numsync`, is also called the replication factor.

    To guarantee zero lost transactions on failover we need to keep the invariant that at all times any subset of
    nodes that can acknowledge a commit overlaps with any subset of nodes that can achieve quorum to promote a new
    leader. Given a desired replication factor and a set of nodes able to participate in sync replication there
    is one optimal state satisfying this condition. Given the node set `active`, the optimal state is:

        sync = voters = active
        numsync = min(sync_wanted, len(active))
        quorum = len(active) - numsync

    We need to be able to produce a series of state changes that take the system to this desired state from any
    other state arbitrary given arbitrary changes is node availability, configuration and interrupted transitions.

    To keep the invariant the rule to follow is that when increasing `numsync` or `quorum`, we need to perform the
    increasing operation first. When decreasing either, the decreasing operation needs to be performed later.

    Order of adding or removing nodes from sync and voters depends on the state of synchronous_standby_names:
    When adding new nodes:
        if sync (synchronous_standby_names) is empty:
            add new nodes first to sync and than to voters when numsync_confirmed > 0
        else:
            add new nodes first to voters and than to sync
    When removing nodes:
        if sync (synchronous_standby_names) will become empty after removal:
            first remove nodes from voters and than from sync
        else:
            first remove nodes from sync and than from voters. make voters empty if numsync_confirmed == 0"""

    def __init__(self, leader: str, quorum: int, voters: Iterable[str],
                 numsync: int, sync: Iterable[str], numsync_confirmed: int,
                 active: Iterable[str], sync_wanted: int, leader_wanted: str) -> None:
        self.leader = leader                          # The leader according to the `/sync` key
        self.quorum = quorum                          # The number of nodes we need to check when doing leader race
        self.voters = CaseInsensitiveSet(voters)      # Set of nodes we need to check (both stored in the /sync key)
        self.numsync = numsync                        # The number of sync nodes in synchronous_standby_names
        self.sync = CaseInsensitiveSet(sync)          # Set of nodes in synchronous_standby_names
        # The number of nodes that are confirmed to reach safe LSN after adding them to `synchronous_standby_names`.
        # We don't list them because it is known that they are always included into active.
        self.numsync_confirmed = numsync_confirmed
        self.active = CaseInsensitiveSet(active)      # Set of active nodes from `pg_stat_replication`
        self.sync_wanted = sync_wanted                # The desired number of sync nodes
        self.leader_wanted = leader_wanted or leader  # The desired leader

    def check_invariants(self) -> None:
        """Checks invatiant of synchronous_standby_names and /sync key in DCS.

        We need to verify that subset of nodes that can acknowledge a commit overlaps
        with any subset of nodes that can achieve quorum to promote a new leader.

        :raises QuorumError: in case of broken state"""

        if self.voters and not (len(self.voters | self.sync) <= self.quorum + self.numsync):
            raise QuorumError("Quorum and sync not guaranteed to overlap: nodes %d >= quorum %d + sync %d" %
                              (len(self.voters | self.sync), self.quorum, self.numsync))
        if not (self.voters <= self.sync or self.sync <= self.voters) or self.voters and not self.sync:
            raise QuorumError("Mismatched sets: quorum only=%s sync only=%s" %
                              (self.voters - self.sync, self.sync - self.voters))

    def quorum_update(self, quorum: int, voters: Set[str],
                      leader: Optional[str] = None) -> Iterator[Tuple[str, str, int, Set[str]]]:
        """Updates quorum, voters and optionally leader fields

        :rtype: Iterator[tuple(type, leader, quorum, voters)] with the new quorum state,
                where type could be 'quorum' or 'restart'. The later means that
                quorum could not be updated with the current input data
                and the `QuorumStateResolver` should be restarted.
        :raises QuorumError: in case of invalid data or if invariant after transition could not be satisfied"""

        if quorum < 0:
            raise QuorumError("Quorum %d < 0 of (%s)" % (quorum, voters))
        if quorum > 0 and quorum >= len(voters):
            raise QuorumError("Quorum %d >= N of (%s)" % (quorum, voters))

        old_leader = self.leader
        if leader is not None:  # Change of leader was requested
            self.leader = leader
        elif self.numsync_confirmed == 0:
            # If there are no nodes that known to caught up with the primary we want to reset quorum/votes in /sync key
            quorum = 0
            voters = CaseInsensitiveSet()
        else:
            # It could be that the number of nodes that are known to catch up with the primary is below desired numsync.
            # We want to increase quorum to guaranty that the sync node will be found during the leader race.
            quorum += max(self.numsync - self.numsync_confirmed, 0)
            if not voters:
                # We want to reset numsync_confirmed to 0 if voters will become empty after next update of /sync key
                self.numsync_confirmed = 0

        if (self.leader, quorum, voters) == (old_leader, self.quorum, self.voters):
            if self.voters:
                return
            # If transition produces no change of leader/quorum/voters we want to give a hint to
            # the caller to fetch the new state from the database and restart QuorumStateResolver.
            yield 'restart', self.leader, self.quorum, self.voters

        self.quorum = quorum
        self.voters = voters
        self.check_invariants()
        logger.debug('quorum %s %s %s', self.leader, self.quorum, self.voters)
        yield 'quorum', self.leader, self.quorum, self.voters

    def sync_update(self, numsync: int, sync: Set[str]) -> Iterator[Tuple[str, str, int, Set[str]]]:
        """Updates numsync and sync fields.

        :rtype: Iterator[tuple('sync', leader, numsync, sync)] with the new state of synchronous_standby_names
        :raises QuorumError: in case of invalid data ot if invariant after transition could not be satisfied"""

        if numsync < 0:
            raise QuorumError("Sync %d < 0 of (%s)" % (numsync, sync))
        if numsync > 0 and numsync > len(sync):
            raise QuorumError("Sync %s > N of (%s)" % (numsync, sync))

        self.numsync = numsync
        self.sync = sync
        self.check_invariants()
        logger.debug('sync %s %s %s', self.leader, self.numsync, self.sync)
        yield 'sync', self.leader, self.numsync, self.sync

    def __iter__(self):
        transitions = list(self._generate_transitions())
        # Merge 2 transitions of the same type to a single one. This is always safe because skipping the first
        # transition is equivalent to no one observing the intermediate state.
        for cur_transition, next_transition in zip(transitions, transitions[1:]+[None]):
            if next_transition and cur_transition[0] == next_transition[0]:
                continue
            yield cur_transition
            if cur_transition[0] == 'restart':
                break

    def _generate_transitions(self):
        logger.debug("Quorum state: leader %s quorum %s, voters %s, numsync %s, sync %s, "
                     "numsync_confirmed %s, active %s, sync_wanted %s leader_wanted %s",
                     self.leader, self.quorum, self.voters, self.numsync, self.sync,
                     self.numsync_confirmed, self.active, self.sync_wanted, self.leader_wanted)
        try:
            self.check_invariants()
        except QuorumError as e:
            logger.warning('%s', e)
            yield from self.quorum_update(len(self.sync) - self.numsync, self.sync)

        # numsync_confirmed could be 0 after restart/failover, we will calculate it from quorum
        if self.numsync_confirmed == 0 and self.sync:
            self.numsync_confirmed = len(self.voters & self.sync) - self.quorum
            logger.debug('numsync_confirmed=0, adjusting it to %d', self.numsync_confirmed)

        # If leader changes we need to add the old leader to quorum (voters)
        if self.leader_wanted != self.leader:
            voters = self.voters | CaseInsensitiveSet([self.leader])
            yield from self.quorum_update(self.quorum, voters, self.leader_wanted)

        # Handle non steady state cases
        if self.sync < self.voters:
            logger.debug("Case 1: synchronous_standby_names subset of DCS state")
            # Case 1: quorum is superset of sync nodes. In the middle of changing quorum.
            # Evict from quorum dead nodes that are not being synced.
            remove_from_quorum = self.voters - (self.sync | self.active)
            if remove_from_quorum:
                yield from self.quorum_update(
                    quorum=len(self.voters) - len(remove_from_quorum) - self.numsync,
                    voters=self.voters - remove_from_quorum)
            # Start syncing to nodes that are in quorum and alive
            add_to_sync = (self.voters & self.active) - self.sync
            if add_to_sync:
                yield from self.sync_update(self.numsync, self.sync | add_to_sync)
        elif self.sync > self.voters:
            logger.debug("Case 2: synchronous_standby_names superset of DCS state")
            # Case 2: sync is superset of quorum nodes. In the middle of changing replication factor.
            # Add to quorum voters nodes that are already synced and active
            add_to_quorum = (self.sync - self.voters) & self.active
            if add_to_quorum:
                voters = self.voters | add_to_quorum
                yield from self.quorum_update(len(voters) - self.numsync, voters)
            # Remove from sync nodes that are dead
            remove_from_sync = self.sync - self.voters
            if remove_from_sync:
                yield from self.sync_update(
                        numsync=min(self.sync_wanted, len(self.sync) - len(remove_from_sync)),
                        sync=self.sync - remove_from_sync)

        # After handling these two cases quorum and sync must match.
        assert self.voters == self.sync

        safety_margin = self.quorum + min(self.numsync, self.numsync_confirmed) - len(self.voters | self.sync)
        if safety_margin > 0:  # In the middle of changing replication factor.
            if self.numsync > self.sync_wanted:
                logger.debug('Case 3: replication factor is bigger than needed')
                yield from self.sync_update(max(self.sync_wanted, len(self.voters) - self.quorum), self.sync)
            else:
                logger.debug('Case 4: quorum is bigger than needed')
                yield from self.quorum_update(len(self.sync) - self.numsync, self.voters)
        else:
            safety_margin = self.quorum + self.numsync - len(self.voters | self.sync)
            if self.numsync == self.sync_wanted and safety_margin > 0 and self.numsync > self.numsync_confirmed:
                yield from self.quorum_update(len(self.sync) - self.numsync, self.voters)

        # We are in a steady state point. Find if desired state is different and act accordingly.

        # If any nodes have gone away, evict them
        to_remove = self.sync - self.active
        if to_remove:
            logger.debug("Removing nodes: %s", to_remove)
            can_reduce_quorum_by = self.quorum
            # If we can reduce quorum size try to do so first
            if can_reduce_quorum_by:
                # Pick nodes to remove by sorted order to provide deterministic behavior for tests
                remove = CaseInsensitiveSet(sorted(to_remove, reverse=True)[:can_reduce_quorum_by])
                sync = self.sync - remove
                # when removing nodes from sync we can safely increase numsync if requested
                numsync = min(self.sync_wanted, len(sync)) if self.sync_wanted > self.numsync else self.numsync
                yield from self.sync_update(numsync, sync)
                voters = self.voters - remove
                yield from self.quorum_update(len(voters) - self.numsync, voters)
                to_remove &= self.sync
            if to_remove:
                assert self.quorum == 0
                yield from self.quorum_update(self.quorum, self.voters - to_remove)
                yield from self.sync_update(self.numsync - len(to_remove), self.sync - to_remove)

        # If any new nodes, join them to quorum
        to_add = self.active - self.sync
        if to_add:
            # First get to requested replication factor
            logger.debug("Adding nodes: %s", to_add)
            sync_wanted = min(self.sync_wanted, len(self.sync | to_add))
            increase_numsync_by = sync_wanted - self.numsync
            if increase_numsync_by > 0:
                if self.sync:
                    add = CaseInsensitiveSet(sorted(to_add)[:increase_numsync_by])
                    increase_numsync_by = len(add)
                else:  # there is only the leader
                    add = to_add  # and it is safe to add all nodes at once if sync is empty
                yield from self.sync_update(self.numsync + increase_numsync_by, self.sync | add)
                voters = self.voters | add
                yield from self.quorum_update(len(voters) - sync_wanted, voters)
                to_add -= self.sync
            if to_add:
                voters = self.voters | to_add
                yield from self.quorum_update(len(voters) - sync_wanted, voters)
                yield from self.sync_update(sync_wanted, self.sync | to_add)

        # Apply requested replication factor change
        sync_increase = min(self.sync_wanted, len(self.sync)) - self.numsync
        if sync_increase > 0:
            # Increase replication factor
            logger.debug("Increasing replication factor to %s", self.numsync + sync_increase)
            yield from self.sync_update(self.numsync + sync_increase, self.sync)
            yield from self.quorum_update(len(self.voters) - self.numsync, self.voters)
        elif sync_increase < 0:
            # Reduce replication factor
            logger.debug("Reducing replication factor to %s", self.numsync + sync_increase)
            if self.quorum - sync_increase < len(self.voters):
                yield from self.quorum_update(len(self.voters) - self.numsync - sync_increase, self.voters)
            yield from self.sync_update(self.numsync + sync_increase, self.sync)
