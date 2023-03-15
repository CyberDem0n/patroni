import logging

logger = logging.getLogger(__name__)


class QuorumError(Exception):
    pass


class QuorumStateResolver(object):
    """
    Calculates a list of state transition tuples of the form `('sync'/'quorum',number,set_of_names)`

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
            add new nodes first to sync and than to voters
        else:
            add new nodes first to voters and than to sync
    When removing nodes:
        if sync (synchronous_standby_names) will become empty after removal:
            first remove nodes from voters and than from sync
        else:
            first remove nodes from sync and than from voters
    """

    def __init__(self, leader, quorum, voters, numsync, sync, active, sync_wanted, leader_wanted=None):
        self.leader = leader
        self.quorum = quorum                 # The number of nodes we need to check when doing leader race
        self.voters = set(voters)            # List of nodes we need to check (both stored in the /sync key)
        self.numsync = numsync               # The number of sync nodes in synchronous_standby_names
        self.sync = set(sync)                # List of nodes in synchronous_standby_names
        self.active = active                 # List of active nodes from pg_stat_replication
        self.sync_wanted = sync_wanted       # The desired number of sync nodes
        self.leader_wanted = leader_wanted or leader

    def check_invariants(self):
        if self.voters and not (len(self.voters | self.sync) <= self.quorum + self.numsync):
            raise QuorumError("Quorum and sync not guaranteed to overlap: nodes %d >= quorum %d + sync %d" %
                              (len(self.voters | self.sync), self.quorum, self.numsync))
        if not (self.voters <= self.sync or self.sync <= self.voters):
            raise QuorumError("Mismatched sets: quorum only=%s sync only=%s" %
                              (self.voters - self.sync, self.sync - self.voters))

    def quorum_update(self, quorum, voters, leader=None):
        if quorum < 0:
            raise QuorumError("Quorum %d < 0 of (%s)" % (quorum, voters))
        if leader is not None:
            self.leader = leader
        self.quorum = quorum
        self.voters = voters
        self.check_invariants()
        logger.debug('quorum %s %s %s', self.leader, self.quorum, self.voters)
        return 'quorum', self.leader, self.quorum, self.voters

    def sync_update(self, numsync, sync):
        if numsync < 0:
            raise QuorumError("Sync %d < 0 of (%s)" % (numsync, sync))
        self.numsync = numsync
        self.sync = sync
        self.check_invariants()
        logger.debug('sync %s %s %s', self.leader, self.numsync, self.sync)
        return 'sync', self.leader, self.numsync, self.sync

    def __iter__(self):
        transitions = list(self._generate_transitions())
        # Merge 2 transitions of the same type to a single one. This is always safe because skipping the first
        # transition is equivalent to no one observing the intermediate state.
        for cur_transition, next_transition in zip(transitions, transitions[1:]+[None]):
            if next_transition and cur_transition[0] == next_transition[0]:
                continue
            yield cur_transition

    def _generate_transitions(self):
        logger.debug("Quorum state: leader %s quorum %s, voters %s, numsync %s, sync %s, active %s, sync_wanted %s leader_wanted %s",
                     self.leader, self.quorum, self.voters, self.numsync, self.sync, self.active, self.sync_wanted, self.leader_wanted)
        try:
            self.check_invariants()
        except QuorumError as e:
            logger.warning('%s', e)
            yield self.quorum_update(len(self.sync) - self.numsync, self.sync)

        # If leader changes we need to add the old leader to quorum (voters)
        if self.leader_wanted != self.leader:
            yield self.quorum_update(self.quorum, self.voters | set([self.leader]), self.leader_wanted)

        # Handle non steady state cases
        if self.sync < self.voters:
            logger.debug("Case 1: synchronous_standby_names subset of DCS state")
            # Case 1: quorum is superset of sync nodes. In the middle of changing quorum.
            # Evict from quorum dead nodes that are not being synced.
            remove_from_quorum = self.voters - (self.sync | self.active)
            if remove_from_quorum:
                yield self.quorum_update(
                    quorum=len(self.voters) - len(remove_from_quorum) - self.numsync,
                    voters=self.voters - remove_from_quorum)
            # Start syncing to nodes that are in quorum and alive
            add_to_sync = self.voters - self.sync
            if add_to_sync:
                yield self.sync_update(self.numsync, self.sync | add_to_sync)
        elif self.sync > self.voters:
            logger.debug("Case 2: synchronous_standby_names superset of DCS state")
            # Case 2: sync is superset of quorum nodes. In the middle of changing replication factor.
            # Add to quorum voters nodes that are already synced and active
            add_to_quorum = (self.sync - self.voters) & self.active
            if add_to_quorum:
                yield self.quorum_update(
                        quorum=self.quorum,
                        voters=self.voters | add_to_quorum)
            # Remove from sync nodes that are dead
            remove_from_sync = self.sync - self.voters
            if remove_from_sync:
                yield self.sync_update(
                        numsync=min(self.sync_wanted, len(self.sync) - len(remove_from_sync)),
                        sync=self.sync - remove_from_sync)

        # After handling these two cases quorum and sync must match.
        assert self.voters == self.sync

        safety_margin = self.quorum + self.numsync - len(self.voters | self.sync)
        if safety_margin > 0:  # In the middle of changing replication factor.
            logger.debug("Case 3: quorum ot replication factor is bigger than needed")

            # If we were increasing replication factor the below line will finish transition
            # In case if it was decrease operation we rollback quorum and let remaining code resolve transition
            yield self.quorum_update(len(self.voters) - self.numsync, self.voters)

        # We are in a steady state point. Find if desired state is different and act accordingly.

        # If any nodes have gone away, evict them
        to_remove = self.sync - self.active
        if to_remove:
            logger.debug("Removing nodes: %s", to_remove)
            can_reduce_quorum_by = self.quorum
            # If we can reduce quorum size try to do so first
            if can_reduce_quorum_by:
                # Pick nodes to remove by sorted order to provide deterministic behavior for tests
                remove = set(sorted(to_remove, reverse=True)[:can_reduce_quorum_by])
                sync = self.sync - remove
                # when removing nodes from sync we can safely increase numsync if requested
                numsync = min(self.sync_wanted, len(sync)) if self.sync_wanted > self.numsync else self.numsync
                yield self.sync_update(numsync, sync)
                voters = self.voters - remove
                yield self.quorum_update(len(voters) - self.numsync, voters)
                to_remove &= self.sync
            if to_remove:
                assert self.quorum == 0
                yield self.quorum_update(self.quorum, self.voters - to_remove)
                yield self.sync_update(self.numsync - len(to_remove), self.sync - to_remove)

        # If any new nodes, join them to quorum
        to_add = self.active - self.sync
        if to_add:
            # First get to requested replication factor
            logger.debug("Adding nodes: %s", to_add)
            sync_wanted = min(self.sync_wanted, len(self.sync | to_add))
            increase_numsync_by = sync_wanted - self.numsync
            if increase_numsync_by > 0:
                if self.sync:
                    add = set(sorted(to_add)[:increase_numsync_by])
                    increase_numsync_by = len(add)
                else:  # there is only the leader
                    add = to_add  # and it is safe to add all nodes at once if sync is empty
                yield self.sync_update(self.numsync + increase_numsync_by, self.sync | add)
                voters = self.voters | add
                yield self.quorum_update(len(voters) - sync_wanted, voters)
                to_add -= self.sync
            if to_add:
                voters = self.voters | to_add
                yield self.quorum_update(len(voters) - sync_wanted, voters)
                yield self.sync_update(sync_wanted, self.sync | to_add)

        # Apply requested replication factor change
        sync_increase = min(self.sync_wanted, len(self.sync)) - self.numsync
        if sync_increase > 0:
            # Increase replication factor
            logger.debug("Increasing replication factor to %s", self.numsync + sync_increase)
            yield self.sync_update(self.numsync + sync_increase, self.sync)
            yield self.quorum_update(len(self.voters) - self.numsync, self.voters)
        elif sync_increase < 0:
            # Reduce replication factor
            logger.debug("Reducing replication factor to %s", self.numsync + sync_increase)
            yield self.quorum_update(len(self.voters) - self.numsync - sync_increase, self.voters)
            yield self.sync_update(self.numsync + sync_increase, self.sync)
