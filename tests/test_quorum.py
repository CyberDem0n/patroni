import unittest

from patroni.quorum import QuorumStateResolver, QuorumError


class QuorumTest(unittest.TestCase):
    def test_1111(self):
        state = 1, set("a"), 1, set("a")
        self.assertEqual(list(QuorumStateResolver(*state, active=set("ab"), sync_wanted=3)), [
            ('sync', 2, set('ab')),
            ('quorum', 1, set('ab')),
        ])
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abcde"), sync_wanted=3)), [
            ('sync', 3, set('abcde')),
            ('quorum', 3, set('abcde')),
        ])

    def test_1222(self):
        """2 node cluster"""
        state = 1, set("ab"), 2, set("ab")
        # Active set matches state
        self.assertEqual(list(QuorumStateResolver(*state, active=set("ab"), sync_wanted=3)), [
        ])
        # Add node by increasing quorum
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=2)), [
            ('quorum', 2, set('abc')),
            ('sync', 2, set('abc')),
        ])
        # Add node by increasing sync
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=3)), [
            ('sync', 3, set('abc')),
            ('quorum', 1, set('abc')),
        ])
        # Add multiple nodes by increasing both sync and quorum
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abcde"), sync_wanted=3)), [
            ('sync', 3, set('abc')),
            ('quorum', 3, set('abcde')),
            ('sync', 3, set('abcde')),
        ])
        # Master is alone
        self.assertEqual(list(QuorumStateResolver(*state, active=set("a"), sync_wanted=2)), [
            ('quorum', 1, set('a')),
            ('sync', 1, set('a')),
        ])
        # Swap out sync replica
        self.assertEqual(list(QuorumStateResolver(*state, active=set("ac"), sync_wanted=2)), [
            ('quorum', 1, set('a')),
            ('sync', 2, set('ac')),
            ('quorum', 1, set('ac')),
        ])

    def test_1233(self):
        """Interrupted transition from 2 node cluster to 3 node fully sync cluster"""
        state = 1, set("ab"), 3, set("abc")
        # Node c went away, transition back to 2 node cluster
        self.assertEqual(list(QuorumStateResolver(*state, active=set("ab"), sync_wanted=3)), [
            ('sync', 2, set('ab')),
        ])
        # Node c is available transition to larger quorum set.
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=3)), [
            ('quorum', 1, set('abc')),
        ])
        # Add in a new node at the same time
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abcd"), sync_wanted=3)), [
            ('quorum', 2, set('abcd')),
            ('sync', 3, set('abcd')),
        ])
        # Change replication factor at the same time
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=2)), [
            ('quorum', 2, set('abc')),
            ('sync', 2, set('abc')),
        ])

    def test_2322(self):
        """Interrupted transition from 2 node cluster to 3 node cluster with replication factor 2"""
        state = 2, set("abc"), 2, set("ab")
        # Node c went away, transition back to 2 node cluster
        self.assertEqual(list(QuorumStateResolver(*state, active=set("ab"), sync_wanted=2)), [
            ('quorum', 1, set('ab')),
        ])
        # Node c is available transition to larger quorum set.
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=2)), [
            ('sync', 2, set('abc')),
        ])
        # Add in a new node at the same time
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abcd"), sync_wanted=2)), [
            ('sync', 2, set('abc')),
            ('quorum', 3, set('abcd')),
            ('sync', 2, set('abcd')),
        ])
        # Convert to a fully synced cluster
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=3)), [
            ('sync', 3, set('abc')),
            ('quorum', 1, set('abc')),
        ])

    def test_3535(self):
        state = 3, set("abcde"), 3, set("abcde")
        # remove nodes
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=3)), [
            ('sync', 3, set('abc')),
            ('quorum', 1, set('abc')),
        ])
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abcd"), sync_wanted=3)), [
            ('sync', 3, set('abcd')),
            ('quorum', 2, set('abcd')),
        ])

        # remove nodes and decrease sync
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=2)), [
            ('sync', 3, set('abc')),
            ('quorum', 2, set('abc')),
            ('sync', 2, set('abc')),
        ])

        # Increase replication factor and decrease quorum
        self.assertEqual(list(QuorumStateResolver(*state, active=set('abcde'), sync_wanted=4)), [
            ('sync', 4, set('abcde')),
            ('quorum', 2, set('abcde')),
        ])

        # Add node with decreasing sync and increasing quorum
        self.assertEqual(list(QuorumStateResolver(*state, active=set('abcdef'), sync_wanted=2)), [
            ('quorum', 5, set('abcdef')),   # increase quorum one more time
            ('sync', 2, set('abcdef')),     # now reduce replication factor to requested value
        ])

        # Remove node with increasing sync and decreasing quorum
        self.assertEqual(list(QuorumStateResolver(*state, active=set('abcd'), sync_wanted=4)), [
            ('sync', 4, set('abcd')),       # node e removed from sync wth replication factor increase
            ('quorum', 1, set('abcd')),     # node e removed from voters with quorum decrease
        ])

    def test_nonsync_promotion(self):
        # Beginning state: 2 of abc in sync. e.g. (a master, ssn = ANY 1 (b c))
        # a fails, d sees b and c, knows that it is in sync and decides to promote.
        # We include in sync state everybody that already was in sync, increasing replication factor
        # and let situation resolve. Node d ssn=ANY 2 (a b c)
        state = 2, set("abc"), 3, set("abcd")
        self.assertEqual(list(QuorumStateResolver(*state, active=set("bcd"), sync_wanted=2)), [
            ('quorum', 2, set('abcd')),  # Set ourselves to be a member of the quorum
            ('sync', 3, set('bcd')),     # Remove a from being synced to.
            ('quorum', 2, set('bcd')),   # Remove a from quorum
            ('sync', 2, set('bcd')),     # Can now reduce replication factor to original value
        ])

    def test_invalid_states(self):
        # Main invariant is not satisfied, system is in an unsafe state
        resolver = QuorumStateResolver(1, set("abc"), 2, set("abc"), active=set("abc"), sync_wanted=2)
        self.assertRaises(QuorumError, resolver.check_invariants)
        self.assertEqual(list(resolver), [
            ('quorum', 2, set('abc'))
        ])
        # Quorum and sync states mismatched, somebody other than Patroni modified system state
        resolver = QuorumStateResolver(2, set("abc"), 3, set("abd"), active=set("abd"), sync_wanted=2)
        self.assertRaises(QuorumError, resolver.check_invariants)
        self.assertEqual(list(resolver), [
            ('quorum', 2, set('abd')),
            ('sync', 2, set('abd')),
        ])

    def test_sync_high_quorum_low_safety_margin_not_1(self):
        state = 3, set('abcdef'), 5, set('abcdef')
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abcdef"), sync_wanted=3)), [
            ('quorum', 4, set('abcdef')),  # Adjust quorum requirements
            ('sync', 3, set('abcdef')),    # Reduce synchronization
        ])
        self.assertEqual(list(QuorumStateResolver(*state, active=set("abcdef"), sync_wanted=1)), [
            ('quorum', 6, set('abcdef')),  # Adjust quorum requirements
            ('sync', 1, set('abcdef')),    # Reduce synchronization
        ])

    def test_quorum_update(self):
        resolver = QuorumStateResolver(2, set('abc'), 2, set('abc'), set('abc'), 2)
        self.assertRaises(QuorumError, resolver.quorum_update, 0, set())

    def test_sync_update(self):
        resolver = QuorumStateResolver(2, set('abc'), 2, set('abc'), set('abc'), 2)
        self.assertRaises(QuorumError, resolver.sync_update, 0, set())
