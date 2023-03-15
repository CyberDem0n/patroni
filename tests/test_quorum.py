import unittest

from patroni.quorum import QuorumStateResolver, QuorumError


class QuorumTest(unittest.TestCase):

    def test_a(self):
        leader = 'a'
        state = leader, 0, set("bc"), 2, set("bc"), 2
        self.assertEqual(list(QuorumStateResolver(*state, active=set("bcde"), sync_wanted=4, leader_wanted=leader)), [
            ('sync', leader, 4, set('bcde')),
            ('quorum', leader, 2, set('bcde')),
        ])

    def test_1111(self):
        leader = 'a'
        # Add node
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set(),
                                                  numsync=0, sync=set(), numsync_confimed=0,
                                                  active=set('b'), sync_wanted=2, leader_wanted=leader)), [
            ('sync', leader, 1, set('b')),
            ('restart', leader, 0, set()),
        ])
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set(),
                                                  numsync=1, sync=set('b'), numsync_confimed=1,
                                                  active=set("b"), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 0, set('b'))
        ])

        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set(),
                                                  numsync=0, sync=set(), numsync_confimed=0,
                                                  active=set("bcde"), sync_wanted=2, leader_wanted=leader)), [
            ('sync', leader, 2, set('bcde')),
            ('restart', leader, 0, set()),
        ])
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set(),
                                                  numsync=2, sync=set('bcde'), numsync_confimed=1,
                                                  active=set("bcde"), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 3, set('bcde')),
        ])

    def test_1222(self):
        """2 node cluster"""
        leader = 'a'

        # Active set matches state
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=1, sync=set('b'), numsync_confimed=1,
                                                  active=set('b'), sync_wanted=2, leader_wanted=leader)), [
        ])

        # Add node by increasing quorum
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=1, sync=set('b'), numsync_confimed=1,
                                                  active=set('bc'), sync_wanted=1, leader_wanted=leader)), [
            ('quorum', leader, 1, set('bc')),
            ('sync', leader, 1, set('bc')),
        ])

        # Add node by increasing sync
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=1, sync=set('b'), numsync_confimed=1,
                                                  active=set('bc'), sync_wanted=2, leader_wanted=leader)), [
            ('sync', leader, 2, set('bc')),
            ('quorum', leader, 1, set('bc')),
        ])
        # Reduce quorum after added node caught up
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=1, voters=set('bc'),
                                                  numsync=2, sync=set('bc'), numsync_confimed=2,
                                                  active=set('bc'), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 0, set('bc')),
        ])

        # Add multiple nodes by increasing both sync and quorum
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=1, sync=set('b'), numsync_confimed=1,
                                                  active=set('bcde'), sync_wanted=2, leader_wanted=leader)), [
            ('sync', leader, 2, set('bc')),
            ('quorum', leader, 3, set('bcde')),
            ('sync', leader, 2, set('bcde')),
        ])
        # Reduce quorum after added nodes caught up
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=3, voters=set('bcde'),
                                                  numsync=2, sync=set('bcde'), numsync_confimed=3,
                                                  active=set('bcde'), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 2, set('bcde')),
        ])

        # Primary is alone
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=1, sync=set('b'), numsync_confimed=0,
                                                  active=set(), sync_wanted=1, leader_wanted=leader)), [
            ('quorum', leader, 0, set()),
            ('sync', leader, 0, set()),
        ])

        # Swap out sync replica
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=1, sync=set('b'), numsync_confimed=0,
                                                  active=set('c'), sync_wanted=1, leader_wanted=leader)), [
            ('quorum', leader, 0, set()),
            ('sync', leader, 1, set('c')),
            ('restart', leader, 0, set()),
        ])
        # Update quorum when added node caught up
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set(),
                                                  numsync=1, sync=set('c'), numsync_confimed=1,
                                                  active=set('c'), sync_wanted=1, leader_wanted=leader)), [
            ('quorum', leader, 0, set('c')),
        ])

    def test_1233(self):
        """Interrupted transition from 2 node cluster to 3 node fully sync cluster"""
        leader = 'a'

        # Node c went away, transition back to 2 node cluster
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=2, sync=set('bc'), numsync_confimed=1,
                                                  active=set('b'), sync_wanted=2, leader_wanted=leader)), [
            ('sync', leader, 1, set('b')),
        ])

        # Node c is available transition to larger quorum set, but not yet caught up.
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=2, sync=set('bc'), numsync_confimed=1,
                                                  active=set('bc'), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 1, set('bc')),
        ])

        # Add in a new node at the same time, but node c didn't caught up yet
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=2, sync=set('bc'), numsync_confimed=1,
                                                  active=set('bcd'), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 2, set('bcd')),
            ('sync', leader, 2, set('bcd')),
        ])
        # All sync nodes caught up, reduce quorum
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=2, voters=set('bcd'),
                                                  numsync=2, sync=set('bcd'), numsync_confimed=3,
                                                  active=set('bcd'), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 1, set('bcd')),
        ])

        # Change replication factor at the same time
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=0, voters=set('b'),
                                                  numsync=2, sync=set('bc'), numsync_confimed=1,
                                                  active=set('bc'), sync_wanted=1, leader_wanted=leader)), [
            ('quorum', leader, 1, set('bc')),
            ('sync', leader, 1, set('bc')),
        ])

    def test_2322(self):
        """Interrupted transition from 2 node cluster to 3 node cluster with replication factor 2"""
        leader = 'a'

        # Node c went away, transition back to 2 node cluster
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=1, voters=set('bc'),
                                                  numsync=1, sync=set('b'), numsync_confimed=1,
                                                  active=set('b'), sync_wanted=1, leader_wanted=leader)), [
            ('quorum', leader, 0, set('b')),
        ])

        # Node c is available transition to larger quorum set.
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=1, voters=set('bc'),
                                                  numsync=1, sync=set('b'), numsync_confimed=1,
                                                  active=set('bc'), sync_wanted=1, leader_wanted=leader)), [
            ('sync', leader, 1, set('bc')),
        ])

        # Add in a new node at the same time
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=1, voters=set('bc'),
                                                  numsync=1, sync=set('b'), numsync_confimed=1,
                                                  active=set('bcd'), sync_wanted=1, leader_wanted=leader)), [
            ('sync', leader, 1, set('bc')),
            ('quorum', leader, 2, set('bcd')),
            ('sync', leader, 1, set('bcd')),
        ])

        # Convert to a fully synced cluster
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=1, voters=set('bc'),
                                                  numsync=1, sync=set('b'), numsync_confimed=1,
                                                  active=set('bc'), sync_wanted=2, leader_wanted=leader)), [
            ('sync', leader, 2, set('bc')),
            ('restart', leader, 1, set('bc')),
        ])
        # Reduce quorum after all nodes caught up
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=1, voters=set('bc'),
                                                  numsync=2, sync=set('bc'), numsync_confimed=2,
                                                  active=set('bc'), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 0, set('bc')),
        ])

    def test_3535(self):
        leader = 'a'

        # remove nodes
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=2, voters=set('bcde'),
                                                  numsync=2, sync=set('bcde'), numsync_confimed=2,
                                                  active=set('bc'), sync_wanted=2, leader_wanted=leader)), [
            ('sync', leader, 2, set('bc')),
            ('quorum', leader, 0, set('bc')),
        ])
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=2, voters=set('bcde'),
                                                  numsync=2, sync=set('bcde'), numsync_confimed=3,
                                                  active=set('bcd'), sync_wanted=2, leader_wanted=leader)), [
            ('sync', leader, 2, set('bcd')),
            ('quorum', leader, 1, set('bcd')),
        ])

        # remove nodes and decrease sync
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=2, voters=set('bcde'),
                                                  numsync=2, sync=set('bcde'), numsync_confimed=2,
                                                  active=set('bc'), sync_wanted=1, leader_wanted=leader)), [
            ('sync', leader, 2, set('bc')),
            ('quorum', leader, 1, set('bc')),
            ('sync', leader, 1, set('bc')),
        ])

        # Increase replication factor and decrease quorum
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=2, voters=set('bcde'),
                                                  numsync=2, sync=set('bcde'), numsync_confimed=2,
                                                  active=set('bcde'), sync_wanted=3, leader_wanted=leader)), [
            ('sync', leader, 3, set('bcde')),
            ('restart', leader, 2, set('bcde')),
        ])
        # decrease quorum after more nodes caught up
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=2, voters=set('bcde'),
                                                  numsync=3, sync=set('bcde'), numsync_confimed=3,
                                                  active=set('bcde'), sync_wanted=3, leader_wanted=leader)), [
            ('quorum', leader, 1, set('bcde')),
        ])

        # Add node with decreasing sync and increasing quorum
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=2, voters=set('bcde'),
                                                  numsync=2, sync=set('bcde'), numsync_confimed=2,
                                                  active=set('bcdef'), sync_wanted=1, leader_wanted=leader)), [
            ('quorum', leader, 4, set('bcdef')),   # increase quorum by 2, 1 for added node and another for reduced sync
            ('sync', leader, 1, set('bcdef')),     # now reduce replication factor to requested value
        ])

        # Remove node with increasing sync and decreasing quorum
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=2, voters=set('bcde'),
                                                  numsync=2, sync=set('bcde'), numsync_confimed=2,
                                                  active=set('bcd'), sync_wanted=3, leader_wanted=leader)), [
            ('sync', leader, 3, set('bcd')),       # node e removed from sync wth replication factor increase
            ('quorum', leader, 1, set('bcd')),     # node e removed from voters with quorum decrease
        ])

    def test_nonsync_promotion(self):
        # Beginning state: 1 of bc in sync. e.g. (a primary, ssn = ANY 1 (b c))
        # a fails, d sees b and c, knows that it is in sync and decides to promote.
        # We include in sync state former primary increasing replication factor
        # and let situation resolve. Node d ssn=ANY 1 (b c)
        leader = 'd'
        self.assertEqual(list(QuorumStateResolver(leader='a', quorum=1, voters=set('bc'),
                                                  numsync=2, sync=set('abc'), numsync_confimed=0,
                                                  active=set('bc'), sync_wanted=1, leader_wanted=leader)), [
            ('quorum', leader, 1, set('abc')),  # Set ourselves to be a member of the quorum
            ('sync', leader, 2, set('bc')),     # Remove a from being synced to.
            ('quorum', leader, 1, set('bc')),   # Remove a from quorum
            ('sync', leader, 1, set('bc')),     # Can now reduce replication factor to original value
        ])

    def test_invalid_states(self):
        leader = 'a'

        # Main invariant is not satisfied, system is in an unsafe state
        resolver = QuorumStateResolver(leader=leader, quorum=0, voters=set('bc'),
                                       numsync=1, sync=set('bc'), numsync_confimed=1,
                                       active=set('bc'), sync_wanted=1, leader_wanted=leader)
        self.assertRaises(QuorumError, resolver.check_invariants)
        self.assertEqual(list(resolver), [
            ('quorum', leader, 1, set('bc'))
        ])

        # Quorum and sync states mismatched, somebody other than Patroni modified system state
        resolver = QuorumStateResolver(leader=leader, quorum=1, voters=set('bc'),
                                       numsync=2, sync=set('bd'), numsync_confimed=1,
                                       active=set('bd'), sync_wanted=1, leader_wanted=leader)
        self.assertRaises(QuorumError, resolver.check_invariants)
        self.assertEqual(list(resolver), [
            ('quorum', leader, 1, set('bd')),
            ('sync', leader, 1, set('bd')),
        ])

    def test_sync_high_quorum_low_safety_margin_high(self):
        leader = 'a'
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=2, voters=set('bcdef'),
                                                  numsync=4, sync=set('bcdef'), numsync_confimed=3,
                                                  active=set('bcdef'), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 4, set('bcdef')),  # Adjust quorum requirements
            ('sync', leader, 2, set('bcdef')),    # Reduce synchronization
        ])
        self.assertEqual(list(QuorumStateResolver(leader=leader, quorum=4, voters=set('bcdef'),
                                                  numsync=2, sync=set('bcdef'), numsync_confimed=4,
                                                  active=set('bcdef'), sync_wanted=2, leader_wanted=leader)), [
            ('quorum', leader, 3, set('bcdef')),  # Adjust quorum requirements
        ])

    def test_quorum_update(self):
        resolver = QuorumStateResolver(leader='a', quorum=1, voters=set('bc'), numsync=1, sync=set('bc'),
                                       numsync_confimed=1, active=set('bc'), sync_wanted=1, leader_wanted='a')
        self.assertRaises(QuorumError, resolver.quorum_update, -1, set())
        self.assertRaises(QuorumError, resolver.quorum_update, 1, set())

    def test_sync_update(self):
        resolver = QuorumStateResolver(leader='a', quorum=1, voters=set('bc'), numsync=1, sync=set('bc'),
                                       numsync_confimed=1, active=set('bc'), sync_wanted=1, leader_wanted='a')
        self.assertRaises(QuorumError, resolver.sync_update, -1, set())
        self.assertRaises(QuorumError, resolver.sync_update, 1, set())
