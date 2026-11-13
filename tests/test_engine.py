"""Unit tests for swarm-consensus-raft-engine."""
import unittest
from swarm_consensus_raft_engine.engine import RaftNode, RaftRole, RaftCluster

class TestSwarmConsensusRaftEngine(unittest.TestCase):
    def setUp(self):
        self.node_ids = ["node-1", "node-2", "node-3"]
        self.cluster = RaftCluster(self.node_ids)

    def test_initial_state(self):
        for node in self.cluster.nodes.values():
            self.assertEqual(node.role, RaftRole.FOLLOWER)
            self.assertEqual(node.current_term, 0)
            self.assertEqual(len(node.log), 0)

    def test_leader_election(self):
        leader_id = self.cluster.elect_leader("node-1")
        self.assertEqual(leader_id, "node-1")
        self.assertEqual(self.cluster.nodes["node-1"].role, RaftRole.LEADER)
        self.assertEqual(self.cluster.nodes["node-1"].current_term, 1)

    def test_replicate_command(self):
        leader_id = self.cluster.elect_leader("node-1")
        ok = self.cluster.broadcast_command(leader_id, "UPDATE_PARAM", {"threshold": 0.85})
        self.assertTrue(ok)
        leader = self.cluster.nodes[leader_id]
        self.assertEqual(leader.commit_index, 1)
        self.assertEqual(len(leader.state_machine), 1)
        for peer_id in leader.peers:
            peer = self.cluster.nodes[peer_id]
            self.assertEqual(peer.commit_index, 1)
            self.assertEqual(len(peer.state_machine), 1)

    def test_reject_vote_lower_term(self):
        n1 = self.cluster.nodes["node-1"]
        n1.current_term = 5
        term, granted = n1.handle_request_vote(term=4, candidate_id="node-2", last_log_index=0, last_log_term=0)
        self.assertFalse(granted)
        self.assertEqual(term, 5)

if __name__ == "__main__":
    unittest.main()
