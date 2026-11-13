"""Core implementation of Swarm Consensus Raft Engine."""
import time
import random
from enum import Enum
from typing import List, Dict, Optional, Tuple, Any

class RaftRole(str, Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

class LogEntry:
    def __init__(self, term: int, index: int, command: str, payload: Any = None):
        self.term = term
        self.index = index
        self.command = command
        self.payload = payload or {}
        self.timestamp = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "payload": self.payload,
            "timestamp": self.timestamp
        }

class RaftNode:
    def __init__(self, node_id: str, peers: Optional[List[str]] = None):
        self.node_id = node_id
        self.peers = peers or []
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0
        self.role = RaftRole.FOLLOWER
        self.state_machine: List[Dict[str, Any]] = []
        self.votes_received: set = set()

    @property
    def last_log_index(self) -> int:
        return len(self.log)

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def start_election(self) -> Dict[str, Any]:
        self.role = RaftRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        return {
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term
        }

    def handle_request_vote(self, term: int, candidate_id: str, last_log_index: int, last_log_term: int) -> Tuple[int, bool]:
        if term > self.current_term:
            self.current_term = term
            self.role = RaftRole.FOLLOWER
            self.voted_for = None

        vote_granted = False
        if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
            # Check up-to-date log
            if (last_log_term > self.last_log_term) or (last_log_term == self.last_log_term and last_log_index >= self.last_log_index):
                vote_granted = True
                self.voted_for = candidate_id

        return self.current_term, vote_granted

    def receive_vote(self, voter_id: str, term: int, granted: bool) -> bool:
        if self.role != RaftRole.CANDIDATE or term != self.current_term:
            return False
        if granted:
            self.votes_received.add(voter_id)
            total_nodes = len(self.peers) + 1
            if len(self.votes_received) > total_nodes // 2:
                self.role = RaftRole.LEADER
                return True
        return False

    def propose_command(self, command: str, payload: Any = None) -> Optional[LogEntry]:
        if self.role != RaftRole.LEADER:
            return None
        idx = self.last_log_index + 1
        entry = LogEntry(term=self.current_term, index=idx, command=command, payload=payload)
        self.log.append(entry)
        return entry

    def handle_append_entries(self, term: int, leader_id: str, prev_log_index: int, prev_log_term: int,
                              entries: List[LogEntry], leader_commit: int) -> Tuple[int, bool]:
        if term < self.current_term:
            return self.current_term, False

        self.current_term = term
        self.role = RaftRole.FOLLOWER

        # Log consistency check
        if prev_log_index > 0:
            if prev_log_index > len(self.log):
                return self.current_term, False
            if self.log[prev_log_index - 1].term != prev_log_term:
                return self.current_term, False

        # Overwrite conflicting entries & append new
        insert_idx = prev_log_index
        for e in entries:
            if insert_idx < len(self.log):
                if self.log[insert_idx].term != e.term:
                    self.log = self.log[:insert_idx]
                    self.log.append(e)
            else:
                self.log.append(e)
            insert_idx += 1

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
            self._apply_entries()

        return self.current_term, True

    def commit_entries(self, match_indices: Dict[str, int]) -> int:
        if self.role != RaftRole.LEADER:
            return self.commit_index

        total_nodes = len(self.peers) + 1
        for N in range(len(self.log), self.commit_index, -1):
            if self.log[N - 1].term == self.current_term:
                matches = 1  # leader itself
                for p_id, m_idx in match_indices.items():
                    if m_idx >= N:
                        matches += 1
                if matches > total_nodes // 2:
                    self.commit_index = N
                    self._apply_entries()
                    break
        return self.commit_index

    def _apply_entries(self):
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            self.state_machine.append(entry.to_dict())

class RaftCluster:
    def __init__(self, node_ids: List[str]):
        self.nodes: Dict[str, RaftNode] = {}
        for nid in node_ids:
            peers = [p for p in node_ids if p != nid]
            self.nodes[nid] = RaftNode(nid, peers)

    def elect_leader(self, preferred_candidate: Optional[str] = None) -> Optional[str]:
        cand_id = preferred_candidate or list(self.nodes.keys())[0]
        cand = self.nodes[cand_id]
        vote_req = cand.start_election()
        for peer_id in cand.peers:
            peer = self.nodes[peer_id]
            term, granted = peer.handle_request_vote(
                vote_req["term"], cand_id, vote_req["last_log_index"], vote_req["last_log_term"]
            )
            cand.receive_vote(peer_id, term, granted)
        return cand_id if cand.role == RaftRole.LEADER else None

    def broadcast_command(self, leader_id: str, command: str, payload: Any = None) -> bool:
        leader = self.nodes.get(leader_id)
        if not leader or leader.role != RaftRole.LEADER:
            return False
        entry = leader.propose_command(command, payload)
        if not entry:
            return False

        match_indices = {}
        for peer_id in leader.peers:
            peer = self.nodes[peer_id]
            prev_idx = entry.index - 1
            prev_term = leader.log[prev_idx - 1].term if prev_idx > 0 else 0
            _, success = peer.handle_append_entries(
                leader.current_term, leader_id, prev_idx, prev_term, [entry], leader.commit_index
            )
            if success:
                match_indices[peer_id] = entry.index

        committed = leader.commit_entries(match_indices)
        # Notify peers of commit
        for peer_id in leader.peers:
            self.nodes[peer_id].handle_append_entries(
                leader.current_term, leader_id, entry.index, entry.term, [], committed
            )
        return committed == entry.index
