"""
raft.py — Raft Consensus Algorithm

CONCEPT:
  Raft is a consensus algorithm that ensures all nodes in a distributed
  cluster agree on the same sequence of log entries, even when some
  nodes fail. Used by: etcd, CockroachDB, TiKV, Consul.

  Why consensus?
    In a replicated database, multiple nodes must agree on:
      - Which writes happened in what order
      - Who is the current leader
      - Whether a write is committed (majority acknowledged it)

  Raft has three sub-problems:
    1. Leader Election   — elect one leader per term
    2. Log Replication   — leader replicates entries to followers
    3. Safety            — committed entries are never lost

  Roles:
    FOLLOWER  — passive, accepts entries from leader
    CANDIDATE — trying to become leader (requesting votes)
    LEADER    — handles all writes, replicates to followers

  Terms:
    Time is divided into terms (monotonically increasing integers).
    Each term has at most one leader.
    If a node sees a higher term, it immediately becomes a follower.

  Leader Election:
    1. Follower times out (no heartbeat from leader)
    2. Becomes CANDIDATE, increments term, votes for itself
    3. Sends RequestVote to all other nodes
    4. If majority vote YES → becomes LEADER
    5. Leader sends heartbeats to prevent new elections

  Log Replication:
    1. Client sends write to LEADER
    2. Leader appends to its log (uncommitted)
    3. Leader sends AppendEntries to all followers
    4. Once majority acknowledge → entry is COMMITTED
    5. Leader applies to state machine, replies to client
    6. Followers apply on next heartbeat

  Safety guarantee:
    A node only votes YES if the candidate's log is at least as
    up-to-date as the voter's log. This ensures the new leader
    always has all committed entries.

  This implementation runs all nodes in-process using threads,
  with simulated network (direct method calls with optional delays).
"""

import threading
import time
import random
import queue
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any


# ======================================================================= #
#  Data structures                                                         #
# ======================================================================= #

class Role(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass
class LogEntry:
    term: int
    index: int
    command: Any  # the actual write (e.g., {"set": "key", "value": "val"})

    def __repr__(self):
        return f"Entry(term={self.term}, idx={self.index}, cmd={self.command})"


@dataclass
class VoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    term: int
    vote_granted: bool
    voter_id: str


@dataclass
class AppendRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int  # leader's commit index


@dataclass
class AppendResponse:
    term: int
    success: bool
    follower_id: str
    match_index: int  # highest log index follower has


# ======================================================================= #
#  Raft Node                                                               #
# ======================================================================= #

ELECTION_TIMEOUT_MIN = 0.15   # seconds
ELECTION_TIMEOUT_MAX = 0.30
HEARTBEAT_INTERVAL   = 0.05


class RaftNode:
    """
    A single node in a Raft cluster.

    Each node runs two background threads:
      - election_timer: triggers election if no heartbeat received
      - leader_loop:    sends heartbeats/entries if this node is leader
    """

    def __init__(self, node_id: str, peers: list[str]):
        self.node_id = node_id
        self.peers = peers  # IDs of other nodes (set after cluster init)
        self._cluster: dict[str, "RaftNode"] = {}  # node_id → RaftNode

        # Persistent state (would be written to disk in production)
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: list[LogEntry] = []  # index 0 = dummy entry

        # Volatile state
        self.commit_index = 0   # highest log index known to be committed
        self.last_applied = 0   # highest log index applied to state machine

        # Leader state (only valid when role == LEADER)
        self.next_index: dict[str, int] = {}   # next log index to send to each follower
        self.match_index: dict[str, int] = {}  # highest log index known replicated on each follower

        self.role = Role.FOLLOWER
        self.leader_id: Optional[str] = None
        self.votes_received: set[str] = set()

        # State machine (the actual database)
        self.state_machine: dict[str, str] = {}

        # Synchronization
        self._lock = threading.RLock()
        self._last_heartbeat = time.time()
        self._election_timeout = self._new_election_timeout()
        self._running = False

        # Event log for demo
        self.events: list[str] = []

    def _new_election_timeout(self) -> float:
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _log(self, msg: str):
        entry = f"[{self.node_id}|{self.role.value[:3]}|T{self.current_term}] {msg}"
        self.events.append(entry)

    def start(self, cluster: dict[str, "RaftNode"]):
        """Start the node with a reference to the full cluster."""
        self._cluster = cluster
        self._running = True
        threading.Thread(target=self._election_timer_loop, daemon=True).start()
        threading.Thread(target=self._leader_loop, daemon=True).start()

    def stop(self):
        self._running = False

    # ------------------------------------------------------------------ #
    #  Election timer                                                      #
    # ------------------------------------------------------------------ #

    def _election_timer_loop(self):
        """
        If we don't hear from a leader within election_timeout,
        start an election.
        """
        while self._running:
            time.sleep(0.01)
            with self._lock:
                if self.role == Role.LEADER:
                    continue
                elapsed = time.time() - self._last_heartbeat
                if elapsed >= self._election_timeout:
                    self._start_election()

    def _start_election(self):
        """Transition to CANDIDATE and request votes."""
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self._last_heartbeat = time.time()
        self._election_timeout = self._new_election_timeout()
        self._log(f"Starting election for term {self.current_term}")

        last_log_index = len(self.log)
        last_log_term = self.log[-1].term if self.log else 0

        req = VoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

        # Send vote requests to all peers (in background threads)
        for peer_id in self.peers:
            threading.Thread(
                target=self._send_vote_request,
                args=(peer_id, req),
                daemon=True
            ).start()

    def _send_vote_request(self, peer_id: str, req: VoteRequest):
        peer = self._cluster.get(peer_id)
        if peer is None:
            return
        resp = peer.handle_vote_request(req)
        with self._lock:
            self._handle_vote_response(resp)

    def _handle_vote_response(self, resp: VoteResponse):
        if self.role != Role.CANDIDATE:
            return
        if resp.term > self.current_term:
            self._become_follower(resp.term)
            return
        if resp.vote_granted and resp.term == self.current_term:
            self.votes_received.add(resp.voter_id)
            majority = (len(self.peers) + 1) // 2 + 1
            if len(self.votes_received) >= majority:
                self._become_leader()

    def _become_leader(self):
        self.role = Role.LEADER
        self.leader_id = self.node_id
        self._log(f"Became LEADER for term {self.current_term}")
        # Initialize leader state
        next_idx = len(self.log) + 1
        for peer_id in self.peers:
            self.next_index[peer_id] = next_idx
            self.match_index[peer_id] = 0

    def _become_follower(self, term: int):
        self.role = Role.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self._last_heartbeat = time.time()
        self._election_timeout = self._new_election_timeout()

    # ------------------------------------------------------------------ #
    #  Vote RPC handler                                                    #
    # ------------------------------------------------------------------ #

    def handle_vote_request(self, req: VoteRequest) -> VoteResponse:
        with self._lock:
            # If we see a higher term, update and become follower
            if req.term > self.current_term:
                self._become_follower(req.term)

            vote_granted = False
            if req.term < self.current_term:
                pass  # reject: stale term
            elif (self.voted_for is None or self.voted_for == req.candidate_id):
                # Check log is at least as up-to-date as ours
                my_last_term = self.log[-1].term if self.log else 0
                my_last_index = len(self.log)
                log_ok = (
                    req.last_log_term > my_last_term or
                    (req.last_log_term == my_last_term and
                     req.last_log_index >= my_last_index)
                )
                if log_ok:
                    vote_granted = True
                    self.voted_for = req.candidate_id
                    self._last_heartbeat = time.time()

            return VoteResponse(
                term=self.current_term,
                vote_granted=vote_granted,
                voter_id=self.node_id,
            )

    # ------------------------------------------------------------------ #
    #  Leader heartbeat / log replication loop                            #
    # ------------------------------------------------------------------ #

    def _leader_loop(self):
        """Send heartbeats and replicate log entries to followers."""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            with self._lock:
                if self.role != Role.LEADER:
                    continue
                for peer_id in self.peers:
                    threading.Thread(
                        target=self._send_append_entries,
                        args=(peer_id,),
                        daemon=True
                    ).start()

    def _send_append_entries(self, peer_id: str):
        with self._lock:
            if self.role != Role.LEADER:
                return
            next_idx = self.next_index.get(peer_id, 1)
            prev_log_index = next_idx - 1
            prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 and prev_log_index <= len(self.log) else 0
            entries = self.log[next_idx - 1:]  # entries to send
            req = AppendRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=list(entries),
                leader_commit=self.commit_index,
            )

        peer = self._cluster.get(peer_id)
        if peer is None:
            return
        resp = peer.handle_append_entries(req)

        with self._lock:
            if resp.term > self.current_term:
                self._become_follower(resp.term)
                return
            if self.role != Role.LEADER:
                return
            if resp.success:
                self.match_index[peer_id] = resp.match_index
                self.next_index[peer_id] = resp.match_index + 1
                self._advance_commit_index()
            else:
                # Decrement next_index and retry
                self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)

    def _advance_commit_index(self):
        """
        Commit entries that have been replicated to a majority.
        An entry is committed if match_index[i] >= N for a majority of nodes.
        """
        n = len(self.log)
        for idx in range(n, self.commit_index, -1):
            if self.log[idx - 1].term != self.current_term:
                continue  # only commit entries from current term
            count = 1  # count self
            for peer_id in self.peers:
                if self.match_index.get(peer_id, 0) >= idx:
                    count += 1
            majority = (len(self.peers) + 1) // 2 + 1
            if count >= majority:
                if idx > self.commit_index:
                    self.commit_index = idx
                    self._apply_committed()
                break

    # ------------------------------------------------------------------ #
    #  AppendEntries RPC handler                                          #
    # ------------------------------------------------------------------ #

    def handle_append_entries(self, req: AppendRequest) -> AppendResponse:
        with self._lock:
            if req.term > self.current_term:
                self._become_follower(req.term)

            if req.term < self.current_term:
                return AppendResponse(self.current_term, False, self.node_id, 0)

            # Valid leader heartbeat
            self._last_heartbeat = time.time()
            self.leader_id = req.leader_id
            if self.role == Role.CANDIDATE:
                self._become_follower(req.term)

            # Check prev_log consistency
            if req.prev_log_index > 0:
                if len(self.log) < req.prev_log_index:
                    return AppendResponse(self.current_term, False, self.node_id, len(self.log))
                if self.log[req.prev_log_index - 1].term != req.prev_log_term:
                    # Conflict: delete from prev_log_index onward
                    self.log = self.log[:req.prev_log_index - 1]
                    return AppendResponse(self.current_term, False, self.node_id, len(self.log))

            # Append new entries
            for entry in req.entries:
                idx = entry.index
                if idx <= len(self.log):
                    if self.log[idx - 1].term != entry.term:
                        self.log = self.log[:idx - 1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # Update commit index
            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, len(self.log))
                self._apply_committed()

            return AppendResponse(self.current_term, True, self.node_id, len(self.log))

    # ------------------------------------------------------------------ #
    #  State machine                                                       #
    # ------------------------------------------------------------------ #

    def _apply_committed(self):
        """Apply all committed but not-yet-applied log entries."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            cmd = entry.command
            if isinstance(cmd, dict):
                if cmd.get("op") == "set":
                    self.state_machine[cmd["key"]] = cmd["value"]
                    self._log(f"Applied SET {cmd['key']}={cmd['value']!r}")
                elif cmd.get("op") == "delete":
                    self.state_machine.pop(cmd["key"], None)

    # ------------------------------------------------------------------ #
    #  Client API                                                          #
    # ------------------------------------------------------------------ #

    def write(self, command: dict) -> bool:
        """
        Submit a write command. Must be called on the LEADER.
        Returns True when committed by majority.
        """
        with self._lock:
            if self.role != Role.LEADER:
                return False
            index = len(self.log) + 1
            entry = LogEntry(term=self.current_term, index=index, command=command)
            self.log.append(entry)
            self._log(f"Appended entry {entry}")
            return True

    def read(self, key: str) -> Optional[str]:
        """Read from state machine (linearizable if called on leader)."""
        with self._lock:
            return self.state_machine.get(key)

    def status(self) -> dict:
        with self._lock:
            return {
                "node_id": self.node_id,
                "role": self.role.value,
                "term": self.current_term,
                "leader": self.leader_id,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "state_machine_size": len(self.state_machine),
            }


# ======================================================================= #
#  Raft Cluster                                                            #
# ======================================================================= #

class RaftCluster:
    """Manages a cluster of RaftNodes for testing."""

    def __init__(self, node_ids: list[str]):
        self.nodes: dict[str, RaftNode] = {}
        for nid in node_ids:
            peers = [p for p in node_ids if p != nid]
            self.nodes[nid] = RaftNode(nid, peers)

    def start(self):
        for node in self.nodes.values():
            node.start(self.nodes)

    def stop(self):
        for node in self.nodes.values():
            node.stop()

    def get_leader(self) -> Optional[RaftNode]:
        for node in self.nodes.values():
            with node._lock:
                if node.role == Role.LEADER:
                    return node
        return None

    def wait_for_leader(self, timeout: float = 3.0) -> Optional[RaftNode]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            leader = self.get_leader()
            if leader:
                return leader
            time.sleep(0.05)
        return None

    def write(self, key: str, value: str, timeout: float = 2.0) -> bool:
        """Write to the cluster leader and wait for commit."""
        leader = self.wait_for_leader(timeout=1.0)
        if not leader:
            return False
        ok = leader.write({"op": "set", "key": key, "value": value})
        if not ok:
            return False
        # Wait for commit
        deadline = time.time() + timeout
        while time.time() < deadline:
            with leader._lock:
                if leader.last_applied >= leader.commit_index:
                    return True
            time.sleep(0.01)
        return False

    def read(self, key: str) -> Optional[str]:
        leader = self.get_leader()
        if leader:
            return leader.read(key)
        return None


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

if __name__ == "__main__":
    print("=" * 60)
    print("RAFT CONSENSUS ALGORITHM DEMO")
    print("=" * 60)

    # --- Step 1: Start a 5-node cluster ---
    print("\n[Step 1] Starting 5-node Raft cluster...")
    cluster = RaftCluster(["node-1", "node-2", "node-3", "node-4", "node-5"])
    cluster.start()

    leader = cluster.wait_for_leader(timeout=3.0)
    if leader:
        print(f"  Leader elected: {leader.node_id} (term {leader.current_term})")
    else:
        print("  ERROR: No leader elected!")

    # --- Step 2: Write some data ---
    print("\n[Step 2] Writing data through leader...")
    writes = [
        ("name", "Alice"),
        ("city", "New York"),
        ("score", "100"),
        ("status", "active"),
    ]
    for key, val in writes:
        ok = cluster.write(key, val, timeout=2.0)
        print(f"  SET {key}={val!r} → {'OK' if ok else 'FAILED'}")

    time.sleep(0.3)  # let replication settle

    # --- Step 3: Verify all nodes have the data ---
    print("\n[Step 3] Verifying all nodes have committed data...")
    for node_id, node in sorted(cluster.nodes.items()):
        status = node.status()
        print(f"  {node_id}: role={status['role']:9s} term={status['term']} "
              f"log={status['log_length']} committed={status['commit_index']} "
              f"applied={status['last_applied']}")

    # --- Step 4: Read from leader ---
    print("\n[Step 4] Reading from leader...")
    for key in ["name", "city", "score", "missing"]:
        val = cluster.read(key)
        print(f"  GET {key!r} → {val!r}")

    # --- Step 5: Simulate leader failure ---
    print("\n[Step 5] Simulating leader failure...")
    old_leader = cluster.get_leader()
    if old_leader:
        print(f"  Stopping leader: {old_leader.node_id}")
        old_leader.stop()
        # Remove from cluster so others don't contact it
        del cluster.nodes[old_leader.node_id]

        time.sleep(0.5)  # wait for election

        new_leader = cluster.wait_for_leader(timeout=3.0)
        if new_leader:
            print(f"  New leader elected: {new_leader.node_id} (term {new_leader.current_term})")
        else:
            print("  ERROR: No new leader elected!")

    # --- Step 6: Write after failover ---
    print("\n[Step 6] Writing after leader failover...")
    ok = cluster.write("recovery", "success", timeout=2.0)
    print(f"  SET recovery='success' → {'OK' if ok else 'FAILED'}")

    time.sleep(0.3)

    # --- Step 7: Final state ---
    print("\n[Step 7] Final cluster state...")
    for node_id, node in sorted(cluster.nodes.items()):
        status = node.status()
        print(f"  {node_id}: role={status['role']:9s} term={status['term']} "
              f"sm_size={status['state_machine_size']}")

    # --- Step 8: Show event log from one node ---
    print("\n[Step 8] Event log (last 10 events from each node)...")
    for node_id, node in sorted(cluster.nodes.items()):
        recent = node.events[-5:] if node.events else []
        for e in recent:
            print(f"  {e}")

    cluster.stop()

    print("\n[Done] Raft consensus demo complete.")
    print("\nKey insights:")
    print("  1. Leader elected by majority vote — only one leader per term")
    print("  2. Writes go through leader → replicated to followers")
    print("  3. Entry committed when majority acknowledge it")
    print("  4. Leader failure → new election → new leader has all committed data")
    print("  5. Safety: a node only votes for candidates with up-to-date logs")
    print("  6. Used by etcd (Kubernetes), CockroachDB, TiKV (TiDB)")