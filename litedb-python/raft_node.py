"""
raft_node.py — one Raft replica of one replication group, over the RPC transport.

This is the in-process `raft.py` algorithm (election, log replication, commit advancement, the
up-to-date-log voting safety rule) lifted onto real RPC and made production-shaped in three ways:

  1. Transport is injected — `send_fn(peer_node_id, kind, payload)` does a network RPC instead of
     a direct method call, so replicas live in different processes.
  2. State is persisted — current_term, voted_for, and the log are written to disk with fsync, so
     a restarted replica recovers exactly where it left off (Raft's durability requirement).
  3. Committed entries are handed to an injected `apply_fn` (the real storage engine), not a dict.

A physical node runs MANY of these — one group per shard it replicates (multi-raft). Each group
is addressed by (node_id, group_id); the owning server routes an incoming RPC to the right group.

Single-machine scope: this is a correct, persisted, networked Raft. It is not hardened for the
full failure matrix (no pre-vote, no membership changes, snapshot install is log-based) — see
ROADMAP.md.
"""

import json
import os
import random
import threading
import time
from enum import Enum
from typing import Callable, Optional, TextIO

# (peer_node_id, kind, payload) -> response dict; kind in {"vote", "append"}
SendFn = Callable[[str, str, dict], dict]
# (index, command) -> apply the committed command to the state machine
ApplyFn = Callable[[int, dict], None]

# Timing relaxed vs. the in-process demo: slower heartbeats keep the dashboard readable and cut
# RPC churn, while still electing within ~1s.
ELECTION_TIMEOUT_MIN = 0.6
ELECTION_TIMEOUT_MAX = 1.2
HEARTBEAT_INTERVAL = 0.15
RPC_TIMEOUT = 0.5  # short, so a hung peer can't stall a heartbeat round


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftGroup:
    def __init__(self, node_id: str, group_id: str, peers: list[str],
                 send_fn: SendFn, apply_fn: ApplyFn, data_dir: str,
                 preferred: bool = False,
                 on_event: Optional[Callable[[str, str], None]] = None):
        self.node_id = node_id
        self.group_id = group_id
        self.peers = list(peers)  # other node_ids replicating this group
        self._send = send_fn
        self._apply = apply_fn
        self._emit = on_event or (lambda cat, msg: None)
        # A shard's preferred leader uses shorter election timeouts so it normally wins, which
        # spreads leadership predictably across nodes for the demo. Any node can still take over.
        self._preferred = preferred

        os.makedirs(data_dir, exist_ok=True)
        self._meta_path = os.path.join(data_dir, f"raft-{group_id}.meta.json")
        self._log_path = os.path.join(data_dir, f"raft-{group_id}.log")

        # Persistent state (recovered below)
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: list[dict] = []  # each: {"term": int, "index": int, "command": Any}

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self._last_applied_term = 0  # term of the most-recently-applied entry (for readiness)
        self.role = Role.FOLLOWER
        self.leader_id: Optional[str] = None
        self.votes_received: set[str] = set()
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._lock = threading.RLock()
        self._commit_cv = threading.Condition(self._lock)
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._new_timeout()
        self._running = False
        self._log_fh: Optional[TextIO] = None  # append handle, opened on start

        self._recover()

    # ------------------------------------------------------------------ #
    #  Persistence                                                         #
    # ------------------------------------------------------------------ #

    def _new_timeout(self) -> float:
        base = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        return base * 0.4 if self._preferred else base

    def _recover(self) -> None:
        if os.path.exists(self._meta_path):
            with open(self._meta_path) as f:
                meta = json.load(f)
            self.current_term = meta.get("term", 0)
            self.voted_for = meta.get("voted_for")
        if os.path.exists(self._log_path):
            with open(self._log_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.log.append(json.loads(line))

    def _persist_meta(self) -> None:
        tmp = self._meta_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"term": self.current_term, "voted_for": self.voted_for}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._meta_path)

    def _append_log_persist(self, entry: dict) -> None:
        assert self._log_fh is not None
        self._log_fh.write(json.dumps(entry) + "\n")
        self._log_fh.flush()
        os.fsync(self._log_fh.fileno())

    def _rewrite_log(self) -> None:
        # Used on conflict truncation: rewrite the whole file (rare, demo-sized logs).
        if self._log_fh is not None:
            self._log_fh.close()
        tmp = self._log_path + ".tmp"
        with open(tmp, "w") as f:
            for entry in self.log:
                f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._log_path)
        self._log_fh = open(self._log_path, "a")

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        self._log_fh = open(self._log_path, "a")
        self._running = True
        threading.Thread(target=self._election_loop, daemon=True).start()
        threading.Thread(target=self._leader_loop, daemon=True).start()

    def stop(self) -> None:
        self._running = False
        if self._log_fh is not None:
            try:
                self._log_fh.close()
            except OSError:
                pass

    # ------------------------------------------------------------------ #
    #  Role transitions                                                    #
    # ------------------------------------------------------------------ #

    def _become_follower(self, term: int) -> None:
        changed = term != self.current_term or self.voted_for is not None
        self.role = Role.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._new_timeout()
        if changed:
            self._persist_meta()

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        nxt = len(self.log) + 1
        for p in self.peers:
            self.next_index[p] = nxt
            self.match_index[p] = 0
        # Commit a no-op in this term so all entries inherited from prior terms get applied (Raft's
        # commit-point rule). Until this applies, the leader is NOT ready to serve conflict-checked
        # writes — otherwise it could miss a prepared intent it hasn't applied yet. Appended directly
        # (we already hold the lock; propose() would re-enter it).
        idx = len(self.log) + 1
        entry = {"term": self.current_term, "index": idx, "command": {"op": "noop"}}
        self.log.append(entry)
        self._append_log_persist(entry)
        self._emit("leader", f"{self.group_id}: won a majority of votes → I am now the LEADER "
                             f"for term {self.current_term}; committing a no-op to become ready")

    # ------------------------------------------------------------------ #
    #  Election                                                            #
    # ------------------------------------------------------------------ #

    def _election_loop(self) -> None:
        while self._running:
            time.sleep(0.02)
            with self._lock:
                if self.role == Role.LEADER:
                    continue
                if time.monotonic() - self._last_heartbeat >= self._election_timeout:
                    self._start_election()

    def _start_election(self) -> None:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._new_timeout()
        self._persist_meta()
        self._emit("election", f"{self.group_id}: election timeout — no heartbeat from a leader, "
                               f"so I'm starting an election for term {self.current_term} and "
                               f"requesting votes from {self.peers}")
        req = {
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": len(self.log),
            "last_log_term": self.log[-1]["term"] if self.log else 0,
        }
        for p in self.peers:
            threading.Thread(target=self._request_vote, args=(p, req), daemon=True).start()

    def _request_vote(self, peer: str, req: dict) -> None:
        resp = self._send(peer, "vote", req)
        if not resp.get("ok"):
            return
        r = resp["result"]
        with self._lock:
            if self.role != Role.CANDIDATE or r["term"] < self.current_term:
                return
            if r["term"] > self.current_term:
                self._become_follower(r["term"])
                return
            if r["vote_granted"]:
                self.votes_received.add(r["voter_id"])
                if len(self.votes_received) >= self._majority():
                    self._become_leader()

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def handle_vote(self, req: dict) -> dict:
        with self._lock:
            if req["term"] > self.current_term:
                self._become_follower(req["term"])
            granted = False
            if req["term"] >= self.current_term and self.voted_for in (None, req["candidate_id"]):
                my_term = self.log[-1]["term"] if self.log else 0
                my_index = len(self.log)
                up_to_date = (req["last_log_term"] > my_term or
                              (req["last_log_term"] == my_term and req["last_log_index"] >= my_index))
                if up_to_date:
                    granted = True
                    self.voted_for = req["candidate_id"]
                    self._last_heartbeat = time.monotonic()
                    self._persist_meta()
                    self._emit("vote", f"{self.group_id}: granted my vote to {req['candidate_id']} "
                                       f"for term {req['term']} (its log is at least as up-to-date "
                                       f"as mine — safe to elect)")
            return {"term": self.current_term, "vote_granted": granted, "voter_id": self.node_id}

    # ------------------------------------------------------------------ #
    #  Log replication                                                     #
    # ------------------------------------------------------------------ #

    def _leader_loop(self) -> None:
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            with self._lock:
                if self.role != Role.LEADER:
                    continue
                peers = list(self.peers)
            for p in peers:
                threading.Thread(target=self._replicate_to, args=(p,), daemon=True).start()

    def _replicate_to(self, peer: str) -> None:
        with self._lock:
            if self.role != Role.LEADER:
                return
            next_idx = self.next_index.get(peer, 1)
            prev_index = next_idx - 1
            prev_term = self.log[prev_index - 1]["term"] if 0 < prev_index <= len(self.log) else 0
            entries = self.log[next_idx - 1:]
            if entries:
                self._emit("replication", f"{self.group_id}: replicating idx "
                                          f"{entries[0]['index']}..{entries[-1]['index']} to "
                                          f"follower {peer} (waiting for a majority to ack before "
                                          f"committing)")
            req = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": entries,
                "leader_commit": self.commit_index,
            }
        resp = self._send(peer, "append", req)
        if not resp.get("ok"):
            return
        r = resp["result"]
        with self._lock:
            if r["term"] > self.current_term:
                self._become_follower(r["term"])
                return
            if self.role != Role.LEADER:
                return
            if r["success"]:
                self.match_index[peer] = r["match_index"]
                self.next_index[peer] = r["match_index"] + 1
                self._advance_commit()
            else:
                self.next_index[peer] = max(1, self.next_index.get(peer, 1) - 1)

    def _advance_commit(self) -> None:
        for idx in range(len(self.log), self.commit_index, -1):
            if self.log[idx - 1]["term"] != self.current_term:
                continue
            count = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= idx)
            if count >= self._majority():
                self.commit_index = idx
                self._apply_committed()
                break

    def handle_append(self, req: dict) -> dict:
        with self._lock:
            if req["term"] > self.current_term:
                self._become_follower(req["term"])
            if req["term"] < self.current_term:
                return {"term": self.current_term, "success": False,
                        "follower_id": self.node_id, "match_index": 0}

            self._last_heartbeat = time.monotonic()
            if self.leader_id != req["leader_id"]:
                self._emit("leader", f"{self.group_id}: accepting {req['leader_id']} as the leader "
                                     f"for term {req['term']} (received its AppendEntries) — "
                                     f"I am a follower for this shard")
            self.leader_id = req["leader_id"]
            if self.role == Role.CANDIDATE:
                self._become_follower(req["term"])

            prev_index = req["prev_log_index"]
            if prev_index > 0:
                if len(self.log) < prev_index:
                    return {"term": self.current_term, "success": False,
                            "follower_id": self.node_id, "match_index": len(self.log)}
                if self.log[prev_index - 1]["term"] != req["prev_log_term"]:
                    self.log = self.log[:prev_index - 1]
                    self._rewrite_log()
                    return {"term": self.current_term, "success": False,
                            "follower_id": self.node_id, "match_index": len(self.log)}

            rewrote = False
            for entry in req["entries"]:
                idx = entry["index"]
                if idx <= len(self.log):
                    if self.log[idx - 1]["term"] != entry["term"]:
                        self.log = self.log[:idx - 1]
                        self.log.append(entry)
                        rewrote = True
                    # else: already have it, skip
                else:
                    self.log.append(entry)
                    if not rewrote:
                        self._append_log_persist(entry)
            if rewrote:
                self._rewrite_log()
            if req["entries"]:
                first = req["entries"][0]["index"]
                last = req["entries"][-1]["index"]
                self._emit("replication", f"{self.group_id}: received {len(req['entries'])} "
                                          f"replicated entr{'y' if len(req['entries']) == 1 else 'ies'} "
                                          f"(idx {first}..{last}) from leader {req['leader_id']} — "
                                          f"appended to my log (durably, fsync'd)")

            if req["leader_commit"] > self.commit_index:
                self.commit_index = min(req["leader_commit"], len(self.log))
                self._apply_committed()

            return {"term": self.current_term, "success": True,
                    "follower_id": self.node_id, "match_index": len(self.log)}

    # ------------------------------------------------------------------ #
    #  State machine apply                                                 #
    # ------------------------------------------------------------------ #

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            self._last_applied_term = entry["term"]
            self._apply(entry["index"], entry["command"])
            self._emit("apply", f"{self.group_id}: entry idx {entry['index']} reached a majority → "
                                f"COMMITTED; applied to the storage engine "
                                f"({self._summarize(entry['command'])})")
        self._commit_cv.notify_all()

    @staticmethod
    def _summarize(command: dict) -> str:
        writes = command.get("writes", []) if isinstance(command, dict) else []
        parts = [f"{k}=∅(delete)" if v is None else f"{k}={v}" for k, v in writes]
        return ", ".join(parts) if parts else "no-op"

    # ------------------------------------------------------------------ #
    #  Client-facing (leader only)                                         #
    # ------------------------------------------------------------------ #

    def propose(self, command: dict) -> Optional[int]:
        """Append a command on the leader. Returns its log index, or None if not leader."""
        with self._lock:
            if self.role != Role.LEADER:
                return None
            index = len(self.log) + 1
            entry = {"term": self.current_term, "index": index, "command": command}
            self.log.append(entry)
            self._append_log_persist(entry)
            return index

    def wait_commit(self, index: int, timeout: float = 3.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._commit_cv:
            while self.last_applied < index:
                if self.role != Role.LEADER:
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._commit_cv.wait(timeout=remaining)
            return True

    def is_leader(self) -> bool:
        with self._lock:
            return self.role == Role.LEADER

    def is_ready(self) -> bool:
        """A leader is ready to serve conflict-checked writes only once it has applied an entry from
        its own term (its election no-op) — guaranteeing it has applied all inherited committed
        entries, including any prepared intents."""
        with self._lock:
            return self.role == Role.LEADER and self._last_applied_term == self.current_term

    def status(self) -> dict:
        with self._lock:
            return {
                "group": self.group_id,
                "node": self.node_id,
                "role": self.role.value,
                "ready": self.role == Role.LEADER and self._last_applied_term == self.current_term,
                "term": self.current_term,
                "leader": self.leader_id,
                "log_len": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
            }
