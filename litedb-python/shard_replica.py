"""
shard_replica.py — one node's replica of one shard.

Binds a RaftGroup (consensus + replication) to a ShardStore (MVCC storage), and adds the
leader-side transactional commit path:

    conflict-check (OCC)  ->  assign HLC commit timestamp  ->  propose through Raft
                          ->  wait for majority commit + local apply

The whole single-shard commit is serialized by a per-shard lock so the conflict check sees every
prior committed write (first-committer-wins). For cross-shard transactions the 2PC coordinator
(txn_coordinator.py) drives prepare/commit across several of these.
"""

import os
import threading
from typing import Callable, Optional

from hlc import HLC
from raft_node import RaftGroup, SendFn
from shard_store import ShardStore


class ShardReplica:
    def __init__(self, node_id: str, shard_id: str, peers: list[str],
                 send_fn: SendFn, data_dir: str, hlc: HLC, preferred: bool = False,
                 on_event: Optional[Callable[[str, str], None]] = None):
        self.node_id = node_id
        self.shard_id = shard_id
        self.hlc = hlc
        self.store = ShardStore(os.path.join(data_dir, f"shard-{shard_id}-data"))
        self.raft = RaftGroup(
            node_id=node_id, group_id=shard_id, peers=peers, send_fn=send_fn,
            apply_fn=lambda index, command: self.store.apply(command),
            data_dir=os.path.join(data_dir, f"shard-{shard_id}-raft"),
            preferred=preferred, on_event=on_event,
        )
        # serialize the check+propose decision section per shard (the durable, cross-operation
        # "lock" on keys is the replicated intent itself, not this in-process lock).
        self._lock = threading.Lock()

    def start(self) -> None:
        self.raft.start()

    def stop(self) -> None:
        self.raft.stop()
        self.store.close()

    def is_leader(self) -> bool:
        return self.raft.is_leader()

    def is_ready(self) -> bool:
        return self.raft.is_ready()

    def leader_id(self) -> Optional[str]:
        return self.raft.leader_id

    def snapshot_ts(self) -> int:
        return self.store.snapshot_ts()

    # ---- single-shard transactional write --------------------------------

    def commit_write(self, writes: dict, read_ts: Optional[int] = None,
                     timeout: float = 3.0) -> dict:
        """writes: {user_key: value_or_None}. Returns {"ok": bool, ...}."""
        with self._lock:
            if not self.raft.is_leader():
                return {"ok": False, "error": "not_leader", "leader": self.raft.leader_id}
            if not self.raft.is_ready():
                return {"ok": False, "error": "not_ready"}
            rts = read_ts if read_ts is not None else self.store.snapshot_ts()
            conflict = self._check_conflicts(writes, rts, txn_id="")
            if conflict is not None:
                return {"ok": False, **conflict}
            # commit must be newer than the snapshot it was validated against
            commit_ts = self.hlc.update(rts) if read_ts is not None else self.hlc.now()
            index = self.raft.propose({"ts": commit_ts,
                                       "writes": [[k, v] for k, v in writes.items()]})
            if index is None:
                return {"ok": False, "error": "not_leader"}
            ok = self.raft.wait_commit(index, timeout=timeout)
            return {"ok": ok, "commit_ts": commit_ts} if ok else {"ok": False, "error": "timeout"}

    def _check_conflicts(self, writes: dict, read_ts: int, txn_id: str) -> Optional[dict]:
        """A write conflicts if a newer committed version exists (OCC) or a key is locked by another
        prepared 2PC intent."""
        for key in writes:
            if self.store.newest_committed_ts(key) > read_ts:
                return {"error": "conflict", "key": key}
            locker = self.store.intent_locking(key, exclude_txn=txn_id)
            if locker is not None:
                return {"error": "locked", "key": key, "by": locker}
        return None

    # ---- 2PC participant side (driven by the coordinator) ----------------

    def prepare(self, txn_id: str, writes: dict, read_ts: int, commit_ts: int,
                coordinator: Optional[str] = None) -> dict:
        """Phase 1: conflict-check, then replicate a PREPARE intent through Raft. Once the intent is
        committed to a majority it is durable AND survives a leadership change (any new leader has
        it in its log), so the vote can't be lost — the participant-recovery property."""
        with self._lock:
            if not self.raft.is_leader():
                return {"ok": False, "error": "not_leader", "leader": self.raft.leader_id}
            if not self.raft.is_ready():
                return {"ok": False, "error": "not_ready"}
            conflict = self._check_conflicts(writes, read_ts, txn_id=txn_id)
            if conflict is not None:
                return {"ok": False, **conflict}
            index = self.raft.propose({"op": "prepare", "txn_id": txn_id, "commit_ts": commit_ts,
                                       "writes": [[k, v] for k, v in writes.items()]})
            if index is None:
                return {"ok": False, "error": "not_leader"}
            ok = self.raft.wait_commit(index, timeout=3.0)  # hold lock until the intent is applied
            return {"ok": True} if ok else {"ok": False, "error": "timeout"}

    def commit_prepared(self, txn_id: str, timeout: float = 3.0) -> dict:
        """Phase 2 (commit): replicate a COMMIT entry; apply turns the intent's writes into committed
        versions. Idempotent — committing an already-resolved txn is a no-op on apply."""
        index = self.raft.propose({"op": "commit", "txn_id": txn_id})
        if index is None:
            return {"ok": False, "error": "not_leader"}
        ok = self.raft.wait_commit(index, timeout=timeout)
        return {"ok": True} if ok else {"ok": False, "error": "timeout"}

    def abort_prepared(self, txn_id: str, timeout: float = 3.0) -> dict:
        index = self.raft.propose({"op": "abort", "txn_id": txn_id})
        if index is None:
            return {"ok": False, "error": "not_leader"}
        ok = self.raft.wait_commit(index, timeout=timeout)
        return {"ok": True} if ok else {"ok": False, "error": "timeout"}

    # ---- reads ------------------------------------------------------------

    def read(self, key: str, read_ts: Optional[int] = None):
        rts = read_ts if read_ts is not None else self.store.snapshot_ts()
        return self.store.read(key, rts)

    def scan(self, lo: str, hi: str, read_ts: Optional[int] = None) -> list[tuple[str, str]]:
        rts = read_ts if read_ts is not None else self.store.snapshot_ts()
        return self.store.scan(lo, hi, rts)

    def status(self) -> dict:
        return self.raft.status()
