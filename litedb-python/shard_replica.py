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
        self._commit_lock = threading.Lock()
        # staged 2PC writes: txn_id -> (commit_ts, writes_dict)
        self._prepared: dict[str, tuple[int, dict]] = {}

    def start(self) -> None:
        self.raft.start()

    def stop(self) -> None:
        self.raft.stop()
        self.store.close()

    def is_leader(self) -> bool:
        return self.raft.is_leader()

    def leader_id(self) -> Optional[str]:
        return self.raft.leader_id

    def snapshot_ts(self) -> int:
        return self.store.snapshot_ts()

    # ---- single-shard transactional write --------------------------------

    def commit_write(self, writes: dict, read_ts: Optional[int] = None,
                     timeout: float = 3.0) -> dict:
        """writes: {user_key: value_or_None}. Returns {"ok": bool, ...}."""
        with self._commit_lock:
            if not self.raft.is_leader():
                return {"ok": False, "error": "not_leader", "leader": self.raft.leader_id}
            rts = read_ts if read_ts is not None else self.store.snapshot_ts()
            conflict = self._check_conflicts(writes, rts)
            if conflict is not None:
                return {"ok": False, "error": "conflict", "key": conflict}
            # commit must be newer than the snapshot it was validated against
            commit_ts = self.hlc.update(rts) if read_ts is not None else self.hlc.now()
            index = self.raft.propose({"ts": commit_ts,
                                       "writes": [[k, v] for k, v in writes.items()]})
            if index is None:
                return {"ok": False, "error": "not_leader"}
            ok = self.raft.wait_commit(index, timeout=timeout)
            return {"ok": ok, "commit_ts": commit_ts} if ok else {"ok": False, "error": "timeout"}

    def _check_conflicts(self, writes: dict, read_ts: int) -> Optional[str]:
        for key in writes:
            if self.store.newest_committed_ts(key) > read_ts:
                return key
        return None

    # ---- 2PC participant side (driven by the coordinator) ----------------

    def prepare(self, txn_id: str, writes: dict, read_ts: int, commit_ts: int) -> dict:
        """Validate conflicts and stage writes. Holds the commit lock for the txn until
        commit/abort so a concurrent write can't slip in between check and apply."""
        self._commit_lock.acquire()
        try:
            if not self.raft.is_leader():
                self._commit_lock.release()
                return {"ok": False, "error": "not_leader", "leader": self.raft.leader_id}
            conflict = self._check_conflicts(writes, read_ts)
            if conflict is not None:
                self._commit_lock.release()
                return {"ok": False, "error": "conflict", "key": conflict}
            self._prepared[txn_id] = (commit_ts, writes)
            return {"ok": True}
        except Exception:
            self._commit_lock.release()
            raise

    def commit_prepared(self, txn_id: str, timeout: float = 3.0) -> dict:
        staged = self._prepared.pop(txn_id, None)
        if staged is None:
            return {"ok": False, "error": "unknown_txn"}
        commit_ts, writes = staged
        try:
            index = self.raft.propose({"ts": commit_ts,
                                       "writes": [[k, v] for k, v in writes.items()]})
            if index is None:
                return {"ok": False, "error": "not_leader"}
            ok = self.raft.wait_commit(index, timeout=timeout)
            return {"ok": ok} if ok else {"ok": False, "error": "timeout"}
        finally:
            self._release_if_held()

    def abort_prepared(self, txn_id: str) -> dict:
        self._prepared.pop(txn_id, None)
        self._release_if_held()
        return {"ok": True}

    def _release_if_held(self) -> None:
        try:
            self._commit_lock.release()
        except RuntimeError:
            pass  # not held

    # ---- reads ------------------------------------------------------------

    def read(self, key: str, read_ts: Optional[int] = None):
        rts = read_ts if read_ts is not None else self.store.snapshot_ts()
        return self.store.read(key, rts)

    def scan(self, lo: str, hi: str, read_ts: Optional[int] = None) -> list[tuple[str, str]]:
        rts = read_ts if read_ts is not None else self.store.snapshot_ts()
        return self.store.scan(lo, hi, rts)

    def status(self) -> dict:
        return self.raft.status()
