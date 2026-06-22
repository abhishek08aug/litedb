"""
shard_store.py — the per-shard MVCC state machine driven by Raft.

Each shard replica owns one of these: an LSMEngine plus the MVCC versioning rules. The crucial
property for replicated MVCC is **deterministic apply** — the leader assigns the commit timestamp
and the exact versioned writes, replicates that command through Raft, and every replica applies the
identical versioned puts. No replica invents a timestamp, so all replicas converge byte-for-byte.

Reuses the version-key encoding from mvcc.py so the single-node and distributed paths share one
on-disk format.
"""

import threading

from lsm_engine import LSMEngine
from mvcc import (
    _HI16,
    _HI_CHAR,
    _SEP,
    TOMBSTONE,
    _ts_from_version_key,
    _user_key_from_version_key,
    _version_key,
)
from storage_engine import WriteOp


class ShardStore:
    def __init__(self, data_dir: str):
        self.engine = LSMEngine(data_dir)
        self._lock = threading.Lock()
        self._last_ts = self._recover_max_ts()

    def _recover_max_ts(self) -> int:
        mx = 0
        for k, _v in self.engine.scan("", _HI_CHAR):
            ts = _ts_from_version_key(k)
            if ts > mx:
                mx = ts
        return mx

    # ---- apply (runs on every replica, in Raft log order) ------------------

    def apply(self, command: dict) -> None:
        """command = {"ts": int, "writes": [[user_key, value_or_None], ...]}.
        A value of None encodes a delete (tombstone)."""
        ts = command["ts"]
        ops = []
        for user_key, value in command["writes"]:
            stored = TOMBSTONE if value is None else value
            ops.append(WriteOp.put(_version_key(user_key, ts), stored))
        if ops:
            self.engine.write_batch(ops)
        with self._lock:
            if ts > self._last_ts:
                self._last_ts = ts

    def snapshot_ts(self) -> int:
        with self._lock:
            return self._last_ts

    # ---- snapshot reads (leader-side; any replica could serve these) -------

    def read(self, key: str, read_ts: int):
        lo = _version_key(key, read_ts)         # newest version <= read_ts sorts first
        hi = key + _SEP + _HI16
        for _k, v in self.engine.scan(lo, hi):
            return None if v == TOMBSTONE else v
        return None

    def scan(self, lo_user: str, hi_user: str, read_ts: int) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        cur = None
        resolved = False
        for k, v in self.engine.scan(lo_user, hi_user + _HI_CHAR):
            uk = _user_key_from_version_key(k)
            ts = _ts_from_version_key(k)
            if uk != cur:
                cur = uk
                resolved = False
            if resolved or ts > read_ts:
                continue
            resolved = True
            if v != TOMBSTONE:
                out.append((uk, v))
        return out

    def newest_committed_ts(self, key: str) -> int:
        for k, _v in self.engine.scan(key + _SEP, key + _SEP + _HI16):
            return _ts_from_version_key(k)
        return 0

    def close(self) -> None:
        self.engine.close()
