"""
shard_store.py — the per-shard MVCC state machine driven by Raft.

Each shard replica owns one of these: an LSMEngine plus the MVCC versioning rules. The crucial
property for replicated MVCC is **deterministic apply** — the leader assigns the commit timestamp
and the exact versioned writes, replicates that command through Raft, and every replica applies the
identical versioned puts. No replica invents a timestamp, so all replicas converge byte-for-byte.

Reuses the version-key encoding from mvcc.py so the single-node and distributed paths share one
on-disk format.
"""

import json
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

# 2PC prepared intents live as replicated keys under this prefix (sorts below user keys, which start
# with printable chars), so any replica — including a NEW leader after a leadership change — has the
# prepared writes and the conflict information. This is the Percolator / CockroachDB intent model.
_INTENT_PREFIX = "\x00__intent__\x00"


class ShardStore:
    def __init__(self, data_dir: str):
        self.engine = LSMEngine(data_dir)
        self._lock = threading.Lock()
        # txn_id -> (commit_ts, writes_list) for prepared-but-not-resolved 2PC intents, rebuilt from
        # the replicated intent keys on restart (so a recovered replica knows what's locked).
        self._intents: dict[str, tuple[int, list]] = {}
        self._recover_intents()
        self._last_ts = self._recover_max_ts()

    @staticmethod
    def _intent_key(txn_id: str) -> str:
        return _INTENT_PREFIX + txn_id

    def _recover_intents(self) -> None:
        for k, v in self.engine.scan(_INTENT_PREFIX, _INTENT_PREFIX + _HI_CHAR):
            rec = json.loads(v)
            self._intents[k[len(_INTENT_PREFIX):]] = (rec["commit_ts"], rec["writes"])

    def _recover_max_ts(self) -> int:
        mx = 0
        for k, _v in self.engine.scan("", _HI_CHAR):
            if k.startswith(_INTENT_PREFIX):
                continue
            ts = _ts_from_version_key(k)
            if ts > mx:
                mx = ts
        return mx

    # ---- apply (runs on every replica, in Raft log order) ------------------

    def apply(self, command: dict) -> None:
        """Deterministic state-machine apply. Commands:
          legacy single-shard commit: {"ts", "writes"}
          2PC prepare:                {"op":"prepare", "txn_id", "commit_ts", "writes"}
          2PC commit:                 {"op":"commit", "txn_id"}   (writes come from the stored intent)
          2PC abort:                  {"op":"abort", "txn_id"}
        writes = [[user_key, value_or_None], ...]; None encodes a delete (tombstone)."""
        op = command.get("op")
        if op == "noop":
            return  # leader-election no-op: only there to advance the commit point
        if op == "prepare":
            txn_id, commit_ts, writes = command["txn_id"], command["commit_ts"], command["writes"]
            self.engine.set(self._intent_key(txn_id),
                            json.dumps({"commit_ts": commit_ts, "writes": writes}))
            with self._lock:
                self._intents[txn_id] = (commit_ts, writes)
            return
        if op == "commit":
            txn_id = command["txn_id"]
            with self._lock:
                staged = self._intents.pop(txn_id, None)
            if staged is None:
                self.engine.delete(self._intent_key(txn_id))  # idempotent
                return
            commit_ts, writes = staged
            ops = [WriteOp.put(_version_key(k, commit_ts), TOMBSTONE if v is None else v)
                   for k, v in writes]
            ops.append(WriteOp.delete(self._intent_key(txn_id)))
            self.engine.write_batch(ops)
            with self._lock:
                if commit_ts > self._last_ts:
                    self._last_ts = commit_ts
            return
        if op == "abort":
            with self._lock:
                self._intents.pop(command["txn_id"], None)
            self.engine.delete(self._intent_key(command["txn_id"]))
            return

        ts = command["ts"]
        ops = [WriteOp.put(_version_key(k, ts), TOMBSTONE if v is None else v)
               for k, v in command["writes"]]
        if ops:
            self.engine.write_batch(ops)
        with self._lock:
            if ts > self._last_ts:
                self._last_ts = ts

    def snapshot_ts(self) -> int:
        with self._lock:
            return self._last_ts

    def intent_locking(self, key: str, exclude_txn: str) -> str | None:
        """Return the txn id of an outstanding intent that holds `key` (other than exclude_txn)."""
        with self._lock:
            for txn_id, (_cts, writes) in self._intents.items():
                if txn_id == exclude_txn:
                    continue
                if any(k == key for k, _v in writes):
                    return txn_id
        return None

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
