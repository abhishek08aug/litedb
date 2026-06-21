"""
mvcc.py — a multi-version, transactional layer over any StorageEngine.

Turns a single-version key-value store into a multi-version one so concurrent readers see a
consistent snapshot while writers add new versions (the CockroachDB / TiKV "MVCC on KV" model).

Versioned key layout (one physical key per version):
    userKey + SEP + descTs   ->   value | TOMBSTONE
where descTs = 016x of (MAX_TS - commitTs) so NEWER versions sort FIRST within a userKey. A
snapshot read at timestamp T seeks the first version with commitTs <= T. The version suffix is
fixed length (SEP + 16 hex), so userKeys may themselves contain SEP (e.g. relational index keys).

Writes are buffered in a Transaction and flushed at commit, all under one commit timestamp, via
an atomic write_batch (so a row + its index entries land together). Commit performs a write-write
conflict check (snapshot isolation, first-committer-wins).
"""

import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from storage_engine import WriteOp  # type: ignore

_MAX_TS = (1 << 63) - 1
_SEP = chr(0)
_HI16 = "f" * 16
_HI_CHAR = chr(0x10FFFF)
_SUFFIX_LEN = 1 + 16
TOMBSTONE = " __DELETED__"


class ConflictException(Exception):
    def __init__(self, key, conflict_ts, read_ts):
        super().__init__(
            f"write-write conflict on key {key!r}: committed at ts={conflict_ts} "
            f"after this txn's snapshot ts={read_ts} (abort and retry)")


class TimestampOracle:
    def __init__(self, start: int = 0):
        self._counter = start
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter

    def current(self) -> int:
        return self._counter


def _version_key(user_key: str, ts: int) -> str:
    return user_key + _SEP + format(_MAX_TS - ts, "016x")


def _ts_from_version_key(vkey: str) -> int:
    return _MAX_TS - int(vkey[-16:], 16)


def _user_key_from_version_key(vkey: str) -> str:
    return vkey[:-_SUFFIX_LEN]


class MVCCEngine:
    def __init__(self, engine):
        self._engine = engine
        max_ts = self._recover_max_ts()
        self._oracle = TimestampOracle(max_ts)
        self._last_commit_ts = max_ts
        self._commit_lock = threading.Lock()

    def begin(self) -> "Transaction":
        return Transaction(self, self._last_commit_ts)

    def last_commit_ts(self) -> int:
        return self._last_commit_ts

    # ---- snapshot reads ---------------------------------------------------

    def _read(self, user_key: str, read_ts: int):
        lo = _version_key(user_key, read_ts)            # newest version <= read_ts is first
        hi = user_key + _SEP + _HI16
        for _k, v in self._engine.scan(lo, hi):
            return None if v == TOMBSTONE else v
        return None

    def _scan(self, lo_user: str, hi_user: str, read_ts: int):
        out = []
        cur = None
        resolved = False
        for k, v in self._engine.scan(lo_user, hi_user + _HI_CHAR):
            uk = _user_key_from_version_key(k)
            ts = _ts_from_version_key(k)
            if uk != cur:
                cur = uk
                resolved = False
            if resolved:
                continue
            if ts > read_ts:
                continue
            resolved = True
            if v != TOMBSTONE:
                out.append((uk, v))
        return out

    def _newest_committed_ts(self, user_key: str) -> int:
        for k, _v in self._engine.scan(user_key + _SEP, user_key + _SEP + _HI16):
            return _ts_from_version_key(k)
        return 0

    # ---- commit (atomic + conflict check) ---------------------------------

    def _commit(self, read_ts: int, writes: dict) -> int:
        with self._commit_lock:
            for key in writes:
                newest = self._newest_committed_ts(key)
                if newest > read_ts:
                    raise ConflictException(key, newest, read_ts)
            commit_ts = self._oracle.next()
            ops = [WriteOp.put(_version_key(k, commit_ts), v) for k, v in writes.items()]
            self._engine.write_batch(ops)
            self._last_commit_ts = commit_ts
            return commit_ts

    # ---- GC ---------------------------------------------------------------

    def vacuum(self, low_water_ts: int) -> int:
        to_delete = []
        prev = None
        kept_floor = False
        for k, _v in self._engine.scan("", _HI_CHAR):
            uk = _user_key_from_version_key(k)
            ts = _ts_from_version_key(k)
            if uk != prev:
                prev = uk
                kept_floor = False
            if ts > low_water_ts:
                continue
            if not kept_floor:
                kept_floor = True
            else:
                to_delete.append(k)
        for k in to_delete:
            self._engine.delete(k)
        return len(to_delete)

    def version_count(self) -> int:
        return sum(1 for _ in self._engine.scan("", _HI_CHAR))

    def _recover_max_ts(self) -> int:
        mx = 0
        for k, _v in self._engine.scan("", _HI_CHAR):
            ts = _ts_from_version_key(k)
            if ts > mx:
                mx = ts
        return mx


class Transaction:
    def __init__(self, mvcc: MVCCEngine, read_ts: int):
        self._mvcc = mvcc
        self._read_ts = read_ts
        self._writes = {}          # key -> value | TOMBSTONE (insertion-ordered)
        self._finished = False
        self._commit_ts = -1

    @property
    def read_ts(self) -> int:
        return self._read_ts

    @property
    def commit_ts(self) -> int:
        return self._commit_ts

    def has_writes(self) -> bool:
        return bool(self._writes)

    def get(self, key: str):
        self._ensure_active()
        if key in self._writes:
            v = self._writes[key]
            return None if v == TOMBSTONE else v
        return self._mvcc._read(key, self._read_ts)

    def scan(self, lo_key: str, hi_key: str):
        merged = {}
        for k, v in self._mvcc._scan(lo_key, hi_key, self._read_ts):
            merged[k] = v
        for k, v in self._writes.items():           # overlay own writes
            if k < lo_key or k > hi_key:
                continue
            if v == TOMBSTONE:
                merged.pop(k, None)
            else:
                merged[k] = v
        return sorted(merged.items())

    def put(self, key: str, value: str):
        self._ensure_active()
        self._writes[key] = value

    def delete(self, key: str):
        self._ensure_active()
        self._writes[key] = TOMBSTONE

    def commit(self) -> int:
        self._ensure_active()
        self._finished = True
        self._commit_ts = self._mvcc._commit(self._read_ts, self._writes)
        return self._commit_ts

    def rollback(self):
        self._finished = True
        self._writes = {}

    def _ensure_active(self):
        if self._finished:
            raise RuntimeError("transaction already finished")
