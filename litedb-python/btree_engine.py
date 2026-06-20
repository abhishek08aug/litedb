"""
btree_engine.py — a primary key->value storage engine backed by an in-memory B+Tree,
made durable by a write-ahead log (the engine replays the WAL on startup).

This is the read-optimized counterpart to lsm_engine.LSMEngine: updates happen in place in
the tree (no MemTable / SSTable / compaction). Point lookups and range scans go straight to a
single sorted structure. Like the LSM engine, durability comes from a WAL.

Scope / limitations (teaching implementation):
  - The tree is held entirely in memory; durability is via WAL replay, not on-disk pages.
  - The WAL is not checkpointed, so it grows with the write history. A production B-tree engine
    periodically snapshots the tree (a checkpoint) and truncates the WAL.

This engine does not provide a secondary index — the LSM engine is the one wired for that.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _loader  # noqa: F401, E402

from btree import BPlusTree               # type: ignore
from wal import WriteAheadLog             # type: ignore
from storage_engine import StorageEngine  # type: ignore


class BTreeEngine(StorageEngine):
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._tree = BPlusTree(order=16)
        self._wal = WriteAheadLog(os.path.join(data_dir, "wal.log"))
        self._recover()
        print(f"[BTreeEngine] Started at {data_dir!r} ({len(self._tree)} keys)")

    def _recover(self) -> None:
        """Rebuild the in-memory tree by replaying the WAL."""
        n = 0
        for entry in self._wal.read_all():
            if entry.operation == "SET":
                self._tree.set(entry.key, entry.value if entry.value is not None else "")
            elif entry.operation == "DELETE":
                self._tree.delete(entry.key)
            n += 1
        if n:
            print(f"[BTreeEngine] Recovered {n} WAL entries into the B+Tree")

    def set(self, key: str, value: str) -> None:
        self._wal.append("SET", key, value)   # durability first
        self._tree.set(key, value)            # in-place update

    def delete(self, key: str) -> None:
        self._wal.append("DELETE", key)
        self._tree.delete(key)

    def get(self, key: str):
        return self._tree.get(key)

    def scan(self, start_key: str, end_key: str):
        return self._tree.scan(start_key, end_key)

    def flush(self) -> None:
        pass  # nothing buffered: writes are already durable in the WAL and applied to the tree

    def stats(self) -> dict:
        return {"engine": "btree", "entries": len(self._tree)}

    def name(self) -> str:
        return "btree"

    def close(self) -> None:
        self._wal.close()
        print("[BTreeEngine] Closed.")
