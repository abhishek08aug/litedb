"""
storage_engine.py — the storage abstraction the server and query layer depend on.

LiteDB ships two implementations, chosen at server startup (--engine):
  - lsm_engine.LSMEngine    — write-optimized LSM-Tree (WAL + MemTable + SSTables)
  - btree_engine.BTreeEngine — read-optimized in-memory B+Tree (durable via WAL)

This mirrors how real databases let the workload pick a storage engine (e.g. MySQL's
pluggable engines: InnoDB B-tree vs MyRocks LSM).

Secondary-index support is optional and advertised via supports_secondary_index().
"""

from abc import ABC, abstractmethod
from typing import Iterator, Optional


class StorageEngine(ABC):
    @abstractmethod
    def set(self, key: str, value: str) -> None:
        """Insert or update a key."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a key."""

    @abstractmethod
    def get(self, key: str) -> Optional[str]:
        """Point lookup; None if absent or deleted."""

    @abstractmethod
    def scan(self, start_key: str, end_key: str) -> Iterator[tuple[str, str]]:
        """Ordered range scan over [start_key, end_key] (lexicographic)."""

    @abstractmethod
    def flush(self) -> None:
        """Force any buffered data to durable storage."""

    @abstractmethod
    def stats(self) -> dict:
        """Engine statistics for the STATS command."""

    @abstractmethod
    def name(self) -> str:
        """Short engine name ('lsm' / 'btree')."""

    @abstractmethod
    def close(self) -> None:
        """Flush and release resources."""

    # ---- optional secondary-index capability -----------------------------

    def supports_secondary_index(self) -> bool:
        """Whether this engine maintains a secondary (value) index."""
        return False

    def find_by_value_range(self, low_value: str, high_value: str) -> list[str]:
        """Primary keys whose stored value is in [low_value, high_value], via the index."""
        raise NotImplementedError(f"Engine {self.name()!r} has no secondary index")

    # ---- optional atomic multi-key write ---------------------------------

    def supports_atomic_batch(self) -> bool:
        """Whether write_batch is applied atomically (all-or-nothing across crashes)."""
        return False

    def write_batch(self, ops: "list[WriteOp]") -> None:
        """Apply a set of writes. Atomic-capable engines commit the whole batch via one WAL
        record (all-or-nothing on recovery); the default applies them sequentially."""
        for op in ops:
            if op.is_delete:
                self.delete(op.key)
            else:
                self.set(op.key, op.value)


class WriteOp:
    """One put or delete within an atomic write batch (see StorageEngine.write_batch)."""

    __slots__ = ("key", "value", "is_delete")

    def __init__(self, key: str, value, is_delete: bool):
        self.key = key
        self.value = value
        self.is_delete = is_delete

    @staticmethod
    def put(key: str, value: str) -> "WriteOp":
        return WriteOp(key, value, False)

    @staticmethod
    def delete(key: str) -> "WriteOp":
        return WriteOp(key, None, True)
