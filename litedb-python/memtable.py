"""
memtable.py — MemTable (In-Memory Write Buffer)

CONCEPT:
  The MemTable is an in-memory sorted data structure that buffers
  recent writes before they are flushed to disk as an SSTable.

  Why sorted? Because when we flush to disk, we write keys in sorted
  order — this makes range scans and merges efficient (merge-sort).

  Read path:
    1. Check MemTable first (most recent writes are here)
    2. If not found, check SSTables on disk (older data)

  Write path:
    1. Append to WAL (durability)
    2. Insert into MemTable (fast in-memory write)
    3. When MemTable exceeds size threshold → flush to SSTable

  This is exactly how LevelDB, RocksDB, and Cassandra work.

  Data structure choice:
    We use Python's SortedDict (or a plain dict + sorted iteration).
    Production databases use a Skip List or Red-Black Tree for O(log n)
    inserts while maintaining sorted order.
    We'll use a plain dict + sort-on-flush for simplicity.
"""

import threading
from typing import Iterator

# Tombstone marker — represents a deleted key
TOMBSTONE = "__DELETED__"


class MemTable:
    """
    In-memory sorted write buffer.

    Stores the most recent value for each key.
    Deleted keys are stored with value=TOMBSTONE (not actually removed).
    This is critical: we must propagate deletes to SSTables during compaction.

    Thread-safe via a read-write lock pattern (RLock for simplicity here).
    """

    def __init__(self, size_limit_bytes: int = 4 * 1024 * 1024):  # 4MB default
        """
        size_limit_bytes: flush to SSTable when approximate size exceeds this.
        Production default: 64MB (RocksDB), 4MB (LevelDB).
        We use 4MB for demo purposes.
        """
        self._data: dict[str, str] = {}   # key → value (or TOMBSTONE)
        self._size_bytes: int = 0
        self._size_limit = size_limit_bytes
        self._lock = threading.RLock()
        self._write_count = 0

    # ------------------------------------------------------------------ #
    #  Write operations                                                    #
    # ------------------------------------------------------------------ #

    def set(self, key: str, value: str) -> None:
        """Insert or update a key-value pair."""
        with self._lock:
            # Subtract old size if key exists
            if key in self._data:
                self._size_bytes -= len(key) + len(self._data[key])

            self._data[key] = value
            self._size_bytes += len(key) + len(value)
            self._write_count += 1

    def delete(self, key: str) -> None:
        """
        Mark a key as deleted by writing a tombstone.
        We do NOT remove the key from the dict — the tombstone must
        propagate to SSTables during compaction to shadow older values.
        """
        with self._lock:
            if key in self._data:
                self._size_bytes -= len(key) + len(self._data[key])

            self._data[key] = TOMBSTONE
            self._size_bytes += len(key) + len(TOMBSTONE)
            self._write_count += 1

    # ------------------------------------------------------------------ #
    #  Read operations                                                     #
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> str | None:
        """
        Look up a key.
        Returns:
          - The value string if key exists and is not deleted
          - TOMBSTONE if the key was deleted (caller must handle this)
          - None if key was never written to this MemTable
        """
        with self._lock:
            return self._data.get(key)  # None if not present

    def scan(self, start_key: str, end_key: str) -> Iterator[tuple[str, str]]:
        """
        Range scan: yield all (key, value) pairs where start_key <= key <= end_key.
        Yields in sorted key order.
        Includes tombstones — caller decides whether to skip them.
        """
        with self._lock:
            # Sort keys on the fly (production uses a skip list to avoid this)
            for key in sorted(self._data.keys()):
                if key < start_key:
                    continue
                if key > end_key:
                    break
                yield key, self._data[key]

    def items_sorted(self) -> list[tuple[str, str]]:
        """
        Return all (key, value) pairs sorted by key.
        Used when flushing to SSTable.
        """
        with self._lock:
            return sorted(self._data.items())

    # ------------------------------------------------------------------ #
    #  Size / flush threshold                                              #
    # ------------------------------------------------------------------ #

    def should_flush(self) -> bool:
        """Returns True when the MemTable has grown past the size limit."""
        return self._size_bytes >= self._size_limit

    def size_bytes(self) -> int:
        return self._size_bytes

    def entry_count(self) -> int:
        with self._lock:
            return len(self._data)

    def clear(self) -> None:
        """
        Clear the MemTable after a successful flush to SSTable.
        Called ONLY after the SSTable is safely written to disk.
        """
        with self._lock:
            self._data.clear()
            self._size_bytes = 0
            self._write_count = 0

    def __len__(self) -> int:
        return self.entry_count()

    def __repr__(self) -> str:
        return (
            f"MemTable(entries={self.entry_count()}, "
            f"size={self._size_bytes / 1024:.1f}KB, "
            f"limit={self._size_limit / 1024 / 1024:.0f}MB)"
        )


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

if __name__ == "__main__":
    print("=" * 60)
    print("MEMTABLE DEMO")
    print("=" * 60)

    mt = MemTable(size_limit_bytes=1024)  # tiny 1KB limit for demo

    # --- Writes ---
    print("\n[Step 1] Writing key-value pairs...")
    mt.set("apple", "red fruit")
    mt.set("banana", "yellow fruit")
    mt.set("cherry", "red berry")
    mt.set("date", "brown fruit")
    mt.set("elderberry", "dark berry")
    print(f"  {mt}")

    # --- Read ---
    print("\n[Step 2] Reading keys...")
    print(f"  GET apple    → {mt.get('apple')}")
    print(f"  GET banana   → {mt.get('banana')}")
    print(f"  GET mango    → {mt.get('mango')} (not found)")

    # --- Delete (tombstone) ---
    print("\n[Step 3] Deleting 'banana'...")
    mt.delete("banana")
    result = mt.get("banana")
    print(f"  GET banana   → {result!r}  ← tombstone, key is deleted")

    # --- Range scan ---
    print("\n[Step 4] Range scan: 'b' to 'd'...")
    for key, value in mt.scan("b", "d"):
        marker = " ← TOMBSTONE (deleted)" if value == TOMBSTONE else ""
        print(f"  {key!r}: {value!r}{marker}")

    # --- Flush simulation ---
    print("\n[Step 5] Items sorted (as they'd be written to SSTable):")
    for key, value in mt.items_sorted():
        print(f"  {key!r}: {value!r}")

    # --- Size threshold ---
    print(f"\n[Step 6] Should flush? {mt.should_flush()} (size={mt.size_bytes()} bytes, limit=1024)")

    print("\n[Done] MemTable demo complete.")
    print("\nKey insights:")
    print("  1. All writes go into a dict — O(1) insert")
    print("  2. Deletes write a TOMBSTONE, not an actual removal")
    print("  3. Sorted iteration happens at flush time (or scan time)")
    print("  4. When size exceeds limit → flush to SSTable on disk")
