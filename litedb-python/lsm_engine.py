"""
lsm_engine.py — LSM-Tree Storage Engine

CONCEPT:
  The LSM-Tree (Log-Structured Merge-Tree) is the storage engine
  used by LevelDB, RocksDB, Cassandra, HBase, and many others.

  It combines WAL + MemTable + SSTables into a unified engine
  optimized for write-heavy workloads.

  Write path (fast):
    1. Append to WAL (durability)
    2. Insert into MemTable (in-memory, O(1))
    3. If MemTable full → flush to Level-0 SSTable
    4. If too many Level-0 SSTables → compact into Level-1

  Read path (check newest to oldest):
    1. Check MemTable (most recent)
    2. Check Level-0 SSTables (newest first)
    3. Check Level-1 SSTables
    4. ... deeper levels ...
    Return first match found (most recent version wins)

  Compaction:
    Merges multiple SSTables into one, removing:
    - Duplicate keys (keep only the newest version)
    - Tombstones (if no older versions exist below)
    This reclaims disk space and reduces read amplification.

  Level structure:
    Level 0: freshly flushed SSTables (may have overlapping key ranges)
    Level 1: compacted, non-overlapping key ranges, max 10MB
    Level 2: compacted, non-overlapping key ranges, max 100MB
    Level N: 10x larger than Level N-1

  We implement a simplified 2-level version for clarity.
"""

import os
import sys
import glob
import heapq
import threading
import time
from typing import Iterator

# Register numbered source files under short module names
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _loader  # noqa: F401, E402

from wal import WriteAheadLog  # type: ignore
from memtable import MemTable, TOMBSTONE  # type: ignore
from sstable import SSTableWriter, SSTableReader  # type: ignore
from storage_engine import StorageEngine  # type: ignore
from secondary_index import SecondaryIndex  # type: ignore


class LSMEngine(StorageEngine):
    """
    Full LSM-Tree storage engine.

    Coordinates WAL, MemTable, and SSTables.
    Thread-safe for concurrent reads and writes.
    """

    # Flush MemTable when it exceeds this size
    MEMTABLE_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB (small for demo; use 64MB in prod)

    # Compact Level-0 when it has this many SSTables
    L0_COMPACTION_THRESHOLD = 4

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        self._lock = threading.RLock()
        self._flush_lock = threading.Lock()  # only one flush at a time

        # WAL for durability
        wal_path = os.path.join(data_dir, "wal.log")
        self._wal = WriteAheadLog(wal_path)

        # Active MemTable (receives all writes)
        self._memtable = MemTable(size_limit_bytes=self.MEMTABLE_SIZE_LIMIT)

        # SSTable levels: list of lists of SSTableReader
        # _levels[0] = Level-0 (newest, may overlap)
        # _levels[1] = Level-1 (compacted, non-overlapping)
        self._levels: list[list[SSTableReader]] = [[], []]

        # SSTable sequence counter (for unique filenames)
        self._sst_sequence = 0

        # Secondary value index (value -> primary keys); rebuilt from data after recovery
        self._value_index = SecondaryIndex()

        # Recover from WAL and load existing SSTables
        self._recover()
        self._rebuild_index()

        print(f"[LSM] Engine started at {data_dir!r}")
        print(f"[LSM] MemTable: {self._memtable}")
        print(f"[LSM] Level-0 SSTables: {len(self._levels[0])}")
        print(f"[LSM] Level-1 SSTables: {len(self._levels[1])}")

    # ------------------------------------------------------------------ #
    #  Write operations                                                    #
    # ------------------------------------------------------------------ #

    def set(self, key: str, value: str) -> None:
        """
        Write a key-value pair.
        1. WAL append (durable)
        2. MemTable insert (fast)
        3. Trigger flush if MemTable is full
        """
        old = self.get(key)  # prior value, to maintain the secondary index
        with self._lock:
            self._wal.append("SET", key, value)
            self._memtable.set(key, value)
            self._value_index.update(key, old, value)

        # Check if flush needed (outside main lock to reduce contention)
        if self._memtable.should_flush():
            self._flush_memtable()

    def delete(self, key: str) -> None:
        """
        Delete a key by writing a tombstone.
        """
        old = self.get(key)
        with self._lock:
            self._wal.append("DELETE", key)
            self._memtable.delete(key)
            if old is not None:
                self._value_index.remove(key, old)

        if self._memtable.should_flush():
            self._flush_memtable()

    # ------------------------------------------------------------------ #
    #  Read operations                                                     #
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> str | None:
        """
        Read a key. Checks newest data first:
          MemTable → Level-0 (newest first) → Level-1 → ...

        Returns None if key doesn't exist or was deleted.
        """
        with self._lock:
            # 1. Check MemTable (most recent writes)
            result = self._memtable.get(key)
            if result is not None:
                return None if result == TOMBSTONE else result

            # 2. Check Level-0 SSTables (newest first — reverse order)
            for sst in reversed(self._levels[0]):
                result = sst.get(key)
                if result is not None:
                    return None if result == TOMBSTONE else result

            # 3. Check Level-1 SSTables (newest first)
            for sst in reversed(self._levels[1]):
                result = sst.get(key)
                if result is not None:
                    return None if result == TOMBSTONE else result

        return None  # key not found anywhere

    def scan(self, start_key: str, end_key: str) -> Iterator[tuple[str, str]]:
        """
        Range scan across all levels.
        Uses a merge-heap to merge sorted iterators from all sources.
        Returns only live (non-deleted) keys.
        """
        with self._lock:
            # Collect iterators from all sources
            iterators = []

            # MemTable items (sorted)
            mem_items = [
                (k, v, 0)  # (key, value, source_priority)
                for k, v in self._memtable.scan(start_key, end_key)
            ]

            # Level-0 SSTables (newer = higher priority = lower number)
            sst_iters = []
            priority = 1
            for sst in reversed(self._levels[0]):
                for k, v in sst.scan(start_key, end_key):
                    sst_iters.append((k, v, priority))
                priority += 1

            # Level-1 SSTables
            for sst in reversed(self._levels[1]):
                for k, v in sst.scan(start_key, end_key):
                    sst_iters.append((k, v, priority))
                priority += 1

            # Merge all sources: for each key, keep the value from the
            # source with the lowest priority number (most recent)
            all_items = mem_items + sst_iters
            # Group by key, keep lowest priority (most recent)
            seen: dict[str, tuple[str, int]] = {}
            for k, v, p in all_items:
                if k not in seen or p < seen[k][1]:
                    seen[k] = (v, p)

            # Yield live keys in sorted order
            for k in sorted(seen.keys()):
                v, _ = seen[k]
                if v != TOMBSTONE:
                    yield k, v

    # ------------------------------------------------------------------ #
    #  Flush: MemTable → SSTable                                          #
    # ------------------------------------------------------------------ #

    def _flush_memtable(self) -> None:
        """
        Flush the MemTable to a new Level-0 SSTable.

        Steps:
          1. Snapshot the MemTable contents
          2. Write SSTable to disk
          3. Clear the MemTable
          4. Truncate the WAL (data is now safe on disk)
          5. Trigger compaction if Level-0 is too large
        """
        with self._flush_lock:
            with self._lock:
                if not self._memtable.should_flush():
                    return  # another thread already flushed

                # Snapshot sorted items
                items = self._memtable.items_sorted()
                if not items:
                    return

            # Write SSTable (outside main lock — this is the slow I/O part)
            sst_path = self._new_sst_path(level=0)
            writer = SSTableWriter(sst_path)
            reader = writer.write(items)

            with self._lock:
                # Add to Level-0
                self._levels[0].append(reader)
                # Clear MemTable
                self._memtable.clear()
                # Truncate WAL — data is now safely in SSTable
                self._wal.truncate()

            print(f"[LSM] Flushed MemTable → {sst_path!r} ({len(items)} entries)")

            # Trigger compaction if needed
            if len(self._levels[0]) >= self.L0_COMPACTION_THRESHOLD:
                self._compact_l0_to_l1()

    # ------------------------------------------------------------------ #
    #  Compaction: merge SSTables, remove duplicates and tombstones       #
    # ------------------------------------------------------------------ #

    def _compact_l0_to_l1(self) -> None:
        """
        Merge all Level-0 SSTables into Level-1.

        Algorithm (merge-sort):
          1. Open iterators on all Level-0 SSTables + existing Level-1 SSTables
          2. Merge them using a min-heap (merge-sort)
          3. For each key, keep only the most recent version
          4. Drop tombstones (if no older data exists below)
          5. Write merged output as new Level-1 SSTable(s)
          6. Delete old SSTables
        """
        with self._flush_lock:
            with self._lock:
                l0_sstables = list(self._levels[0])
                l1_sstables = list(self._levels[1])

            if not l0_sstables:
                return

            print(f"[LSM] Compacting {len(l0_sstables)} L0 + {len(l1_sstables)} L1 SSTables...")

            # Collect all entries from all SSTables
            # Priority: lower index = newer (L0 is newer than L1)
            # Within L0: higher index = newer
            all_entries: dict[str, str] = {}

            # Process oldest first (L1, then L0 oldest to newest)
            # Later entries overwrite earlier ones → newest wins
            for sst in l1_sstables:
                for k, v in sst.iter_all():
                    all_entries[k] = v

            for sst in l0_sstables:  # L0 oldest to newest
                for k, v in sst.iter_all():
                    all_entries[k] = v

            # Sort and filter tombstones
            merged = sorted(
                (k, v) for k, v in all_entries.items()
                if v != TOMBSTONE  # drop tombstones in final compaction
            )

            if merged:
                # Write new Level-1 SSTable
                sst_path = self._new_sst_path(level=1)
                writer = SSTableWriter(sst_path)
                new_l1 = writer.write(merged)

                with self._lock:
                    # Replace all L0 and L1 with the new compacted L1
                    old_paths = (
                        [sst.path for sst in l0_sstables] +
                        [sst.path for sst in l1_sstables]
                    )
                    self._levels[0] = []
                    self._levels[1] = [new_l1]

                # Delete old SSTable files
                for path in old_paths:
                    try:
                        os.remove(path)
                        print(f"[LSM] Deleted old SSTable: {path!r}")
                    except OSError:
                        pass

                print(f"[LSM] Compaction done → {sst_path!r} ({len(merged)} entries)")
            else:
                # All entries were tombstones — just delete everything
                with self._lock:
                    old_paths = (
                        [sst.path for sst in l0_sstables] +
                        [sst.path for sst in l1_sstables]
                    )
                    self._levels[0] = []
                    self._levels[1] = []
                for path in old_paths:
                    try:
                        os.remove(path)
                    except OSError:
                        pass
                print("[LSM] Compaction done — all entries were tombstones, files deleted")

    # ------------------------------------------------------------------ #
    #  Recovery: replay WAL + load existing SSTables on startup           #
    # ------------------------------------------------------------------ #

    def _recover(self) -> None:
        """
        On startup:
          1. Load existing SSTable files from disk
          2. Replay WAL to rebuild MemTable (for writes not yet flushed)
        """
        # Load existing SSTables
        for level in [0, 1]:
            pattern = os.path.join(self.data_dir, f"sst_l{level}_*.sst")
            paths = sorted(glob.glob(pattern))  # sorted = oldest first
            for path in paths:
                try:
                    reader = SSTableReader(path)
                    self._levels[level].append(reader)
                    # Update sequence counter
                    seq = int(os.path.basename(path).split("_")[2].split(".")[0])
                    if seq >= self._sst_sequence:
                        self._sst_sequence = seq + 1
                except Exception as e:
                    print(f"[LSM] Warning: could not load SSTable {path!r}: {e}")

        # Replay WAL into MemTable
        replayed = 0
        for entry in self._wal.read_all():
            if entry.operation == "SET":
                self._memtable.set(entry.key, entry.value or "")
            elif entry.operation == "DELETE":
                self._memtable.delete(entry.key)
            replayed += 1

        if replayed:
            print(f"[LSM] Recovered {replayed} entries from WAL")

    def _new_sst_path(self, level: int) -> str:
        seq = self._sst_sequence
        self._sst_sequence += 1
        return os.path.join(self.data_dir, f"sst_l{level}_{seq:06d}.sst")

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def flush(self) -> None:
        """Force flush MemTable to disk (e.g., on graceful shutdown)."""
        with self._lock:
            if self._memtable.entry_count() > 0:
                # Temporarily lower the threshold to force flush
                self._memtable._size_limit = 0
        self._flush_memtable()

    def close(self) -> None:
        """Graceful shutdown: flush and close WAL."""
        self.flush()
        self._wal.close()
        print("[LSM] Engine closed.")

    # ------------------------------------------------------------------ #
    #  Engine identity + secondary index                                   #
    # ------------------------------------------------------------------ #

    def name(self) -> str:
        return "lsm"

    def supports_secondary_index(self) -> bool:
        return True

    def find_by_value_range(self, low_value: str, high_value: str) -> list[str]:
        """Reverse lookup via the secondary index: primary keys with value in [low, high]."""
        return self._value_index.keys_in_value_range(low_value, high_value)

    def _rebuild_index(self) -> None:
        """Rebuild the in-memory secondary index from all live data (after recovery)."""
        for key, value in self.scan("", chr(0x10FFFF)):
            self._value_index.add(key, value)

    def stats(self) -> dict:
        with self._lock:
            return {
                "engine": "lsm",
                "memtable_entries": self._memtable.entry_count(),
                "memtable_size_bytes": self._memtable.size_bytes(),
                "l0_sstables": len(self._levels[0]),
                "l1_sstables": len(self._levels[1]),
                "l0_entries": sum(s.entry_count for s in self._levels[0]),
                "l1_entries": sum(s.entry_count for s in self._levels[1]),
                "index_entries": self._value_index.size(),
            }

    def __repr__(self) -> str:
        s = self.stats()
        return (
            f"LSMEngine(dir={self.data_dir!r}, "
            f"mem={s['memtable_entries']} entries, "
            f"L0={s['l0_sstables']} SSTables, "
            f"L1={s['l1_sstables']} SSTables)"
        )


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

if __name__ == "__main__":
    import tempfile, shutil

    data_dir = tempfile.mkdtemp(prefix="litedb_lsm_demo_")

    print("=" * 60)
    print("LSM-TREE ENGINE DEMO")
    print("=" * 60)

    engine = LSMEngine(data_dir)

    # --- Step 1: Write data ---
    print("\n[Step 1] Writing 20 key-value pairs...")
    fruits = [
        "apple", "banana", "cherry", "date", "elderberry",
        "fig", "grape", "honeydew", "kiwi", "lemon",
        "mango", "nectarine", "orange", "papaya", "quince",
        "raspberry", "strawberry", "tangerine", "ugli", "vanilla"
    ]
    for i, fruit in enumerate(fruits):
        engine.set(fruit, f"value_{i}")
    print(f"  Stats: {engine.stats()}")

    # --- Step 2: Read ---
    print("\n[Step 2] Reading keys...")
    for key in ["apple", "mango", "zebra"]:
        result = engine.get(key)
        print(f"  GET {key!r:12} → {result!r}")

    # --- Step 3: Update ---
    print("\n[Step 3] Updating 'apple'...")
    engine.set("apple", "updated_apple_value")
    print(f"  GET apple → {engine.get('apple')!r}")

    # --- Step 4: Delete ---
    print("\n[Step 4] Deleting 'banana'...")
    engine.delete("banana")
    print(f"  GET banana → {engine.get('banana')!r} (None = deleted)")

    # --- Step 5: Range scan ---
    print("\n[Step 5] Range scan 'c' to 'f'...")
    for k, v in engine.scan("c", "f"):
        print(f"  {k!r}: {v!r}")

    # --- Step 6: Force flush to trigger SSTable creation ---
    print("\n[Step 6] Forcing flush to SSTable...")
    engine.flush()
    print(f"  Stats after flush: {engine.stats()}")

    # --- Step 7: Read after flush (data now in SSTable) ---
    print("\n[Step 7] Reading after flush (data is now on disk)...")
    print(f"  GET apple  → {engine.get('apple')!r}")
    print(f"  GET banana → {engine.get('banana')!r} (still deleted)")
    print(f"  GET mango  → {engine.get('mango')!r}")

    # --- Step 8: Simulate crash recovery ---
    print("\n[Step 8] Simulating crash & recovery...")
    engine._wal.close()
    # Write something new (will be in WAL but not yet flushed)
    engine2 = LSMEngine(data_dir)
    engine2.set("recovery_test", "survived_crash")
    engine2._wal.close()

    engine3 = LSMEngine(data_dir)
    print(f"  GET recovery_test → {engine3.get('recovery_test')!r}")
    print(f"  GET apple         → {engine3.get('apple')!r}")

    engine3.close()
    shutil.rmtree(data_dir)

    print("\n[Done] LSM-Tree engine demo complete.")
    print("\nKey insights:")
    print("  1. Writes go to WAL + MemTable (fast, durable)")
    print("  2. MemTable flushes to SSTable when full")
    print("  3. SSTables are immutable — never modified")
    print("  4. Compaction merges SSTables, removes tombstones")
    print("  5. On crash: replay WAL to recover unflushed writes")