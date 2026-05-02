"""
sstable.py — SSTable (Sorted String Table)

CONCEPT:
  An SSTable is an immutable, sorted file on disk.
  Once written, it is NEVER modified — only read or deleted.

  When the MemTable fills up, we flush it to disk as an SSTable.
  The keys are written in sorted order, which enables:
    1. Binary search for point lookups (O(log n))
    2. Efficient range scans (sequential read)
    3. Efficient merge during compaction (merge-sort)

  SSTable File Format:
  ┌─────────────────────────────────────────────────────┐
  │  DATA BLOCK                                         │
  │  [entry] [entry] [entry] ... [entry]                │
  │                                                     │
  │  Each entry:                                        │
  │    [2 bytes: key_len] [key_len bytes: key]          │
  │    [4 bytes: val_len] [val_len bytes: value]        │
  │                                                     │
  │  INDEX BLOCK (at end of file)                       │
  │  [key → byte_offset] for every Nth entry            │
  │  (sparse index — not every key, just checkpoints)   │
  │                                                     │
  │  FOOTER                                             │
  │  [8 bytes: index_block_offset]                      │
  │  [8 bytes: index_block_length]                      │
  │  [4 bytes: entry_count]                             │
  │  [8 bytes: magic number 0xLITEDB01]                 │
  └─────────────────────────────────────────────────────┘

  Bloom Filter:
    Before reading the SSTable, check a Bloom filter.
    If the Bloom filter says "definitely not here" → skip the file.
    This avoids expensive disk reads for keys that don't exist.
"""

import os
import json
import struct
import zlib
from typing import Iterator

TOMBSTONE = "__DELETED__"
MAGIC = b"LITEDB01"  # 8-byte magic number to identify our files
INDEX_INTERVAL = 16  # write an index entry every 16 data entries


class BloomFilter:
    """
    A simple Bloom filter for probabilistic key membership testing.

    A Bloom filter uses multiple hash functions and a bit array.
    - add(key): set bits at hash positions
    - might_contain(key): if ANY bit is 0 → definitely not present
                          if ALL bits are 1 → probably present (false positive possible)

    False positive rate with m=10000 bits, k=3 hashes, n=1000 keys ≈ 1%
    This means: 1% of "not found" lookups will still read the SSTable.
                99% of "not found" lookups skip the SSTable entirely.
    """

    def __init__(self, size_bits: int = 10000, num_hashes: int = 3):
        self._bits = bytearray(size_bits // 8 + 1)
        self._size = size_bits
        self._num_hashes = num_hashes

    def _hash_positions(self, key: str) -> list[int]:
        """Generate k different hash positions for a key."""
        positions = []
        key_bytes = key.encode("utf-8")
        for seed in range(self._num_hashes):
            # Use CRC32 with different seeds as k independent hash functions
            h = zlib.crc32(key_bytes, seed * 0x9e3779b9 & 0xFFFFFFFF) & 0xFFFFFFFF
            positions.append(h % self._size)
        return positions

    def add(self, key: str) -> None:
        for pos in self._hash_positions(key):
            byte_idx = pos // 8
            bit_idx = pos % 8
            self._bits[byte_idx] |= (1 << bit_idx)

    def might_contain(self, key: str) -> bool:
        for pos in self._hash_positions(key):
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self._bits[byte_idx] & (1 << bit_idx)):
                return False  # definitely not present
        return True  # probably present

    def to_bytes(self) -> bytes:
        header = struct.pack(">II", self._size, self._num_hashes)
        return header + bytes(self._bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        size, num_hashes = struct.unpack(">II", data[:8])
        bf = cls(size_bits=size, num_hashes=num_hashes)
        bf._bits = bytearray(data[8:])
        return bf


class SSTableWriter:
    """
    Writes a new SSTable file from a sorted sequence of (key, value) pairs.
    Called when flushing a MemTable to disk.
    """

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)

    def write(self, sorted_items: list[tuple[str, str]]) -> "SSTableReader":
        """
        Write sorted_items to disk as an SSTable.
        Returns an SSTableReader for the newly created file.
        """
        bloom = BloomFilter()
        index: list[tuple[str, int]] = []  # (key, byte_offset) sparse index
        entry_count = 0

        with open(self.path, "wb") as f:
            for i, (key, value) in enumerate(sorted_items):
                # Record byte offset for sparse index (every INDEX_INTERVAL entries)
                if i % INDEX_INTERVAL == 0:
                    offset = f.tell()
                    index.append((key, offset))

                # Add key to bloom filter
                bloom.add(key)

                # Encode entry: [2B key_len][key][4B val_len][value]
                key_bytes = key.encode("utf-8")
                val_bytes = value.encode("utf-8")
                entry = (
                    struct.pack(">H", len(key_bytes)) +  # 2-byte key length
                    key_bytes +
                    struct.pack(">I", len(val_bytes)) +  # 4-byte value length
                    val_bytes
                )
                f.write(entry)
                entry_count += 1

            # Write index block
            index_offset = f.tell()
            index_data = json.dumps(index).encode("utf-8")
            f.write(struct.pack(">I", len(index_data)))
            f.write(index_data)
            index_end = f.tell()

            # Write bloom filter block
            bloom_offset = index_end
            bloom_data = bloom.to_bytes()
            f.write(struct.pack(">I", len(bloom_data)))
            f.write(bloom_data)

            # Write footer: [8B index_offset][4B index_len][4B entry_count][8B magic]
            f.write(struct.pack(">Q", index_offset))           # 8 bytes
            f.write(struct.pack(">I", len(index_data)))        # 4 bytes
            f.write(struct.pack(">I", entry_count))            # 4 bytes
            f.write(struct.pack(">Q", bloom_offset))           # 8 bytes
            f.write(MAGIC)                                     # 8 bytes

        size = os.path.getsize(self.path)
        print(f"[SSTable] Written {self.path!r}: {entry_count} entries, {size} bytes")
        return SSTableReader(self.path)


class SSTableReader:
    """
    Reads from an immutable SSTable file.
    Supports point lookups (get) and range scans (scan).
    Uses a Bloom filter to skip files that definitely don't contain a key.
    Uses a sparse index to avoid full file scans for point lookups.
    """

    FOOTER_SIZE = 8 + 4 + 4 + 8 + 8  # index_offset + index_len + entry_count + bloom_offset + magic

    def __init__(self, path: str):
        self.path = path
        self._index: list[tuple[str, int]] = []
        self._bloom: BloomFilter | None = None
        self._entry_count: int = 0
        self._index_offset: int = 0
        self._bloom_offset: int = 0
        self._load_metadata()

    def _load_metadata(self):
        """Read the footer and index block into memory at open time."""
        with open(self.path, "rb") as f:
            # Read footer from end of file
            f.seek(-self.FOOTER_SIZE, 2)  # seek from end
            footer = f.read(self.FOOTER_SIZE)

            magic = footer[-8:]
            if magic != MAGIC:
                raise ValueError(f"Not a valid SSTable file: {self.path}")

            self._index_offset = struct.unpack(">Q", footer[0:8])[0]
            index_len = struct.unpack(">I", footer[8:12])[0]
            self._entry_count = struct.unpack(">I", footer[12:16])[0]
            self._bloom_offset = struct.unpack(">Q", footer[16:24])[0]

            # Read index block
            f.seek(self._index_offset)
            index_len_check = struct.unpack(">I", f.read(4))[0]
            index_data = f.read(index_len_check)
            self._index = json.loads(index_data.decode("utf-8"))

            # Read bloom filter
            f.seek(self._bloom_offset)
            bloom_len = struct.unpack(">I", f.read(4))[0]
            bloom_data = f.read(bloom_len)
            self._bloom = BloomFilter.from_bytes(bloom_data)

    def get(self, key: str) -> str | None:
        """
        Point lookup for a key.
        1. Check Bloom filter — if definitely absent, return None immediately
        2. Use sparse index to find approximate position in file
        3. Scan forward from that position until key found or passed
        """
        # Step 1: Bloom filter check
        if self._bloom and not self._bloom.might_contain(key):
            return None  # definitely not in this SSTable

        # Step 2: Find best starting offset from sparse index
        start_offset = 0
        for idx_key, idx_offset in self._index:
            if idx_key <= key:
                start_offset = idx_offset
            else:
                break

        # Step 3: Scan forward from start_offset
        with open(self.path, "rb") as f:
            f.seek(start_offset)
            while f.tell() < self._index_offset:
                entry = self._read_entry(f)
                if entry is None:
                    break
                k, v = entry
                if k == key:
                    return v
                if k > key:
                    break  # passed it, key not here

        return None

    def scan(self, start_key: str, end_key: str) -> Iterator[tuple[str, str]]:
        """
        Range scan: yield all (key, value) pairs in [start_key, end_key].
        Uses sparse index to find start position.
        """
        # Find best starting offset
        start_offset = 0
        for idx_key, idx_offset in self._index:
            if idx_key <= start_key:
                start_offset = idx_offset
            else:
                break

        with open(self.path, "rb") as f:
            f.seek(start_offset)
            while f.tell() < self._index_offset:
                entry = self._read_entry(f)
                if entry is None:
                    break
                k, v = entry
                if k < start_key:
                    continue
                if k > end_key:
                    break
                yield k, v

    def _read_entry(self, f) -> tuple[str, str] | None:
        """Read one entry from the current file position."""
        key_len_bytes = f.read(2)
        if len(key_len_bytes) < 2:
            return None
        key_len = struct.unpack(">H", key_len_bytes)[0]
        key = f.read(key_len).decode("utf-8")

        val_len_bytes = f.read(4)
        if len(val_len_bytes) < 4:
            return None
        val_len = struct.unpack(">I", val_len_bytes)[0]
        value = f.read(val_len).decode("utf-8")

        return key, value

    def iter_all(self) -> Iterator[tuple[str, str]]:
        """Iterate all entries in sorted order. Used during compaction."""
        with open(self.path, "rb") as f:
            f.seek(0)
            while f.tell() < self._index_offset:
                entry = self._read_entry(f)
                if entry is None:
                    break
                yield entry

    @property
    def entry_count(self) -> int:
        return self._entry_count

    def __repr__(self) -> str:
        return f"SSTableReader(path={self.path!r}, entries={self._entry_count})"


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

if __name__ == "__main__":
    import tempfile, shutil

    data_dir = tempfile.mkdtemp(prefix="litedb_sstable_demo_")
    sstable_path = os.path.join(data_dir, "sst_000001.sst")

    print("=" * 60)
    print("SSTABLE DEMO")
    print("=" * 60)

    # --- Step 1: Write an SSTable ---
    print("\n[Step 1] Writing SSTable from sorted data...")
    sorted_data = [
        ("apple", "red fruit"),
        ("banana", TOMBSTONE),       # deleted key
        ("cherry", "red berry"),
        ("date", "brown fruit"),
        ("elderberry", "dark berry"),
        ("fig", "purple fruit"),
        ("grape", "green or purple"),
        ("honeydew", "green melon"),
    ]

    writer = SSTableWriter(sstable_path)
    reader = writer.write(sorted_data)
    print(f"  Created: {reader}")

    # --- Step 2: Point lookups ---
    print("\n[Step 2] Point lookups...")
    for key in ["apple", "cherry", "mango", "banana"]:
        result = reader.get(key)
        if result is None:
            print(f"  GET {key!r:12} → NOT FOUND (bloom filter or scan)")
        elif result == TOMBSTONE:
            print(f"  GET {key!r:12} → TOMBSTONE (deleted)")
        else:
            print(f"  GET {key!r:12} → {result!r}")

    # --- Step 3: Range scan ---
    print("\n[Step 3] Range scan: 'c' to 'f'...")
    for key, value in reader.scan("c", "f"):
        marker = " ← TOMBSTONE" if value == TOMBSTONE else ""
        print(f"  {key!r}: {value!r}{marker}")

    # --- Step 4: Bloom filter demo ---
    print("\n[Step 4] Bloom filter check...")
    test_keys = ["apple", "mango", "zebra", "cherry"]
    for key in test_keys:
        in_bloom = reader._bloom.might_contain(key) if reader._bloom else False
        actual = reader.get(key)
        print(f"  {key!r:12} bloom={in_bloom} actual={'found' if actual else 'not found'}")

    # --- Step 5: File size ---
    size = os.path.getsize(sstable_path)
    print(f"\n[Step 5] SSTable file size: {size} bytes for {reader.entry_count} entries")

    shutil.rmtree(data_dir)
    print("\n[Done] SSTable demo complete.")
    print("\nKey insights:")
    print("  1. SSTable is IMMUTABLE — written once, never modified")
    print("  2. Keys are sorted — enables binary search via sparse index")
    print("  3. Bloom filter avoids disk reads for missing keys")
    print("  4. Tombstones propagate deletes through the LSM levels")