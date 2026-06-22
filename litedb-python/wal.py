"""
wal.py — Write-Ahead Log (WAL)

CONCEPT:
  Before writing data anywhere, write it to the WAL first.
  The WAL is an append-only file on disk.
  If the process crashes mid-write, on restart we replay the WAL
  to recover all committed writes — nothing is lost.

  This is how PostgreSQL, MySQL, RocksDB, and Cassandra all guarantee
  durability (the 'D' in ACID).

WAL Entry Format (binary):
  [4 bytes: entry_length] [N bytes: JSON payload] [4 bytes: CRC32 checksum]

  CRC32 detects corruption — if the last entry is truncated (crash during write),
  the checksum won't match and we skip that entry.
"""

import json
import os
import struct
import threading
import zlib
from typing import Iterator

# Sentinel value for deleted keys (tombstone)
TOMBSTONE = "__DELETED__"


class WALEntry:
    """A single entry in the Write-Ahead Log."""

    def __init__(self, sequence: int, operation: str, key: str, value: str | None = None):
        self.sequence = sequence   # monotonically increasing ID
        self.operation = operation # "SET" or "DELETE"
        self.key = key
        self.value = value         # None for DELETE

    def to_dict(self) -> dict:
        return {
            "seq": self.sequence,
            "op": self.operation,
            "key": self.key,
            "val": self.value,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "WALEntry":
        return cls(
            sequence=d["seq"],
            operation=d["op"],
            key=d["key"],
            value=d.get("val"),  # type: ignore[arg-type]
        )

    def __repr__(self):
        return f"WALEntry(seq={self.sequence}, op={self.operation}, key={self.key!r}, val={self.value!r})"


class WriteAheadLog:
    """
    Append-only Write-Ahead Log.

    Every write (SET/DELETE) is appended here BEFORE being applied
    to the MemTable. On crash recovery, we replay this file.

    File format per entry:
      [4 bytes big-endian uint32: payload_length]
      [payload_length bytes: UTF-8 JSON]
      [4 bytes big-endian uint32: CRC32 of payload]
    """

    HEADER_SIZE = 4   # payload length prefix
    CHECKSUM_SIZE = 4 # CRC32 suffix

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._sequence = 0

        # Open in append+binary mode; create if not exists
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        self._file = open(path, "ab")

        # Recover sequence number from existing WAL
        for entry in self.read_all():
            if entry.sequence >= self._sequence:
                self._sequence = entry.sequence + 1

        print(f"[WAL] Opened {path!r}, next sequence={self._sequence}")

    # ------------------------------------------------------------------ #
    #  Write path                                                          #
    # ------------------------------------------------------------------ #

    def append(self, operation: str, key: str, value: str | None = None) -> WALEntry:
        """
        Append a new entry to the WAL.
        Returns the WALEntry with its assigned sequence number.
        fsync() ensures the entry is on disk before we return.
        """
        with self._lock:
            entry = WALEntry(self._sequence, operation, key, value)
            self._sequence += 1
            self._write_entry(entry)
            return entry

    def _write_entry(self, entry: WALEntry):
        payload = json.dumps(entry.to_dict()).encode("utf-8")
        length = struct.pack(">I", len(payload))          # 4-byte big-endian
        checksum = struct.pack(">I", zlib.crc32(payload) & 0xFFFFFFFF)

        self._file.write(length)
        self._file.write(payload)
        self._file.write(checksum)
        self._file.flush()
        os.fsync(self._file.fileno())  # force to disk — this is the durability guarantee

    # ------------------------------------------------------------------ #
    #  Read / Recovery path                                               #
    # ------------------------------------------------------------------ #

    def read_all(self) -> Iterator[WALEntry]:
        """
        Read all valid entries from the WAL file.
        Skips corrupted/truncated entries at the end (crash during write).
        Used during crash recovery to rebuild the MemTable.
        """
        if not os.path.exists(self.path):
            return

        with open(self.path, "rb") as f:
            while True:
                # Read length prefix
                length_bytes = f.read(self.HEADER_SIZE)
                if len(length_bytes) < self.HEADER_SIZE:
                    break  # EOF

                payload_length = struct.unpack(">I", length_bytes)[0]

                # Read payload
                payload = f.read(payload_length)
                if len(payload) < payload_length:
                    print("[WAL] Truncated entry at offset — skipping (crash during write)")
                    break

                # Read checksum
                checksum_bytes = f.read(self.CHECKSUM_SIZE)
                if len(checksum_bytes) < self.CHECKSUM_SIZE:
                    print("[WAL] Missing checksum — skipping")
                    break

                # Verify checksum
                expected_crc = struct.unpack(">I", checksum_bytes)[0]
                actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                if expected_crc != actual_crc:
                    print("[WAL] CRC mismatch — entry corrupted, skipping")
                    break

                try:
                    d = json.loads(payload.decode("utf-8"))
                    yield WALEntry.from_dict(d)
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"[WAL] Malformed entry: {e} — skipping")
                    break

    def truncate(self):
        """
        Delete the WAL file after a successful MemTable flush to SSTable.
        Once data is safely on disk as an SSTable, the WAL is no longer needed.
        """
        with self._lock:
            self._file.close()
            os.remove(self.path)
            self._file = open(self.path, "ab")
            print("[WAL] Truncated (data safely flushed to SSTable)")

    def close(self):
        with self._lock:
            self._file.flush()
            self._file.close()

    def __repr__(self):
        return f"WriteAheadLog(path={self.path!r}, next_seq={self._sequence})"


# ======================================================================= #
#  DEMO — run this file directly to see the WAL in action                 #
# ======================================================================= #

if __name__ == "__main__":
    import shutil
    import tempfile

    data_dir = tempfile.mkdtemp(prefix="litedb_wal_demo_")
    wal_path = os.path.join(data_dir, "wal.log")

    print("=" * 60)
    print("WAL DEMO")
    print("=" * 60)

    # --- Step 1: Write some entries ---
    print("\n[Step 1] Writing entries to WAL...")
    wal = WriteAheadLog(wal_path)
    e1 = wal.append("SET", "name", "Alice")
    e2 = wal.append("SET", "age", "30")
    e3 = wal.append("SET", "city", "New York")
    e4 = wal.append("DELETE", "age")
    print(f"  Written: {e1}")
    print(f"  Written: {e2}")
    print(f"  Written: {e3}")
    print(f"  Written: {e4}")
    wal.close()

    # --- Step 2: Simulate crash & recovery ---
    print("\n[Step 2] Simulating crash... reopening WAL for recovery")
    wal2 = WriteAheadLog(wal_path)
    print("\n[Step 3] Replaying WAL entries (crash recovery):")
    for entry in wal2.read_all():
        print(f"  Replaying: {entry}")

    # --- Step 3: Show file on disk ---
    size = os.path.getsize(wal_path)
    print(f"\n[Step 4] WAL file size on disk: {size} bytes")
    print(f"         Location: {wal_path}")

    wal2.close()
    shutil.rmtree(data_dir)
    print("\n[Done] WAL demo complete.")
    print("\nKey insight: Even if the process crashed after Step 1,")
    print("we can replay the WAL to recover all 4 operations.")
