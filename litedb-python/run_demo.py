"""
run_demo.py — LiteDB Comprehensive Demo

Runs all 14 modules in sequence:
  Foundational (inline):
    WAL → MemTable → SSTable → LSM Engine → Query Parser → Replication

  Advanced (subprocess — each module has its own self-contained demo):
    Transactions → B-Tree → SQL Parser → Sharding → Raft → Auth/Pool → Metrics

Usage:
    python run_demo.py              # run all 14 modules
    python run_demo.py wal          # run one foundational module by name
    python run_demo.py transactions # run one advanced module by name

Pure Python stdlib — no dependencies to install.
"""

import sys
import os
import tempfile
import shutil
import time
import threading
import subprocess

# Make sure we can import our modules
_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _dir)

import _loader  # type: ignore  # ensures litedb/ is on sys.path

from wal import WriteAheadLog, WALEntry                          # type: ignore
from memtable import MemTable, TOMBSTONE                         # type: ignore
from sstable import SSTableWriter, SSTableReader, BloomFilter    # type: ignore
from lsm_engine import LSMEngine                                 # type: ignore
from query_parser import QueryParser                             # type: ignore
from replication import ReplicationPublisher, ReplicationSubscriber  # type: ignore


def separator(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def section(title: str):
    print(f"\n{'─' * 50}")
    print(f"  {title}")
    print(f"{'─' * 50}")


# ======================================================================= #
#  PART 1: WAL                                                             #
# ======================================================================= #

def demo_wal(data_dir: str):
    separator("PART 1: Write-Ahead Log (WAL)")
    print("""
  The WAL is the foundation of durability.
  Every write is appended here BEFORE touching memory.
  On crash, we replay the WAL to recover all committed writes.
    """)

    wal_path = os.path.join(data_dir, "demo_wal.log")
    wal = WriteAheadLog(wal_path)

    section("Writing 4 entries to WAL")
    entries = [
        ("SET", "user:1", "Alice"),
        ("SET", "user:2", "Bob"),
        ("SET", "score:1", "9500"),
        ("DELETE", "user:2"),
    ]
    for op, key, *val in entries:
        e = wal.append(op, key, val[0] if val else None)
        print(f"  WAL ← {op:6} {key!r:15} seq={e.sequence}")

    wal.close()

    section("Crash recovery: replaying WAL")
    wal2 = WriteAheadLog(wal_path)
    for entry in wal2.read_all():
        print(f"  Replayed: seq={entry.sequence} {entry.operation} {entry.key!r} = {entry.value!r}")
    wal2.close()

    print(f"\n  ✓ WAL file: {os.path.getsize(wal_path)} bytes on disk")


# ======================================================================= #
#  PART 2: MemTable                                                        #
# ======================================================================= #

def demo_memtable():
    separator("PART 2: MemTable (In-Memory Write Buffer)")
    print("""
  The MemTable buffers recent writes in memory.
  Reads check here first — most recent data is always here.
  Deletes write a TOMBSTONE (not an actual removal).
    """)

    mt = MemTable(size_limit_bytes=512)

    section("Writes and reads")
    mt.set("apple", "red")
    mt.set("banana", "yellow")
    mt.set("cherry", "red")
    mt.set("date", "brown")
    print(f"  SET apple=red, banana=yellow, cherry=red, date=brown")
    print(f"  GET apple  → {mt.get('apple')!r}")
    print(f"  GET mango  → {mt.get('mango')!r}  (not found)")

    section("Tombstone delete")
    mt.delete("banana")
    print(f"  DELETE banana")
    print(f"  GET banana → {mt.get('banana')!r}  ← TOMBSTONE")

    section("Sorted items (as flushed to SSTable)")
    for k, v in mt.items_sorted():
        marker = " ← TOMBSTONE" if v == TOMBSTONE else ""
        print(f"  {k!r:12} → {v!r}{marker}")

    print(f"\n  ✓ Should flush: {mt.should_flush()} (size={mt.size_bytes()} bytes, limit=512)")


# ======================================================================= #
#  PART 3: SSTable                                                         #
# ======================================================================= #

def demo_sstable(data_dir: str):
    separator("PART 3: SSTable (Sorted String Table on Disk)")
    print("""
  An SSTable is an immutable sorted file on disk.
  Written once when MemTable flushes. Never modified.
  Uses a Bloom filter to skip files that don't contain a key.
  Uses a sparse index for fast point lookups.
    """)

    sst_path = os.path.join(data_dir, "demo.sst")

    section("Writing SSTable")
    data = [
        ("apple", "red fruit"),
        ("banana", TOMBSTONE),
        ("cherry", "red berry"),
        ("date", "brown fruit"),
        ("elderberry", "dark berry"),
        ("fig", "purple fruit"),
        ("grape", "green or purple"),
        ("honeydew", "green melon"),
        ("kiwi", "brown outside green inside"),
        ("lemon", "yellow citrus"),
    ]
    writer = SSTableWriter(sst_path)
    reader = writer.write(data)
    print(f"  Written {reader.entry_count} entries to {os.path.getsize(sst_path)} bytes")

    section("Point lookups (with Bloom filter)")
    for key in ["apple", "cherry", "mango", "banana"]:
        result = reader.get(key)
        if result is None:
            print(f"  GET {key!r:12} → NOT FOUND  (Bloom filter saved a disk read)")
        elif result == TOMBSTONE:
            print(f"  GET {key!r:12} → TOMBSTONE  (deleted)")
        else:
            print(f"  GET {key!r:12} → {result!r}")

    section("Range scan: 'c' to 'g'")
    for k, v in reader.scan("c", "g"):
        marker = " ← TOMBSTONE" if v == TOMBSTONE else ""
        print(f"  {k!r:12} → {v!r}{marker}")


# ======================================================================= #
#  PART 4: LSM Engine                                                      #
# ======================================================================= #

def demo_lsm_engine(data_dir: str):
    separator("PART 4: LSM-Tree Engine (Full Stack)")
    print("""
  The LSM engine combines WAL + MemTable + SSTables.
  Write path: WAL → MemTable → (flush) → SSTable → (compact)
  Read path:  MemTable → L0 SSTables → L1 SSTables
    """)

    engine_dir = os.path.join(data_dir, "lsm")
    engine = LSMEngine(engine_dir)

    section("Writing data")
    records = {
        "user:alice": "Alice Smith",
        "user:bob": "Bob Jones",
        "user:carol": "Carol White",
        "score:alice": "9500",
        "score:bob": "8200",
        "score:carol": "9100",
        "config:max_users": "1000",
        "config:version": "2.1",
    }
    for k, v in records.items():
        engine.set(k, v)
    print(f"  Wrote {len(records)} records")
    print(f"  Stats: {engine.stats()}")

    section("Point reads")
    for key in ["user:alice", "score:bob", "user:dave"]:
        result = engine.get(key)
        print(f"  GET {key!r:20} → {result!r}")

    section("Update and delete")
    engine.set("score:alice", "9999")
    engine.delete("user:bob")
    print(f"  Updated score:alice → {engine.get('score:alice')!r}")
    print(f"  Deleted user:bob   → {engine.get('user:bob')!r}  (None = deleted)")

    section("Range scan: 'score:' prefix")
    for k, v in engine.scan("score:", "score:z"):
        print(f"  {k!r:20} → {v!r}")

    section("Flush to SSTable")
    engine.flush()
    print(f"  Stats after flush: {engine.stats()}")

    section("Read after flush (data is on disk now)")
    print(f"  GET user:alice  → {engine.get('user:alice')!r}")
    print(f"  GET user:bob    → {engine.get('user:bob')!r}  (still deleted)")

    section("Crash recovery simulation")
    engine._wal.close()
    engine2 = LSMEngine(engine_dir)
    engine2.set("post_crash_key", "survived!")
    engine2._wal.close()

    engine3 = LSMEngine(engine_dir)
    print(f"  GET post_crash_key → {engine3.get('post_crash_key')!r}")
    print(f"  GET user:alice     → {engine3.get('user:alice')!r}")
    engine3.close()


# ======================================================================= #
#  PART 5: Query Parser                                                    #
# ======================================================================= #

def demo_query_parser(data_dir: str):
    separator("PART 5: Query Parser")
    print("""
  The query parser converts raw text commands into engine operations.
  This is the interface between clients and the storage engine.
    """)

    engine_dir = os.path.join(data_dir, "parser_engine")
    engine = LSMEngine(engine_dir)
    parser = QueryParser(engine)

    section("Executing commands")
    commands = [
        "PING",
        "SET name Alice",
        "SET age 30",
        'SET bio "Software Engineer at LiteDB Corp"',
        "GET name",
        "GET age",
        "GET missing",
        "SCAN a z",
        "DELETE age",
        "GET age",
        "STATS",
        "HELP",
        "BADCMD",
        "SET",
    ]

    for cmd in commands:
        result = parser.execute(cmd)
        wire = result.to_wire().rstrip("\n").replace("\n", " | ")
        print(f"  > {cmd:<40} → {wire}")

    engine.close()


# ======================================================================= #
#  PART 6: Replication                                                     #
# ======================================================================= #

def demo_replication():
    separator("PART 6: Async WAL Replication")
    print("""
  Replication streams WAL entries from primary to replica.
  Replica sends its offset on connect → primary replays missed entries.
  This is async: primary doesn't wait for replica acknowledgment.
    """)

    primary_store: dict[str, str] = {}
    replica_store: dict[str, str] = {}
    applied_log: list[str] = []

    publisher = ReplicationPublisher("127.0.0.1", 17380)
    publisher.start()
    time.sleep(0.1)

    def apply_fn(entry: WALEntry):
        if entry.operation == "SET":
            replica_store[entry.key] = entry.value or ""
        elif entry.operation == "DELETE":
            replica_store.pop(entry.key, None)
        applied_log.append(f"seq={entry.sequence} {entry.operation} {entry.key!r}")

    subscriber = ReplicationSubscriber("127.0.0.1", 17380, apply_fn, initial_offset=-1)
    subscriber.start()
    time.sleep(0.2)

    section("Writing to primary (async replication to replica)")
    seq = 0
    writes = [
        ("SET", "user:1", "Alice"),
        ("SET", "user:2", "Bob"),
        ("SET", "user:3", "Carol"),
        ("DELETE", "user:2"),
        ("SET", "config:version", "3.0"),
    ]
    for op, key, *val in writes:
        value = val[0] if val else None
        if op == "SET":
            primary_store[key] = value or ""
        else:
            primary_store.pop(key, None)
        entry = WALEntry(seq, op, key, value)
        seq += 1
        publisher.publish(entry)
        print(f"  Primary: {op} {key!r}" + (f" = {value!r}" if value else ""))

    time.sleep(0.4)

    section("State comparison after replication")
    print(f"  Primary: {primary_store}")
    print(f"  Replica: {replica_store}")
    match = primary_store == replica_store
    status = "✓ IN SYNC" if match else "✗ LAGGING"
    print(f"  Status: {status}")

    section("Entries applied by replica")
    for log_line in applied_log:
        print(f"  {log_line}")

    subscriber.stop()
    publisher.stop()


# ======================================================================= #
#  ADVANCED MODULES (subprocess runner)                                    #
# ======================================================================= #

ADVANCED_MODULES = [
    ("transactions", "transactions.py", "MVCC Transactions — Optimistic & Pessimistic Locking"),
    ("btree",        "btree.py",        "B-Tree Storage Engine"),
    ("sql_parser",   "sql_parser.py",   "SQL Parser & Query Planner"),
    ("sharding",     "sharding.py",     "Consistent Hashing & Sharding"),
    ("raft",         "raft.py",         "Raft Consensus Algorithm"),
    ("auth_pool",    "auth_pool.py",    "Auth, RBAC & Connection Pool"),
    ("metrics",      "metrics.py",      "Metrics, Slow Query Log & Tracing"),
]


def run_advanced_module(name: str, filename: str, title: str) -> bool:
    path = os.path.join(_dir, filename)
    print()
    print("╔" + "═" * 62 + "╗")
    print(f"║  {title:<62}║")
    print("╚" + "═" * 62 + "╝")
    print()

    start = time.time()
    result = subprocess.run(
        [sys.executable, path],
        cwd=_dir,
        capture_output=False,
    )
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"\n  ✓ {filename} completed in {elapsed:.2f}s")
        return True
    else:
        print(f"\n  ✗ {filename} FAILED (exit code {result.returncode})")
        return False


# ======================================================================= #
#  MAIN                                                                    #
# ======================================================================= #

FOUNDATIONAL_MODULES = ["wal", "memtable", "sstable", "lsm_engine", "query_parser", "replication"]
ADVANCED_MODULE_NAMES = [name for name, _, _ in ADVANCED_MODULES]
ALL_MODULE_NAMES = FOUNDATIONAL_MODULES + ADVANCED_MODULE_NAMES


def main():
    filter_arg = sys.argv[1].lower() if len(sys.argv) > 1 else None

    # Validate filter
    if filter_arg and filter_arg not in ALL_MODULE_NAMES:
        print(f"No module matching {filter_arg!r}. Available modules:")
        print("  Foundational: " + ", ".join(FOUNDATIONAL_MODULES))
        print("  Advanced:     " + ", ".join(ADVANCED_MODULE_NAMES))
        sys.exit(1)

    print("\n" + "█" * 64)
    print("  LiteDB — Build Your Own Database")
    print("  Comprehensive Demo — All 14 Modules")
    print("█" * 64)
    print("""
  Foundational modules (inline):
    1. WAL          — durability via append-only log
    2. MemTable     — fast in-memory write buffer
    3. SSTable      — immutable sorted file on disk
    4. LSM Engine   — combines all three + compaction
    5. Query Parser — text command → engine operation
    6. Replication  — async WAL streaming to replica

  Advanced modules (self-contained):
    7.  Transactions — MVCC + optimistic (OCC) + pessimistic (2PL) locking
    8.  B-Tree       — page-based tree storage engine
    9.  SQL Parser   — SELECT/INSERT/UPDATE/DELETE + JOINs
    10. Sharding     — consistent hashing + virtual nodes
    11. Raft         — leader election + log replication
    12. Auth/Pool    — PBKDF2 auth, RBAC, connection pool
    13. Metrics      — Prometheus-style metrics + tracing
    """)

    data_dir = tempfile.mkdtemp(prefix="litedb_demo_")
    print(f"  Temp data directory: {data_dir}\n")

    passed = 0
    failed = 0

    try:
        # ── Foundational modules ──────────────────────────────────────── #
        foundational = [
            ("wal",          lambda: demo_wal(data_dir)),
            ("memtable",     lambda: demo_memtable()),
            ("sstable",      lambda: demo_sstable(data_dir)),
            ("lsm_engine",   lambda: demo_lsm_engine(data_dir)),
            ("query_parser", lambda: demo_query_parser(data_dir)),
            ("replication",  lambda: demo_replication()),
        ]

        for name, fn in foundational:
            if filter_arg and filter_arg != name:
                continue
            try:
                fn()
                passed += 1
            except Exception as exc:
                print(f"\n  ✗ {name} FAILED: {exc}")
                failed += 1

        # ── Advanced modules ──────────────────────────────────────────── #
        for name, filename, title in ADVANCED_MODULES:
            if filter_arg and filter_arg != name:
                continue
            ok = run_advanced_module(name, filename, title)
            if ok:
                passed += 1
            else:
                failed += 1

        # ── Summary ───────────────────────────────────────────────────── #
        if not filter_arg:
            separator("SUMMARY")
            print("""
  You just ran a real database from scratch. Here's what happened:

  ┌──────────────────────────────────────────────────────────────┐
  │  Component       Algorithm            Production Equivalent   │
  ├──────────────────────────────────────────────────────────────┤
  │  WAL             Append-only log      PostgreSQL WAL          │
  │                  CRC32 checksums      MySQL InnoDB redo log   │
  │                                                               │
  │  MemTable        Sorted dict          LevelDB SkipList        │
  │                  Tombstones           RocksDB MemTable        │
  │                                                               │
  │  SSTable         Sorted file          LevelDB SSTable         │
  │                  Bloom filter         RocksDB BlockBasedTable │
  │                  Sparse index         Cassandra SSTable       │
  │                                                               │
  │  LSM Engine      WAL+Mem+SST          LevelDB, RocksDB        │
  │                  Compaction           Cassandra, HBase        │
  │                  Crash recovery                               │
  │                                                               │
  │  Query Parser    Tokenizer            MySQL parser            │
  │                  Dispatcher           PostgreSQL executor     │
  │                                                               │
  │  Replication     WAL streaming        MySQL binlog            │
  │                  Offset tracking      PostgreSQL streaming    │
  │                  Async fanout         Cassandra gossip        │
  │                                                               │
  │  Transactions    MVCC + snapshots     PostgreSQL, MySQL InnoDB│
  │                  Optimistic (OCC)     CockroachDB             │
  │                  Pessimistic (2PL)    MySQL SELECT FOR UPDATE  │
  │                  get_for_update()     PostgreSQL FOR UPDATE    │
  │                                                               │
  │  B-Tree          Page-based tree      PostgreSQL heap         │
  │                  Linked leaves        MySQL InnoDB            │
  │                                                               │
  │  SQL Parser      Tokenizer→AST        MySQL, PostgreSQL       │
  │                  Planner+Executor     SQLite                  │
  │                                                               │
  │  Sharding        Consistent hashing   DynamoDB, Cassandra     │
  │                  Virtual nodes        MongoDB sharding        │
  │                                                               │
  │  Raft            Leader election      etcd, CockroachDB       │
  │                  Log replication      TiKV, Consul            │
  │                                                               │
  │  Auth/Pool       PBKDF2 + RBAC        PostgreSQL roles        │
  │                  Token bucket         PgBouncer               │
  │                                                               │
  │  Metrics         Prometheus counters  Prometheus, Datadog     │
  │                  Distributed tracing  Jaeger, Zipkin          │
  └──────────────────────────────────────────────────────────────┘
            """)

    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
        print(f"  Cleaned up temp directory.")

    print()
    print("=" * 64)
    print(f"  Results: {passed} passed, {failed} failed")
    print("=" * 64)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()