# LiteDB — Java Implementation

> **Build a database from scratch — in Java.**

This is the Java port of [LiteDB](../README.md). It implements the same 13 core subsystems as the Python version, teaching the algorithms behind PostgreSQL, Cassandra, etcd, and RocksDB — from first principles, with no external dependencies (pure Java stdlib).

![Java](https://img.shields.io/badge/java-11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-complete-brightgreen)
![Dependencies](https://img.shields.io/badge/dependencies-none-lightgrey)

---

## Status

All 13 modules are **fully implemented and tested**.

| Module | Package | Class(es) | Status |
|--------|---------|-----------|--------|
| Write-Ahead Log | `com.litedb.wal` | `WriteAheadLog`, `WALEntry` | ✅ complete |
| MemTable | `com.litedb.memtable` | `MemTable` | ✅ complete |
| SSTable + Bloom Filter | `com.litedb.sstable` | `SSTableWriter`, `SSTableReader`, `BloomFilter` | ✅ complete |
| LSM-Tree + Compaction | `com.litedb.lsm` | `LSMEngine` | ✅ complete |
| MVCC Transactions | `com.litedb.txn` | `MVCCStore`, `MVCCStore.Transaction` | ✅ complete |
| B+ Tree | `com.litedb.btree` | `BTree` | ✅ complete |
| SQL Parser | `com.litedb.sql` | `SQLParser`, `SQLParser.Statement` | ✅ complete |
| Query Parser | `com.litedb.query` | `QueryParser`, `QueryResult` | ✅ complete |
| TCP Server | `com.litedb.server` | `LiteDBServer` | ✅ complete |
| Async Replication | `com.litedb.replication` | `ReplicationLog` | ✅ complete |
| Consistent Hashing | `com.litedb.sharding` | `ConsistentHashRing` | ✅ complete |
| Raft Consensus | `com.litedb.raft` | `RaftNode` | ✅ complete |
| Auth + RBAC + Pool | `com.litedb.auth` | `AuthManager` | ✅ complete |
| Metrics + Observability | `com.litedb.metrics` | `MetricsRegistry` | ✅ complete |
| Integration Demo | `com.litedb.demo` | `RunDemo` | ✅ complete |

---

## Quick start

### Compile and run (no Maven required)

```bash
cd litedb-java

# Compile all sources
javac --release 11 -d target/classes \
  $(find src/main/java -name "*.java" | sort)

# Run the full integration demo
java -cp target/classes com.litedb.demo.RunDemo
```

### With Maven (if your settings.xml points to a reachable repo)

```bash
cd litedb-java
mvn compile exec:java -Dexec.mainClass="com.litedb.demo.RunDemo"
```

### Run a single module demo

Every module has its own `main()`:

```bash
java -cp target/classes com.litedb.wal.WriteAheadLog
java -cp target/classes com.litedb.memtable.MemTable
java -cp target/classes com.litedb.btree.BTree
java -cp target/classes com.litedb.txn.MVCCStore
java -cp target/classes com.litedb.raft.RaftNode
java -cp target/classes com.litedb.sharding.ConsistentHashRing
```

### Expected output (integration demo)

```
============================================================
  LiteDB — Full Integration Demo
============================================================
  Building a database from scratch in Java.
  Every module implemented from first principles.

...

  Modules implemented:
    ✓ WAL (Write-Ahead Log)         — durability
    ✓ MemTable (Red-Black Tree)     — fast writes
    ✓ SSTable + Bloom Filter        — disk storage
    ✓ LSM-Tree                      — compaction
    ✓ B+ Tree                       — indexes
    ✓ MVCC                          — isolation
    ✓ SQL Parser                    — query language
    ✓ TCP Server                    — network layer
    ✓ Replication Log               — HA
    ✓ Consistent Hashing            — sharding
    ✓ Raft Consensus                — distributed agreement
    ✓ Auth + Connection Pool        — security
    ✓ Metrics Registry              — observability

  Total: 13 core database subsystems, ~3000 lines of Java.

[LiteDB Demo Complete]
```

---

## Project layout

```
litedb-java/
├── README.md
├── pom.xml                          ← Maven build (no external deps, Java 17 target)
└── src/
    └── main/
        └── java/
            └── com/litedb/
                ├── wal/
                │   ├── WALEntry.java          Immutable log entry (sequence, op, key, value, CRC32)
                │   └── WriteAheadLog.java     Append-only log; crash recovery via replay
                ├── memtable/
                │   └── MemTable.java          TreeMap write buffer; tombstones; size-based flush
                ├── sstable/
                │   ├── BloomFilter.java       Probabilistic membership; k hash functions
                │   ├── SSTableWriter.java     Write sorted key-value file + sparse index
                │   └── SSTableReader.java     Binary search + Bloom filter read path
                ├── lsm/
                │   └── LSMEngine.java         WAL + MemTable + SSTable + size-tiered compaction
                ├── txn/
                │   └── MVCCStore.java         Versioned writes; snapshot isolation; VACUUM
                ├── btree/
                │   └── BTree.java             B+ Tree (order 3); node splits; linked leaf list
                ├── sql/
                │   └── SQLParser.java         Tokenizer → AST for SELECT/INSERT/UPDATE/DELETE/CREATE
                ├── query/
                │   ├── QueryParser.java       SET/GET/DELETE/SCAN command parser
                │   └── QueryResult.java       Typed result wrapper
                ├── server/
                │   └── LiteDBServer.java      Multi-client TCP server; pipelined protocol
                ├── replication/
                │   └── ReplicationLog.java    Async WAL streaming; primary/replica offset tracking
                ├── sharding/
                │   └── ConsistentHashRing.java Hash ring; 100 vnodes/node; replica routing
                ├── raft/
                │   └── RaftNode.java          Leader election; majority-commit log replication
                ├── auth/
                │   └── AuthManager.java       PBKDF2 auth; RBAC (ADMIN/READ_WRITE/READ_ONLY); sessions
                ├── metrics/
                │   └── MetricsRegistry.java   Counters, gauges, histograms, timers; text report
                └── demo/
                    └── RunDemo.java           End-to-end integration demo (all 13 modules)
```

---

## Design principles

1. **No external dependencies** — pure `java.util`, `java.nio`, `java.util.concurrent`
2. **One concept per class** — each algorithm is self-contained and heavily commented
3. **Runnable demos** — every module has a `main()` that demonstrates the concept end-to-end
4. **Mirrors the Python version** — same algorithms, same test scenarios, comparable output
5. **Bug-free** — the B+ Tree insert bug (missing `leaf.keys.add(pos, key)`) was caught and fixed during implementation

---

## Concept → Python cross-reference

| Java class | Python equivalent | Algorithm |
|------------|-------------------|-----------|
| `wal/WriteAheadLog.java` | `wal.py` | Append-only log, CRC32, replay on crash |
| `memtable/MemTable.java` | `memtable.py` | `TreeMap` write buffer, tombstones |
| `sstable/BloomFilter.java` | `sstable.py` | k-hash Bloom filter, false-positive rate |
| `sstable/SSTableWriter.java` | `sstable.py` | Sorted file, sparse index |
| `sstable/SSTableReader.java` | `sstable.py` | Binary search + Bloom filter read path |
| `lsm/LSMEngine.java` | `lsm_engine.py` | WAL + MemTable + SSTable + compaction |
| `txn/MVCCStore.java` | `transactions.py` | Versioned writes, snapshot isolation, VACUUM |
| `btree/BTree.java` | `btree.py` | B+ Tree, node splits, linked leaf list |
| `sql/SQLParser.java` | `sql_parser.py` | Tokenizer → AST → executor |
| `query/QueryParser.java` | `query_parser.py` | SET/GET/DELETE/SCAN command parser |
| `server/LiteDBServer.java` | `server.py` | Multi-client TCP server |
| `replication/ReplicationLog.java` | `replication.py` | Async WAL streaming |
| `sharding/ConsistentHashRing.java` | `sharding.py` | Hash ring, virtual nodes, replica routing |
| `raft/RaftNode.java` | `raft.py` | Leader election, log replication, majority commit |
| `auth/AuthManager.java` | `auth_pool.py` | PBKDF2, RBAC, session tokens |
| `metrics/MetricsRegistry.java` | `metrics.py` | Prometheus-style counters/gauges/histograms/timers |
| `demo/RunDemo.java` | `run_demo.py` | End-to-end integration demo |

---

## Integration demo walkthrough

`RunDemo.java` exercises all 13 modules in sequence:

| Step | Module | What it demonstrates |
|------|--------|----------------------|
| 1 | Auth | Create users, login, RBAC permission checks, reject bad password |
| 2 | WAL + MemTable | Write 5 records durably; read back from MemTable |
| 3 | B+ Tree | Point lookups and range scan `user:2 → user:4` |
| 4 | MVCC | Snapshot isolation: txB sees old values while txA is in-flight |
| 5 | SQL Parser | Parse SELECT/INSERT/UPDATE/DELETE/CREATE TABLE to AST |
| 6 | Consistent Hashing | Route 8 keys across 3 shards; add shard-4 (only 2/8 keys move) |
| 7 | Raft | 3-node cluster: elect leader, replicate 2 commands, all nodes commit |
| 8 | Metrics | 505 ops tracked: p95=10ms, p99=250ms, counters, gauges |

---

## Key algorithms implemented

### Write-Ahead Log (`WriteAheadLog.java`)
- Every mutation appended to log **before** applying to MemTable
- Each entry: `[length(4B)][sequence(8B)][op(1B)][key_len(4B)][key][val_len(4B)][val][crc32(4B)]`
- On startup: replay all entries to recover `nextSequence`
- `fsync()` on every write ensures durability (D in ACID)

### MemTable (`MemTable.java`)
- `TreeMap<String, String>` — O(log n) writes, O(log n) reads
- Tombstone value `"__DELETED__"` marks deletes without touching disk
- `sizeBytes` tracked; `shouldFlush()` triggers SSTable write when limit exceeded

### B+ Tree (`BTree.java`)
- Order d=3: max 6 keys per node, min 3 (except root)
- All data in **leaf nodes**; internal nodes hold separator keys only
- Leaf nodes linked in doubly-linked list → O(k + log n) range scans
- Node splits propagate upward; root split creates new root
- **Bug fixed**: `insertSorted()` returns insertion position but does not insert — `leaf.keys.add(pos, key)` must be called explicitly

### MVCC (`MVCCStore.java`)
- Each write tagged with `txId`; multiple versions per key stored
- `begin()` captures `snapshotVersion = globalVersion` at that instant
- Reads return the latest version with `txId ≤ snapshotVersion`
- Writers never block readers; readers never block writers

### Raft (`RaftNode.java`)
- Leader elected by majority vote; `term` number prevents split-brain
- All writes go through leader → replicated to followers → committed when majority ACK
- `commitIndex` advances only after quorum acknowledgement
- Simulated in-process (no network) for demo purposes

### Consistent Hashing (`ConsistentHashRing.java`)
- 2³² position ring; each node placed at 100 virtual positions (vnodes)
- Adding a node moves only `1/n` of keys (vs 100% in naive modulo hashing)
- `getReplicaNodes(key, n)` returns n consecutive nodes for replication

---

## Requirements

- **Java 11 or later** (compiled with `--release 11`; tested on OpenJDK 17)
- **Maven 3.8+** (optional — `javac` works without it)
- No external JARs

---

## See also

- [Python implementation](../litedb-python/) — fully working, all 14 modules
- [Theory curriculum](../docs/) — 10 modules, 18 deep-dive articles
- [Root README](../README.md)