# LiteDB — Documentation

This directory contains the full theory curriculum that accompanies the LiteDB implementation. Each module covers one major area of database engineering, with deep-dive articles that explain the algorithms and trade-offs used by real production systems.

---

## Table of Contents

### [Module 01 — Database Fundamentals](./01-fundamentals/)

The foundation: what databases are, why they exist, and how they are structured internally.

| Article | Topic |
|---------|-------|
| [Relational vs Non-Relational](./01-fundamentals/deep-dive-relational-vs-nonrelational.md) | SQL vs NoSQL, when to use each |
| [Atomicity](./01-fundamentals/deep-dive-atomicity.md) | All-or-nothing transactions |
| [Buffer Pool & WAL](./01-fundamentals/deep-dive-buffer-pool-and-wal.md) | How data moves between disk and memory |
| [Commit Guarantee](./01-fundamentals/deep-dive-commit-guarantee.md) | What "committed" actually means on disk |
| [Isolation Levels](./01-fundamentals/deep-dive-isolation-levels.md) | READ UNCOMMITTED → SERIALIZABLE |
| [Locks & Concurrency](./01-fundamentals/deep-dive-locks-and-concurrency.md) | Shared/exclusive locks, deadlock detection |
| [Replication & Sync](./01-fundamentals/deep-dive-replication-and-sync.md) | How data is kept in sync across nodes |

---

### [Module 02 — ACID Properties](./02-acid/)

Deep dive into the four guarantees every reliable database must provide.

| Article | Topic |
|---------|-------|
| [ACID, Consistency & Durability](./02-acid/acid-consistency-durability.md) | Full ACID breakdown with implementation details |
| [Lock Internals](./02-acid/deep-dive-lock-internals.md) | Row locks, table locks, intent locks, lock escalation |

---

### [Module 03 — Storage Engine Internals](./03-storage-engines/)

How data is physically stored and retrieved from disk.

| Article | Topic |
|---------|-------|
| [Storage Engine Internals](./03-storage-engines/storage-engine-internals.md) | LSM-Tree vs B+ Tree, compaction, page layout |

**Implemented in:** `litedb-python/wal.py`, `litedb-python/memtable.py`, `litedb-python/sstable.py`, `litedb-python/lsm_engine.py`, `litedb-python/btree.py`  
**Java:** `com.litedb.wal.WriteAheadLog`, `com.litedb.memtable.MemTable`, `com.litedb.sstable.*`, `com.litedb.lsm.LSMEngine`, `com.litedb.btree.BTree`

---

### [Module 04 — Indexing](./04-indexing/)

How databases answer queries in O(log n) instead of O(n).

| Article | Topic |
|---------|-------|
| [Indexing Deep Dive](./04-indexing/indexing-deep-dive.md) | B+ Tree, Hash, Bloom Filter, Inverted, Composite indexes |

**Implemented in:** `litedb-python/sstable.py` (Bloom filter), `litedb-python/btree.py` (B+ Tree)  
**Java:** `com.litedb.sstable.BloomFilter`, `com.litedb.btree.BTree`

---

### [Module 05 — Query Processing & Optimization](./05-query-processing/)

How a SQL string becomes an efficient execution plan.

| Article | Topic |
|---------|-------|
| [Query Processing & Optimization](./05-query-processing/query-processing-optimization.md) | Tokenizer → Parser → AST → Planner → Executor |

**Implemented in:** `litedb-python/query_parser.py`, `litedb-python/sql_parser.py`  
**Java:** `com.litedb.query.QueryParser`, `com.litedb.sql.SQLParser`

---

### [Module 06 — MVCC & Concurrency Control](./06-mvcc/)

How databases let readers and writers proceed without blocking each other.

| Article | Topic |
|---------|-------|
| [MVCC & Concurrency Control](./06-mvcc/mvcc-concurrency-control.md) | Versioned writes, snapshot isolation, VACUUM |

**Implemented in:** `litedb-python/transactions.py`  
**Java:** `com.litedb.txn.MVCCStore`

---

### [Module 07 — Distributed Databases & CAP Theorem](./07-distributed-systems/)

What changes when your database spans multiple machines.

| Article | Topic |
|---------|-------|
| [Distributed Databases & CAP Theorem](./07-distributed-systems/distributed-databases-cap-theorem.md) | CAP theorem, eventual consistency, CRDTs, vector clocks |

---

### [Module 08 — Sharding & Partitioning](./08-sharding/)

How to split data across thousands of machines with minimal overhead.

| Article | Topic |
|---------|-------|
| [Sharding & Partitioning](./08-sharding/sharding-partitioning.md) | Range partitioning, hash partitioning, consistent hashing, virtual nodes |

**Implemented in:** `litedb-python/sharding.py`  
**Java:** `com.litedb.sharding.ConsistentHashRing`

---

### [Module 09 — Replication & Consistency Models](./09-replication/)

How copies of data stay in sync — and what happens when they don't.

| Article | Topic |
|---------|-------|
| [Replication & Consistency Models](./09-replication/replication-consistency-models.md) | Sync vs async replication, strong/eventual/causal consistency, Raft |

**Implemented in:** `litedb-python/replication.py`, `litedb-python/raft.py`  
**Java:** `com.litedb.replication.ReplicationLog`, `com.litedb.raft.RaftNode`

---

### [Module 10 — NoSQL Design Patterns](./10-nosql/)

Patterns for designing schemas and access patterns in document, key-value, and column-family stores.

| Article | Topic |
|---------|-------|
| [NoSQL Design Patterns](./10-nosql/nosql-design-patterns.md) | Denormalization, embedding vs referencing, time-series, wide rows |

---

## Concept → Implementation Map

| Concept | Doc | Python (`litedb-python/`) | Java (`com.litedb.*`) |
|---------|-----|--------------------------|----------------------|
| Write-Ahead Log | [Module 01](./01-fundamentals/deep-dive-buffer-pool-and-wal.md) | `wal.py` | `wal.WriteAheadLog` |
| MemTable | [Module 03](./03-storage-engines/storage-engine-internals.md) | `memtable.py` | `memtable.MemTable` |
| SSTable + Bloom Filter | [Module 03](./03-storage-engines/storage-engine-internals.md) | `sstable.py` | `sstable.{SSTableWriter,SSTableReader,BloomFilter}` |
| LSM-Tree + Compaction | [Module 03](./03-storage-engines/storage-engine-internals.md) | `lsm_engine.py` | `lsm.LSMEngine` |
| Command Parser | [Module 05](./05-query-processing/query-processing-optimization.md) | `query_parser.py` | `query.QueryParser` |
| TCP Server | [Module 05](./05-query-processing/query-processing-optimization.md) | `server.py` | `server.LiteDBServer` |
| Async Replication | [Module 09](./09-replication/replication-consistency-models.md) | `replication.py` | `replication.ReplicationLog` |
| MVCC Transactions | [Module 06](./06-mvcc/mvcc-concurrency-control.md) | `transactions.py` | `txn.MVCCStore` |
| B+ Tree | [Module 03](./03-storage-engines/storage-engine-internals.md) | `btree.py` | `btree.BTree` |
| SQL Parser & Executor | [Module 05](./05-query-processing/query-processing-optimization.md) | `sql_parser.py` | `sql.SQLParser` |
| Consistent Hashing | [Module 08](./08-sharding/sharding-partitioning.md) | `sharding.py` | `sharding.ConsistentHashRing` |
| Raft Consensus | [Module 09](./09-replication/replication-consistency-models.md) | `raft.py` | `raft.RaftNode` |
| Auth + RBAC + Pool | — | `auth_pool.py` | `auth.AuthManager` |
| Metrics + Tracing | — | `metrics.py` | `metrics.MetricsRegistry` |
