# LiteDB

> **A database engine built from first principles — in Python and Java.**

LiteDB is a fully working key-value and SQL database implementing the core algorithms behind PostgreSQL, Cassandra, etcd, and RocksDB — storage, transactions, query processing, replication, consensus, and observability — with zero external dependencies. Each subsystem is documented end-to-end (concept → implementation).

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Java](https://img.shields.io/badge/java-11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Tests](https://img.shields.io/badge/tests-passed-brightgreen)
![Dependencies](https://img.shields.io/badge/dependencies-none-lightgrey)

---

## What's implemented

| Layer | What | Real-world analogue |
|-------|------|---------------------|
| Storage | WAL, MemTable, SSTable, LSM-Tree, B+ Tree | LevelDB, RocksDB, PostgreSQL |
| Transactions | MVCC, snapshot isolation, VACUUM | PostgreSQL, MySQL InnoDB |
| Query | SQL parser, query planner, executor | SQLite, DuckDB |
| Distribution | Consistent hashing, async replication, Raft | Cassandra, etcd, CockroachDB |
| Operations | PBKDF2 auth, RBAC, connection pool, rate limiter | PgBouncer, ProxySQL |
| Observability | Prometheus metrics, slow query log, distributed tracing | Prometheus, Jaeger |

---

## Quick start

### Python

```bash
git clone https://github.com/abhishek08aug/litedb.git
cd litedb/litedb-python

# All 14 modules — WAL → MemTable → SSTable → LSM-Tree → Parser → Replication
#                   → Transactions → B-Tree → SQL → Sharding → Raft → Auth → Metrics
python run_demo.py
```

Expected output:

```
Results: 13 passed, 0 failed
```

Run a single module directly:

```bash
python run_demo.py wal          # wal.py
python run_demo.py btree        # btree.py
python run_demo.py raft         # raft.py
python transactions.py          # standalone
```

Run the TCP server:

```bash
# Primary on port 7379
python server.py --port 7379 --data-dir ./data/primary

# Replica on port 7380 (streams WAL from primary)
python server.py --port 7380 --data-dir ./data/replica --replica-of localhost:7379

# Connect with netcat
nc localhost 7379
SET name Alice
GET name
SCAN a z
DELETE name
```

### Java

```bash
cd litedb/litedb-java

# Compile (no Maven required)
javac --release 11 -d target/classes \
  $(find src/main/java -name "*.java" | sort)

# Run the full integration demo (all 13 modules)
java -cp target/classes com.litedb.demo.RunDemo

# Run a single module
java -cp target/classes com.litedb.btree.BPlusTree
java -cp target/classes com.litedb.raft.RaftNode
```

Expected output:

```
=== COMPILE OK ===
  LiteDB — Full Integration Demo
...
  Total: 13 core database subsystems, ~3000 lines of Java.
[LiteDB Demo Complete]
```

---

## Repository layout

```
litedb/                          ← repo root
├── README.md                    ← you are here
├── LICENSE                      ← MIT
├── CONTRIBUTING.md
├── CHANGELOG.md
├── .gitignore
│
├── .github/
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.md
│   │   └── feature_request.md
│   └── PULL_REQUEST_TEMPLATE.md
│
├── docs/                        ← theory curriculum (10 modules, 18 articles)
│   ├── README.md                ← docs index & concept→implementation map
│   ├── 01-fundamentals/         ← what is a database, architecture, WAL
│   ├── 02-acid/                 ← ACID properties, lock internals
│   ├── 03-storage-engines/      ← LSM-Tree, B+ Tree, compaction
│   ├── 04-indexing/             ← B+ Tree, Hash, Bloom Filter, Inverted
│   ├── 05-query-processing/     ← tokenizer → AST → planner → executor
│   ├── 06-mvcc/                 ← MVCC, snapshot isolation, VACUUM
│   ├── 07-distributed-systems/  ← CAP theorem, eventual consistency, CRDTs
│   ├── 08-sharding/             ← consistent hashing, virtual nodes
│   ├── 09-replication/          ← sync/async replication, Raft
│   └── 10-nosql/                ← NoSQL design patterns
│
├── litedb-python/               ← Python implementation (pure stdlib)
│   ├── wal.py                    Basic: Write-Ahead Log
│   ├── memtable.py               Basic: MemTable
│   ├── sstable.py                Basic: SSTable + Bloom Filter
│   ├── lsm_engine.py             Basic: Full LSM-Tree + Compaction
│   ├── query_parser.py           Basic: SET/GET/DELETE/SCAN parser
│   ├── server.py                 Basic: Multi-client TCP server
│   ├── replication.py            Basic: Async WAL streaming
│   ├── transactions.py           Advanced: MVCC + snapshot isolation
│   ├── btree.py                  Advanced: B+ Tree storage engine
│   ├── sql_parser.py             Advanced: SQL parser & executor
│   ├── sharding.py               Advanced: Consistent hashing + vnodes
│   ├── raft.py                   Advanced: Raft consensus
│   ├── auth_pool.py              Advanced: Auth + RBAC + connection pool
│   └── metrics.py                Advanced: Metrics + tracing + slow log
│
└── litedb-java/                 ← Java implementation (complete — 13 modules, ~3000 lines)
```

---

## Implementation modules

### Basic — `run_demo.py`

| File | Concept | Key algorithm |
|------|---------|---------------|
| `wal.py` | Write-Ahead Log | Append-only log; replay on crash |
| `memtable.py` | MemTable | Sorted dict; O(log n) writes |
| `sstable.py` | SSTable + Bloom Filter | Binary search; probabilistic membership |
| `lsm_engine.py` | LSM-Tree | WAL + MemTable + SSTable + Compaction |
| `query_parser.py` | Command parser | Tokenize → parse → execute |
| `server.py` | TCP server | Multi-client; pipelined protocol |
| `replication.py` | Async replication | WAL streaming; primary/replica |

### Advanced — run via `run_demo.py`

| File | Concept | Key algorithm |
|------|---------|---------------|
| `transactions.py` | MVCC | Versioned writes; snapshot isolation; VACUUM |
| `btree.py` | B+ Tree | Sorted pages; node splits; linked leaves |
| `sql_parser.py` | SQL engine | Tokenizer → AST → planner → executor |
| `sharding.py` | Consistent hashing | Hash ring; virtual nodes; rebalancing |
| `raft.py` | Raft consensus | Leader election; log replication; majority commit |
| `auth_pool.py` | Auth + pooling | PBKDF2; RBAC; pool; token bucket |
| `metrics.py` | Observability | Counters/gauges/histograms; slow log; tracing |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        LiteDB Architecture                        │
│                                                                  │
│  Client (TCP / Python API)                                       │
│      │                                                           │
│      ▼                                                           │
│  Auth & RBAC · Rate Limiter                    (auth_pool)       │
│      │                                                           │
│      ▼                                                           │
│  SQL Parser & Query Planner · Command Parser   (sql_parser,      │
│                                                 query_parser)    │
│      │                                                           │
│      ▼                                                           │
│  Transaction Manager — MVCC                    (transactions)    │
│  Snapshot isolation · Write-write conflict · VACUUM              │
│      │                                                           │
│      ▼                                                           │
│  ┌──────────────────────┐   ┌──────────────────────────────┐    │
│  │  LSM-Tree            │   │  B+ Tree (btree)             │    │
│  │  wal → memtable      │   │  Sorted pages · O(log n)     │    │
│  │  → sstable → compact │   │  Range scans via linked leaves│   │
│  └──────────────────────┘   └──────────────────────────────┘    │
│      │                                                           │
│      ▼                                                           │
│  Sharding — Consistent Hashing + Virtual Nodes (sharding)        │
│  Replication — Async WAL Streaming             (replication)     │
│  Consensus — Raft Leader Election + Log Repl.  (raft)            │
│      │                                                           │
│      ▼                                                           │
│  Prometheus Metrics · Slow Query Log · Tracing (metrics)         │
│  Connection Pool                               (auth_pool)       │
└──────────────────────────────────────────────────────────────────┘
```

---

## Theory curriculum

The [`docs/`](./docs/) directory contains 10 modules and 18 deep-dive articles covering every concept implemented in the code:

- **[Module 01 — Fundamentals](./docs/01-fundamentals/)** — architecture, WAL, buffer pool, isolation levels, locks
- **[Module 02 — ACID](./docs/02-acid/)** — atomicity, consistency, durability, lock internals
- **[Module 03 — Storage Engines](./docs/03-storage-engines/)** — LSM-Tree, B+ Tree, compaction, page layout
- **[Module 04 — Indexing](./docs/04-indexing/)** — B+ Tree, Hash, Bloom Filter, Inverted, Composite
- **[Module 05 — Query Processing](./docs/05-query-processing/)** — tokenizer, AST, planner, optimizer, executor
- **[Module 06 — MVCC](./docs/06-mvcc/)** — versioned writes, snapshot isolation, VACUUM
- **[Module 07 — Distributed Systems](./docs/07-distributed-systems/)** — CAP theorem, eventual consistency, CRDTs
- **[Module 08 — Sharding](./docs/08-sharding/)** — range/hash partitioning, consistent hashing, virtual nodes
- **[Module 09 — Replication](./docs/09-replication/)** — sync/async replication, consistency models, Raft
- **[Module 10 — NoSQL Patterns](./docs/10-nosql/)** — denormalization, embedding, time-series, wide rows
- **[Module 11 — Security: Auth, RBAC & Pooling](./docs/11-security/)** — PBKDF2 auth, RBAC, connection pooling, rate limiting
- **[Module 12 — Metrics & Observability](./docs/12-observability/)** — counters/gauges/histograms, percentiles, slow query log, tracing

→ **[Full docs index](./docs/README.md)**

---

## Concept → implementation map

| Concept | Doc | Python | Java |
|---------|-----|--------|------|
| Write-Ahead Log | [Module 01](./docs/01-fundamentals/deep-dive-buffer-pool-and-wal.md) | `litedb-python/wal.py` | `com.litedb.wal.WriteAheadLog` |
| MemTable | [Module 03](./docs/03-storage-engines/storage-engine-internals.md) | `litedb-python/memtable.py` | `com.litedb.memtable.MemTable` |
| SSTable + Bloom Filter | [Module 03](./docs/03-storage-engines/storage-engine-internals.md) | `litedb-python/sstable.py` | `com.litedb.sstable.*` |
| LSM-Tree + Compaction | [Module 03](./docs/03-storage-engines/storage-engine-internals.md) | `litedb-python/lsm_engine.py` | `com.litedb.lsm.LSMEngine` |
| Command Parser | [Module 05](./docs/05-query-processing/query-processing-optimization.md) | `litedb-python/query_parser.py` | `com.litedb.query.QueryParser` |
| TCP Server | [Module 05](./docs/05-query-processing/query-processing-optimization.md) | `litedb-python/server.py` | `com.litedb.server.LiteDBServer` |
| Async Replication | [Module 09](./docs/09-replication/replication-consistency-models.md) | `litedb-python/replication.py` | `com.litedb.replication.ReplicationLog` |
| MVCC Transactions | [Module 06](./docs/06-mvcc/mvcc-concurrency-control.md) | `litedb-python/transactions.py` | `com.litedb.txn.MVCCStore` |
| B+ Tree | [Module 03](./docs/03-storage-engines/storage-engine-internals.md) | `litedb-python/btree.py` | `com.litedb.btree.BPlusTree` |
| SQL Parser & Executor | [Module 05](./docs/05-query-processing/query-processing-optimization.md) | `litedb-python/sql_parser.py` | `com.litedb.sql.SQLParser` |
| Consistent Hashing | [Module 08](./docs/08-sharding/sharding-partitioning.md) | `litedb-python/sharding.py` | `com.litedb.sharding.ConsistentHashRing` |
| Raft Consensus | [Module 09](./docs/09-replication/replication-consistency-models.md) | `litedb-python/raft.py` | `com.litedb.raft.RaftNode` |
| Auth + RBAC + Pool | [Module 11](./docs/11-security/auth-rbac-and-pooling.md) | `litedb-python/auth_pool.py` | `com.litedb.auth.AuthManager` |
| Metrics + Tracing | [Module 12](./docs/12-observability/metrics-and-observability.md) | `litedb-python/metrics.py` | `com.litedb.metrics.MetricsRegistry` |

---

## Key concepts implemented

| Concept | Where it's used in production |
|---------|-------------------------------|
| Write-Ahead Log | PostgreSQL, MySQL, SQLite |
| LSM-Tree + Compaction | LevelDB, RocksDB, Cassandra, HBase |
| Bloom Filter | RocksDB, Cassandra, BigTable |
| B+ Tree | PostgreSQL, MySQL InnoDB, Oracle |
| MVCC | PostgreSQL, MySQL InnoDB, CockroachDB |
| Consistent Hashing | Cassandra, DynamoDB, Chord DHT |
| Raft Consensus | etcd, CockroachDB, TiKV, Consul |
| PBKDF2 Auth | Django, PostgreSQL, bcrypt family |
| Token Bucket | AWS API Gateway, Nginx, Redis |
| Prometheus Metrics | Kubernetes, Grafana stack |
| Distributed Tracing | Jaeger, Zipkin, Datadog APM |

---

## Requirements

- Python 3.10 or later
- No external packages — pure stdlib only

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). All contributions welcome — bug fixes, new modules, documentation improvements, and tests.

---

## License

[MIT](./LICENSE)