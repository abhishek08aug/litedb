# Changelog

All notable changes to LiteDB are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).  
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

---

## [1.0.0] — 2026-05-02

### Added

#### Basic Modules (run_demo.py — 7 modules)
- `wal.py` — Write-Ahead Log with crash recovery and replay
- `memtable.py` — In-memory sorted write buffer
- `sstable.py` — Immutable sorted disk file with Bloom filter
- `lsm_engine.py` — Full LSM-Tree: WAL + MemTable + SSTable + Compaction
- `query_parser.py` — SET / GET / DELETE / SCAN command parser and executor
- `server.py` — Multi-client TCP server with pipelined protocol
- `replication.py` — Async WAL streaming to replica node

#### Advanced Modules (run_demo.py — 7 modules)
- `transactions.py` — MVCC with snapshot isolation, write-write conflict detection, and VACUUM
- `btree.py` — B+ Tree storage engine with linked leaves and range scans
- `sql_parser.py` — SQL parser and executor: SELECT / INSERT / UPDATE / DELETE / WHERE / AND / OR / NOT / JOIN / aggregates
- `sharding.py` — Consistent hashing with virtual nodes and rebalancing
- `raft.py` — Raft consensus: leader election and log replication
- `auth_pool.py` — PBKDF2 authentication, RBAC, connection pool, token bucket rate limiter
- `metrics.py` — Prometheus-style metrics, slow query log, distributed tracing

#### Documentation
- `docs/01-fundamentals/` — 7 deep-dive articles on database fundamentals
- `docs/02-acid/` — ACID properties and lock internals
- `docs/03-storage-engines/` — LSM-Tree and B+ Tree internals
- `docs/04-indexing/` — Index types and implementation
- `docs/05-query-processing/` — Query parsing, planning, and optimization
- `docs/06-mvcc/` — MVCC and concurrency control
- `docs/07-distributed-systems/` — CAP theorem and distributed databases
- `docs/08-sharding/` — Sharding and partitioning strategies
- `docs/09-replication/` — Replication and consistency models
- `docs/10-nosql/` — NoSQL design patterns

#### Repo infrastructure
- `LICENSE` — MIT
- `CONTRIBUTING.md` — contribution guide
- `.gitignore` — Python + LiteDB runtime artifacts
- `.github/ISSUE_TEMPLATE/` — bug report and feature request templates
- `.github/PULL_REQUEST_TEMPLATE.md` — PR checklist