# LiteDB — Implementation

We are building **LiteDB** — a simple but real database from scratch in Python, covering the same core algorithms used by LevelDB, RocksDB, PostgreSQL, Cassandra, and etcd.

No external dependencies — pure Python stdlib only.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        LiteDB Architecture                        │
│                                                                  │
│  Client (TCP / Python API)                                       │
│      │                                                           │
│      ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Auth & RBAC (auth_pool)   Rate Limiter (auth_pool)     │    │
│  └─────────────────────────────────────────────────────────┘    │
│      │                                                           │
│      ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  SQL Parser & Query Planner (sql_parser)                │    │
│  │  SET/GET/DELETE/SCAN command parser (query_parser)      │    │
│  └─────────────────────────────────────────────────────────┘    │
│      │                                                           │
│      ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Transaction Manager — MVCC (transactions)              │    │
│  │  Snapshot isolation · Write-write conflict · VACUUM     │    │
│  └─────────────────────────────────────────────────────────┘    │
│      │                                                           │
│      ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Storage Engines                                        │    │
│  │  ┌──────────────────┐   ┌──────────────────────────┐   │    │
│  │  │  LSM-Tree        │   │  B+ Tree (btree)         │   │    │
│  │  │  WAL (wal)       │   │  Sorted index pages      │   │    │
│  │  │  MemTable        │   │  O(log n) point lookup   │   │    │
│  │  │  SSTable         │   │  O(k+log n) range scan   │   │    │
│  │  │  Compaction      │   └──────────────────────────┘   │    │
│  │  └──────────────────┘                                   │    │
│  └─────────────────────────────────────────────────────────┘    │
│      │                                                           │
│      ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Distribution Layer                                     │    │
│  │  Sharding — Consistent Hashing (sharding)               │    │
│  │  Replication — Async WAL streaming (replication)        │    │
│  │  Consensus — Raft leader election + log replication     │    │
│  │              (raft)                                     │    │
│  └─────────────────────────────────────────────────────────┘    │
│      │                                                           │
│      ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Observability                                          │    │
│  │  Metrics · Slow Query Log · Distributed Tracing         │    │
│  │  (metrics)   Connection Pool (auth_pool)                │    │
│  └─────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

---

## All Files

### Basic Modules — LSM-Tree Core

| File | Concept | Real-world analogue |
|------|---------|---------------------|
| `wal.py` | Write-Ahead Log — durability before crash | PostgreSQL WAL, MySQL binlog |
| `memtable.py` | In-memory sorted write buffer | LevelDB MemTable |
| `sstable.py` | Immutable sorted file on disk + Bloom filter | RocksDB SST files |
| `lsm_engine.py` | Full LSM-Tree: WAL + MemTable + SSTables + Compaction | LevelDB, RocksDB, Cassandra |
| `query_parser.py` | Parse & execute SET/GET/DELETE/SCAN commands | Redis protocol |
| `server.py` | TCP server — multi-client, pipelined commands | Redis, Memcached |
| `replication.py` | Async WAL streaming to replica node | MySQL replication, Postgres streaming |

### Advanced Modules — Production Features

| File | Concept | Real-world analogue |
|------|---------|---------------------|
| `transactions.py` | MVCC + snapshot isolation + VACUUM | PostgreSQL MVCC |
| `btree.py` | B+ Tree storage engine with range scans | PostgreSQL/MySQL indexes |
| `sql_parser.py` | SQL parser: SELECT/INSERT/UPDATE/DELETE/WHERE/AND/OR/NOT/JOIN | SQLite, DuckDB |
| `sharding.py` | Consistent hashing + virtual nodes + rebalancing | Cassandra, DynamoDB |
| `raft.py` | Raft consensus: leader election + log replication | etcd, CockroachDB, TiKV |
| `auth_pool.py` | PBKDF2 auth + RBAC + connection pool + token bucket rate limiter | PgBouncer, ProxySQL |
| `metrics.py` | Prometheus-style metrics + slow query log + distributed tracing | Prometheus, Jaeger, Datadog |

### Support Files

| File | Purpose |
|------|---------|
| `_loader.py` | Import helper — adds module dir to `sys.path` |
| `client.py` | TCP client for `server.py` — interactive REPL or `--demo` smoke test |
| `run_demo.py` | Comprehensive demo runner — all 14 modules (foundational + advanced) |

---

## Quick Start

```bash
# No installation needed — pure Python 3.10+ stdlib

# Run all 14 modules:
python run_demo.py

# Run a single module by name:
python run_demo.py wal
python run_demo.py btree
python run_demo.py transactions

# Run any module directly:
python transactions.py
python btree.py
```

### Run as a database (server + client)

The **server** (`server.py`) brings up the database; the **client** (`client.py`) connects to it. Data persists across restarts — on startup the engine replays the WAL and loads existing SSTables.

```bash
# Terminal 1 — start the server (persistent; Ctrl-C to stop)
python server.py --port 7379 --data-dir ./data/primary --engine lsm
#   --engine lsm (default) or btree

#   a replica node (replicates from primary):
python server.py --port 7380 --data-dir ./data/replica --replica-of localhost:7379

# Terminal 2 — connect with the interactive client
python client.py --host 127.0.0.1 --port 7379
#   litedb> SET name Alice
#   litedb> GET name
#   litedb> SCAN a z

# Or run the client's scripted smoke test:
python client.py --demo

# Or use any TCP client:
nc localhost 7379
```

Commands: `PING`, `SET k v`, `GET k`, `DELETE k`, `SCAN start end`, `FINDVAL lowVal highVal`, `STATS`, `HELP`, `QUIT`.

### Pluggable storage engine + secondary index

The server runs against a `StorageEngine` (`storage_engine.py`) chosen at startup with `--engine`:

| `--engine` | Module | Model |
|---|---|---|
| `lsm` (default) | `lsm_engine.py` | Write-optimized LSM-Tree (WAL + MemTable + SSTables + compaction) |
| `btree` | `btree_engine.py` | Read-optimized in-memory B+Tree, durable via WAL |

This mirrors how real databases let the workload pick an engine (e.g. MySQL's InnoDB B-tree vs MyRocks LSM).

The **LSM engine** also maintains a **secondary index** (`secondary_index.py`) — a B+Tree mapping stored *values* → primary keys, updated on every write and rebuilt from the data on startup. It powers a reverse lookup that avoids a full scan:

```
FINDVAL <lowValue> <highValue>   # primary keys whose value is in [low, high], served from the index
```

`FINDVAL` is available only on `--engine lsm`; the B+Tree engine reports it as unsupported.

---

## Relational SQL + MVCC transactions

On top of the storage engine sits a small relational database that mirrors the Java implementation:

- **Schema + DDL** — `CREATE TABLE` / `DROP TABLE` (catalog persisted in the store), `CREATE INDEX` / `DROP INDEX`
- **DML** — `INSERT` / `SELECT` / `DELETE` with projection, `WHERE` (incl. `AND`/`OR`/`NOT`), `ORDER BY`, `LIMIT`
- **Multiple secondary indexes** + a **query planner** that uses an index range-scan when the `WHERE` column is indexed, else a full scan (`-- plan:` line shows which)
- **Typed, order-preserving encoding** (`type_codec.py`) so numeric ranges sort correctly (`10 > 9`, negatives)
- **MVCC transactions** (`mvcc.py`) — every statement runs in a transaction: snapshot reads, atomic commit with write-write conflict detection; auto-commit or explicit `BEGIN` / `COMMIT` / `ROLLBACK`; catalog, rows, and index entries are all MVCC-versioned

```bash
python relational_engine.py   # SQL through MVCC + cross-session snapshot isolation
python mvcc_demo.py            # snapshot isolation, conflict detection, tombstones, GC
python atomicity_demo.py       # all-or-nothing write batches
```

| Module | Role |
|--------|------|
| `relational_engine.py` | SQL layer (DDL/DML, planner) over MVCC |
| `catalog.py`, `table_schema.py`, `column.py`, `index_def.py`, `row_codec.py` | schema/catalog + row encoding |
| `type_codec.py` | order-preserving typed-value encoding |
| `mvcc.py` | MVCC engine + transactions (snapshot isolation, OCC, GC) |

---

## Distributed cluster (multiple instances on one machine)

`dashboard.py` brings up **three database instances** (separate OS processes) that coordinate over
Raft to form one distributed database — **partitioned, replicated, and transactional** — with a
live web UI that narrates what each instance is doing.

```bash
python dashboard.py                    # then open http://127.0.0.1:7080
JARVIS_CLUSTER_RF=2 python dashboard.py # 3 instances, replication factor 2
```

The dashboard shows the whole system centrally: health badge, configuration, the **consistent-hash
ring** (which shard owns which arc of the keyspace), the **shard → node placement matrix**
(leader / follower / not-hosted, live), one **event feed per instance**, and a **merged system
stream**. The same cluster is implemented in Java (`com.litedb.cluster.Dashboard`).

End to end, it demonstrates:

- **Partitioning** — keys spread across 6 shards by consistent hashing (`partition.py`)
- **Multi-raft** — each shard is its *own* Raft group (`raft_node.py`); an instance is leader of
  some shards and follower of others, so leadership and write load spread across the cluster
  (vs. a single cluster-wide Raft leader that would bottleneck every write)
- **Replication** — each write goes through the shard leader's Raft log and is replicated +
  `fsync`'d on a majority before commit; replicas apply identical versioned writes and converge
  byte-for-byte
- **Routing** — a client can hit *any* instance; it routes the op to the shard's leader (or forwards)
- **Distributed transactions** — a write spanning shards runs **two-phase commit** across the
  shard leaders, with **HLC** timestamps (`hlc.py`) for snapshot isolation
- **Failover** — kill an instance from the UI and watch its shards re-elect leaders on the
  survivors, data intact (persisted Raft logs replay on restart)

Each instance has its own dashboard panel streaming its reasoning — election timeouts, accepting a
leader, routing by consistent hashing, replicating, applying, running 2PC — so you can *watch* the
distributed logic instead of reading about it.

| Module | Role |
|--------|------|
| `rpc.py` | length-framed JSON-over-TCP RPC (Raft, client, 2PC) |
| `raft_node.py` | one Raft replica of one shard (election, replication, persistent log) |
| `shard_store.py` / `shard_replica.py` | per-shard MVCC state machine + leader-side commit |
| `partition.py` | key → shard (consistent hashing) and shard → replica nodes |
| `hlc.py` | hybrid logical clock for distributed snapshot timestamps |
| `node.py` | one instance: hosts every shard, routes requests, coordinates 2PC |
| `cluster_client.py` | contact-any-node client |
| `events.py` / `dashboard.py` | per-instance event log + launcher + live dashboard |

```bash
pytest test_distributed.py     # fast in-process tests (RPC, Raft, replicated MVCC)
python cluster_smoke.py        # 3 real processes, full scenario, headless
```

**Scope (honest):** this runs many instances on **one machine**. It is a faithful integration of
the distributed algorithms — real RPC, real Raft, real partitioning, real 2PC, real failover — but
it is **not** hardened for the cross-machine failure matrix: no Raft membership changes, snapshot
install is log-based, 2PC blocks if a coordinator dies mid-commit, and it is not Jepsen-tested. See
[../ROADMAP.md](../ROADMAP.md) for exactly what remains.

---

## Concepts Demonstrated

### Storage
1. **WAL** — every write goes to WAL first; on crash, replay WAL to recover
2. **MemTable** — writes buffer in memory (sorted); reads check here first
3. **SSTable** — when MemTable is full, flush to immutable sorted file on disk
4. **Bloom Filter** — probabilistic check before reading SSTable (avoid disk I/O)
5. **Compaction** — merge multiple SSTables, remove deleted keys (tombstones)
6. **LSM-Tree** — the full algorithm combining WAL + MemTable + SSTable + Compaction
7. **B+ Tree** — all data in leaves, linked for range scans, O(log n) all operations

### Transactions
8. **MVCC** — each write creates a new version; readers see a consistent snapshot
9. **Snapshot Isolation** — transactions read from a point-in-time snapshot
10. **Write-Write Conflict** — first writer wins; second writer is aborted
11. **VACUUM** — garbage-collect old versions no active transaction can see

### Query Processing
12. **SQL Parser** — tokenizer → AST → executor pipeline
13. **Query Planner** — full-scan vs index-scan decision
14. **Execution Engine** — filter, project, aggregate, join

### Distribution
15. **Consistent Hashing** — add/remove nodes with minimal key movement
16. **Virtual Nodes** — even load distribution across physical nodes
17. **Async Replication** — stream WAL entries to replicas
18. **Raft Consensus** — leader election + log replication for strong consistency

### Operations
19. **RBAC** — roles grant permissions; users get roles
20. **PBKDF2 Auth** — 260K-iteration password hashing; constant-time comparison
21. **Connection Pool** — reuse expensive TCP connections; min/max size; health checks
22. **Token Bucket** — rate limiting with burst allowance
23. **Prometheus Metrics** — counters, gauges, histograms with labels
24. **Slow Query Log** — capture queries exceeding latency threshold
25. **Distributed Tracing** — trace spans across components with parent/child relationships

---

## Key Insights by Module

### `transactions.py` — MVCC
- Each write creates a new **version** tagged with `txid`
- Readers see a **snapshot** — never blocked by writers
- Write-write conflicts detected at commit (first writer wins)
- VACUUM removes old versions no active transaction can see
- This is exactly how **PostgreSQL MVCC** works

### `btree.py` — B+ Tree
- B+ Tree keeps all data in **leaves**; internal nodes are just routing guides
- Leaf nodes are **linked** → O(k + log n) range scans
- All leaves at same depth → guaranteed **O(log n)** operations
- Node splits propagate upward → tree grows from root
- In production: each node = one disk page (4 KB / 8 KB)

### `sql_parser.py` — SQL Engine
- Tokenizer → Parser → AST → Executor pipeline
- WHERE clause supports AND, OR, NOT, =, !=, <, >, <=, >=
- Query planner chooses full-scan vs index-scan
- Aggregates: COUNT, SUM, AVG, MIN, MAX
- JOIN: nested-loop join (upgradeable to hash join)

### `sharding.py` — Consistent Hashing
- Hash ring with 2³² positions; nodes placed at multiple virtual positions
- Adding/removing a node moves only `1/n` of keys (vs 100% in naive modulo)
- Virtual nodes (vnodes) ensure even distribution even with heterogeneous hardware
- Replication factor R: each key stored on R consecutive nodes

### `raft.py` — Consensus
- Leader elected by majority vote; term number prevents split-brain
- All writes go through leader → replicated to followers → committed when majority ACK
- Log entries are committed only after majority acknowledgement
- Leader sends heartbeats to prevent unnecessary elections

### `auth_pool.py` — Auth & Pooling
- PBKDF2 with 260K iterations makes brute-force impractical (~1 hash/sec on GPU)
- Constant-time comparison (`hmac.compare_digest`) prevents timing attacks
- Account lockout after 5 failed attempts prevents online brute-force
- Connection pool: reuse expensive connections (saves ~10 ms/query)
- Token bucket: allows bursts while enforcing average rate

### `metrics.py` — Observability
- Counters (monotonically increasing), Gauges (current value), Histograms (latency distribution)
- Slow query log captures queries above a configurable threshold
- Distributed trace: each operation gets a `trace_id`; child spans record parent
- Prometheus text format: `metric_name{label="value"} value timestamp`

---

## Expected Output

```
python run_demo.py
→ 13 passed, 0 failed
```