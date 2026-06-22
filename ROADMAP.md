# Roadmap: from a single-node engine to a distributed database

litedb today is a **correct single-node** transactional engine, **plus standalone, working
implementations of the core distributed algorithms** (replication, consensus, sharding) that
are not yet wired under the live transactional path. This document is an honest map of the
distance to a *distributed, production-grade* database — both to scope the project accurately
and to record the design ladder.

The mental model is the CockroachDB / TiKV split: litedb's transactional engine is roughly
**one storage node**. The distributed algorithms exist beside it as demonstrations; the work
is to put the transactional engine *on top of* them and then harden the result.

---

## Where it is today

### Single-node transactional engine (live path, done)
- Pluggable storage engines: LSM-tree (WAL, MemTable, SSTable, leveled compaction) and B+Tree
- WAL durability with fsync-per-append and crash recovery
- Atomic multi-key write batches (all-or-nothing, batch-aware recovery)
- Relational SQL: DDL/DML, multiple secondary indexes, a query planner (index vs full scan),
  order-preserving typed encoding
- **MVCC** on the live path: snapshot isolation, OCC write-write conflict detection,
  tombstones, vacuum/GC, an in-process timestamp oracle
- Java and Python implementations at feature parity

### Distributed algorithms (standalone, correct, NOT yet wired into the live path)
- **Async WAL replication** (`replication.py`) — real primary→replica streaming over **TCP**;
  publisher tails real WAL entries, subscriber applies them and tracks an offset.
  *Async only — no quorum/sync acks.*
- **Raft consensus** (`raft.py`, Java `RaftNode`) — leader election, log replication, terms,
  commit-index advancement, safety. *Runs as an in-process cluster with its own log; does not
  drive the LSM/MVCC storage engine.*
- **Consistent-hashing sharding** (`sharding.py`) — hash ring, virtual nodes, replication
  factor, ~1/N rebalancing on node add/remove. *Backs in-process dicts, not real engines, and
  does not route live SQL.*

The key gap is **integration**: the relational/MVCC engine imports none of these. They prove
the algorithms; they don't yet form one distributed database.

---

## The ladder to distributed, production-grade

Ordered roughly by dependency. Each tier assumes the ones above it.
Legend: **[demo]** = standalone implementation exists; **[ ]** = not started.

### 1. Replication & consensus — integrate and harden
- [demo] Async WAL replication over TCP — exists standalone
- [demo] Raft (election, log replication, safety) — exists standalone, own log
- [ ] Drive Raft's state machine from the **real** LSM/MVCC engine (not a private log)
- [ ] Replicate the **WAL** through Raft so every replica is a consistent transactional copy
- [ ] Sync / quorum replication (wait for N acks) and semi-sync modes
- [ ] Follower reads (lease / closed-timestamp) for read scaling
- [ ] Raft log compaction + snapshot install for slow/recovering followers
- [ ] Real network RPC transport for Raft (replace in-process peer calls)

### 2. Sharding & data placement — integrate and harden
- [demo] Consistent-hashing ring with vnodes + rebalancing — exists standalone
- [ ] Back each shard with a real LSM/MVCC engine (not an in-process dict)
- [ ] Route live SQL/KV operations through the ring to the owning node
- [ ] One Raft group **per shard** (multi-raft), not one global log
- [ ] Range split/merge as data grows/shrinks (consistent hashing alone won't do ranges)
- [ ] A placement driver / balancer: capacity- and load-aware placement; hot-shard movement

### 3. Distributed transactions & time (no implementation yet — the hardest tier)
- [ ] Cross-shard atomic commit: 2PC or parallel-commit
- [ ] Distributed clock: HLC, a TSO service, or TrueTime-style bounded uncertainty
      (the current oracle is single-process and does not generalize)
- [ ] Distributed snapshot isolation / serializability across shards
- [ ] Distributed deadlock detection and lock/latch management

### 4. Cluster lifecycle & membership
- [ ] Gossip / membership, heartbeats, failure detection
- [ ] Automated failover end-to-end (Raft election wired to client-visible leader change)
- [ ] Node add / drain / decommission; rebalancing on topology change
- [ ] Online schema changes / migrations propagated cluster-wide; catalog consistency

### 5. Routing & client layer
- [ ] Request router/coordinator aware of shard topology and leaders
- [ ] Leader-aware smart client: retries, connection pooling, backpressure
- [ ] Load balancing across replicas; follower-read routing

### 6. Durability & data integrity (hardening)
- [ ] Checksums/CRC on WAL and SSTable blocks (silent-corruption detection)
- [ ] Backups + point-in-time recovery; consistent cluster snapshots/restore
- [ ] Bloom filters and a block cache on the read path
- [ ] Compaction tuning: write-stall control, space/write amplification management

### 7. Concurrency & throughput
- [ ] Remove the global-lock commit bottleneck
- [ ] Group commit, pipelined WAL, async replication flow control

### 8. Observability, security & multi-tenancy
- [ ] Metrics, distributed tracing, structured logs, SLOs
- [ ] TLS, authN/authZ, rate limiting, quotas, tenant isolation

### 9. Correctness assurance
- [ ] Jepsen-style fault-injection and partition testing
- [ ] Fuzz / property-based tests; chaos testing
- [ ] A formal model of the isolation level (e.g. TLA+)

### 10. Multi-region (if going global)
- [ ] Cross-region replication and locality-aware placement
- [ ] Geo-partitioning; region-pinned data for latency/compliance

---

## What this list is for

The distributed algorithms are implemented and demonstrably correct in isolation. The distance
to production is **not** "invent Raft" — it is (a) wiring those algorithms under the live
transactional engine, (b) upgrading them (quorum replication, multi-raft per shard, real RPC),
and (c) building the genuinely-absent layer: distributed transactions, a distributed clock, and
the lifecycle/hardening tiers. That third part is where the real distributed-systems difficulty
lives. Scoping that boundary precisely is the point of this document.
