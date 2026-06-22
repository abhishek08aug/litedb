# Roadmap: from a single-node engine to a distributed database

litedb today is a **correct single-node** transactional engine **and** an **integrated, single-machine
distributed database** — multiple instances (separate processes) that partition data into shards,
replicate each shard through its own Raft group, route requests to shard leaders, and commit
cross-shard transactions via 2PC (see `litedb-python/dashboard.py` and the cluster modules). This
document is an honest map of what is built versus the distance still remaining to a *cross-machine,
production-grade* database.

The mental model is the CockroachDB / TiKV split. The single-machine integration is real (real RPC,
real Raft, real partitioning, real 2PC, real failover); what remains is the **cross-machine failure
matrix and operational hardening** — the part that genuinely takes years.

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

### Single-machine distributed cluster (integrated, built — **Python and Java at parity**)
Both `litedb-python/` and `litedb-java/` (`com.litedb.cluster`) implement the full system:
- **Real RPC transport** — length-framed JSON over TCP; Raft, client, and 2PC ride it
- **Multi-process Raft** — election, log replication, safety, **persistent** term/vote/log with
  fsync, restart-recovery; one Raft group **per shard** (multi-raft)
- **Raft drives the real engine** — committed entries apply to a per-shard LSM/MVCC store
  deterministically, so replicas converge byte-for-byte
- **Consistent-hash partitioning** — keys → shards, shards → replica nodes, leadership spread
- **Configurable replication factor** — `JARVIS_CLUSTER_RF` (e.g. RF 2 on 3 nodes); a node may
  not host a given shard, so routing resolves the leader via the shard's replica nodes
- **RF-agnostic routing** — hit any instance; it forwards to the shard's leader, even across nodes
  that don't host the shard
- **Cross-shard 2PC** + **HLC** — atomic multi-shard writes with snapshot isolation
- **Live failover** (RF ≥ 3) — kill an instance, its shards re-elect on survivors, data intact
- **Rich central dashboard** — health, config, consistent-hash ring, shard→node placement matrix,
  one event feed per instance + a merged system stream; kill/restart nodes from the UI

The remaining gap is no longer *integration* — it is **cross-machine hardening**.

---

## Known simplifications (deliberate, single-machine scope)

These are conscious shortcuts in the current build — each works for the demo and each is a real,
named gap for production. Listed so the boundary is explicit, not hidden.

- **Placement is static config, not a dynamic metadata service.** RF < node-count works (a node
  that doesn't host a shard resolves the leader via the shard's replica nodes and forwards), but
  the placement itself is fixed shared config. Real scale needs a **placement/metadata service**
  (à la TiKV's Placement Driver) that tracks live shard→node assignment as it changes.
- **2PC recovery covers coordinator AND participant failure (single-machine).** The coordinator
  persists a transaction record (`txn_log.py` / `TxnLog.java`) and a periodic sweep re-drives
  in-doubt transactions to completion — `committing` → re-send COMMIT, `aborted` → re-send ABORT,
  stale `preparing` → ABORT — so a coordinator can die mid-2PC and recover (`recovery_smoke.py` /
  `ClusterRecoverySmoke`). On the participant side, PREPARE/COMMIT/ABORT are **replicated through
  each shard's Raft log as intents** (Percolator/CockroachDB model), so a prepared intent survives a
  participant leader crash — a new leader inherits it, preserving isolation and allowing commit
  (`participant_recovery_smoke.py` / `ClusterParticipantRecoverySmoke`). A newly-elected leader
  commits a no-op and is **not "ready"** to serve conflict-checked writes until it applies that
  no-op, guaranteeing it has applied all inherited intents. What remains for *production* is the
  hard part: **parallel-commit** to cut 2PC latency, and **Jepsen-grade** validation under
  partitions — the correctness here is shown by scripted scenarios, not proven adversarially.
- **HLC assumes roughly-synced clocks.** On one machine all processes read the same clock, so
  snapshot ordering is exact. Across machines you need NTP plus **uncertainty bounds** (Spanner's
  commit-wait / TrueTime) to keep snapshot isolation correct under skew.
- **Fixed shards, no range split/merge.** The shard set is static; there is no rebalancing as data
  grows or nodes join/leave.
- **No Raft membership changes.** The cluster roster is static config; no joint-consensus add/remove.
- **Snapshot install is log-based.** A far-behind replica catches up by log replication, not by
  shipping a compacted snapshot — fine for small logs, not for large state.
- **Static service discovery.** Node addresses come from shared config; no gossip / failure
  detector across machines.
- **Not adversarially tested.** No Jepsen / partition / fault-injection suite; correctness is shown
  by scripted scenarios, not proven under the full failure matrix.

---

## The ladder to distributed, production-grade

Ordered roughly by dependency. Each tier assumes the ones above it.
Legend: **[x]** = built (single-machine); **[ ]** = remaining.

### 1. Replication & consensus
- [x] Real network RPC transport for Raft
- [x] Raft drives the **real** LSM/MVCC state machine (not a private log)
- [x] Per-shard Raft replication with persistent log + restart-recovery
- [ ] Sync / quorum replication tuning and semi-sync modes (commit is majority today)
- [ ] Follower reads (lease / closed-timestamp) for read scaling
- [ ] Raft log compaction + snapshot install for slow/recovering followers (catchup is log-based)
- [ ] Pre-vote / leader leases to avoid disruptive elections under partition

### 2. Sharding & data placement
- [x] Consistent-hash partitioning of keys into shards
- [x] Each shard backed by a real LSM/MVCC engine
- [x] One Raft group **per shard** (multi-raft), leadership spread across nodes
- [x] Route live KV operations to the owning shard's leader
- [ ] Range split/merge as data grows/shrinks (shards are fixed today)
- [ ] A placement driver / balancer: capacity- and load-aware placement; hot-shard movement

### 3. Distributed transactions & time
- [x] Cross-shard atomic commit via 2PC
- [x] Distributed clock (HLC) for snapshot timestamps across shards
- [x] Snapshot isolation across shards
- [x] Coordinator-failure recovery (durable txn record + restart sweep finishes in-doubt 2PC)
- [x] Participant-failure recovery: PREPARE intents replicated through Raft survive a participant
      leader crash (a new leader inherits them); readiness gate ensures no write bypasses an
      inherited intent
- [ ] Parallel-commit (cut 2PC latency, replace blocking 2PC); Jepsen-grade adversarial testing
- [ ] Distributed deadlock detection / finer-grained latching (per-shard lock today)

### 4. Cluster lifecycle & membership
- [x] Automated failover end-to-end (Raft election → client-visible leader change)
- [ ] Gossip / membership, heartbeats, failure detection across machines
- [ ] Raft membership changes: node add / drain / decommission; rebalancing on topology change
- [ ] Online schema changes / migrations propagated cluster-wide; catalog consistency

### 5. Routing & client layer
- [x] Routing/coordinator aware of shard topology and leaders (any node can serve)
- [x] Contact-any-node client with retry + connection pooling
- [ ] Leader-aware client caching, backpressure; follower-read load balancing

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

The single-machine cluster proves the architecture end to end: partitioning, multi-raft
replication, routing, cross-shard 2PC, and failover all work with real RPC between real processes.
What remains is not "invent Raft" or "wire it together" — it is the **cross-machine failure matrix
and operational hardening**: membership changes, snapshot install, parallel-commit, backups,
observability, and Jepsen-grade correctness testing. That is the part that genuinely takes
a team years, and it is deliberately out of scope here. Scoping that boundary precisely — and
demonstrating everything up to it — is the point of this document.
