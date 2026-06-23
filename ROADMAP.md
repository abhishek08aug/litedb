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
- **Configurable replication factor** — `LITEDB_CLUSTER_RF` (e.g. RF 2 on 3 nodes); a node may
  not host a given shard, so routing resolves the leader via the shard's replica nodes
- **RF-agnostic routing** — hit any instance; it forwards to the shard's leader, even across nodes
  that don't host the shard
- **Cross-shard 2PC** + **HLC** — atomic multi-shard writes with snapshot isolation
- **2PC failure recovery** — coordinator-crash (durable txn record + a sweep that re-drives
  in-doubt transactions) *and* participant-crash (PREPARE intents replicated through Raft survive a
  leader crash; a new leader inherits them); a leader serves conflict-checked writes only after
  applying its election no-op, so no write can bypass an inherited intent
- **Live failover** (RF ≥ 3) — kill an instance, its shards re-elect on survivors, data intact
- **Dynamic membership + online rebalancing** — **Raft configuration changes** (voter set carried in
  the log, one server changed at a time so majorities overlap; a removed leader steps down only after
  the change commits) + a **control plane** (`pd.py` / `Pd.java`): add a node and shards (with their
  data) rebalance onto it via Raft catch-up; remove a node and its shards re-replicate to restore RF —
  online, data intact
- **Gossip-based discovery** — nodes join from a small **seed** set (not a static full list) and learn
  the whole cluster via **SWIM/Cassandra-style gossip** (`gossip.py` / `Gossip.java`); weak liveness
  (alive/suspect/dead) is derived locally from heartbeat freshness, so the static address book is now
  just a bootstrap seed source, not the source of truth
- **Control plane is its OWN Raft group (PD)** — the placement driver (`pd.py` / `Pd.java`, the TiKV PD
  model) is co-located on `PD_NODES`; membership decisions are committed to its Raft log and the PD
  leader runs reconcile + failure detection. Kill the PD leader and a surviving replica takes over and
  resumes from the durable log — a half-finished rebalance is completed, not lost. The control plane is
  **no longer a SPOF** (`pd_failover_smoke.py` / `PdFailoverSmoke.java`); `controller.py` is now a thin client
- **Auto-heal on node death** — the **PD leader** runs a gossip-fed **failure detector**; when a node
  is confirmed DEAD by a majority of its live peers past a grace window, it **proposes `remove_node`
  through the PD Raft log** and reconcile re-replicates the shards to restore RF — no operator action,
  and the healing survives a PD-leader crash (`autoheal_smoke.py` / `AutoHealSmoke.java`)
- **Rich central dashboard** — health, config, consistent-hash ring, shard→node placement matrix,
  the live gossip membership matrix (what each node discovered + alive/suspect/dead),
  one event feed per instance + a merged system stream; kill/restart **and add/remove** nodes from the
  UI, with a control-plane rebalancing log

**Run it:** `python dashboard.py` or `java com.litedb.cluster.Dashboard` → http://127.0.0.1:7080
(Java: 7180). **Tests:** `pytest test_distributed.py`; `python cluster_smoke.py` /
`recovery_smoke.py` / `participant_recovery_smoke.py` / `rebalance_smoke.py` / `gossip_smoke.py` /
`autoheal_smoke.py` / `pd_failover_smoke.py`; Java `ClusterSmoke` / `ClusterRecoverySmoke` /
`ClusterParticipantRecoverySmoke` / `ClusterRebalanceSmoke` / `GossipSmoke` / `AutoHealSmoke` /
`PdFailoverSmoke`. Set `LITEDB_CLUSTER_RF=2` for replication factor 2.

The remaining gap is no longer *integration* — it is **cross-machine hardening**.

---

## Known simplifications (deliberate, single-machine scope)

These are conscious shortcuts in the current build — each works for the demo and each is a real,
named gap for production. Listed so the boundary is explicit, not hidden.

- **The control plane is a replicated Raft group (PD), but its membership is fixed and co-located.**
  The placement driver (`pd.py` / `Pd.java`) is its OWN Raft group on `PD_NODES`: decisions are durable
  in its log and a new PD leader resumes a half-finished rebalance — so it is **no longer a SPOF** (the
  TiKV PD model). The residual simplifications: the PD's voter set is a fixed co-located trio that
  doesn't auto-shrink (kill a PD node and the PD group runs degraded until the trio is manually
  reconfigured), the PD is co-located in data nodes rather than a separate tier, and reconcile is a
  single-leader control loop (no sharded scheduler). Production also decentralizes placement entirely
  (CockroachDB) or scales the PD out.
- **Rebalancing is even-spread; no range split/merge.** The balancer assigns whole fixed shards
  round-robin; it has no capacity/load awareness, throttling, or range splitting as data grows.
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
- **Membership change is single-server only.** Add/remove one voter at a time is implemented; there's
  no joint-consensus (multi-server) change and no learner-then-promote phase. Death-triggered
  re-replication is now **automatic** (the controller's gossip failure detector reaps a confirmed-dead
  node and restores RF), but a returning node is not yet auto-re-added — bringing capacity back is a
  manual `+ Add node`.
- **Snapshot install is log-based.** A far-behind replica catches up by log replication, not by
  shipping a compacted snapshot — fine for small logs, not for large state.
- **Seed-based discovery, single-machine seeds.** Nodes discover each other via **gossip** from a
  small seed set (no static full list) and derive liveness locally; the gossip "dead" verdict now
  **auto-triggers re-replication** to restore RF. The remaining single-machine assumption is that the
  seed addresses come from shared config — cross-machine you'd point seeds at stable DNS/IPs. The
  failure detector runs on the PD leader (replicated, not a SPOF), but proposes through the single PD
  Raft group described above.
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
- [x] A placement driver / balancer (even-spread): add/remove a node → shards rebalance online, with
      data moving via Raft catch-up; remove → re-replicate to restore RF (`pd.py` / `Pd.java`)
- [x] **Placement driver replicated as its OWN Raft group** (TiKV PD model): decisions committed to the
      PD log, reconcile + failure detection on the PD leader; survives a PD-leader crash, no SPOF
- [ ] Auto-shrink/grow the PD's own voter set; separate PD tier; sharded (multi-leader) scheduler
- [ ] Range split/merge as data grows/shrinks (shards are fixed today)
- [ ] Capacity-/load-aware placement, hot-shard movement, throttled relocation (the balancer is naive)

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
- [x] Gossip membership + heartbeats + local failure detection (SWIM/Cassandra-style, seed-based discovery)
- [x] Raft membership changes (single-server) + online rebalancing on node add/remove
- [x] Gossip failure detector **auto-triggers re-replication** to restore RF on node death (no operator action)
- [ ] Cross-machine gossip hardening: stable seed/DNS discovery, phi-accrual detector, partition handling
- [ ] Joint-consensus (multi-server) changes; learner-then-promote to avoid catch-up stalls
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
