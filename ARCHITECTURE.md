# litedb — Architecture & Design Decisions

A study guide to how litedb works, bottom to top, and *why* each decision was made. Read it once
to understand the system; re-read the "Design decisions" table before an interview. File references
point at the Python implementation (`litedb-python/`); the Java implementation
(`litedb-java/`, `com.litedb.*`) mirrors it one-to-one.

## The one-line mental model

litedb is **"SQL on a replicated key-value store"** — the CockroachDB / TiKV architecture. Every
layer is a clean abstraction over the one below it:

```
  SQL  (tables, indexes, query planner)              ─┐
   │   everything is encoded as KV keys               │  single-node
  MVCC (multi-version keys, snapshot isolation)        │  database
   │   one physical key per version                    │
  Storage engine (LSM-tree | B+Tree), pluggable       ─┘
   │
  ── partition a key into a shard (consistent hashing) ─┐
  Multi-Raft (each shard = one Raft group, replicated)   │  distributed
   │   committed log entries apply to the shard's MVCC    │  cluster
  Routing + cross-shard 2PC + failure recovery          ─┘  (many instances)
```

---

## Layer 1 — Storage engine (pluggable)

**Interface:** [`storage_engine.py`](litedb-python/storage_engine.py) defines `StorageEngine`
(`set`/`get`/`delete`/`scan`/`write_batch`) and `WriteOp` (one put/delete in an atomic batch).
Two implementations sit behind it:

### LSM-tree — [`lsm_engine.py`](litedb-python/lsm_engine.py)
Write-optimized. A write goes to a **WAL** (append + `fsync`, for durability) and an in-memory
**MemTable** (sorted). When the MemTable fills, it's flushed to an immutable **SSTable** on disk;
SSTables are merged by **leveled compaction**. Reads check MemTable → L0 → L1 (newest first).

- *Why:* sequential writes (append-only) are far faster than in-place updates; compaction amortizes
  the cost. This is how RocksDB / Cassandra / modern KV stores work.
- *Tradeoff:* reads may touch several SSTables (read amplification); compaction costs IO. Bloom
  filters + a block cache would mitigate (a listed gap).

### B+Tree — [`btree.py`](litedb-python/btree.py) / [`btree_engine.py`](litedb-python/btree_engine.py)
Read-optimized: leaf nodes are linked, so a range scan is `O(k + log n)` with no tree re-traversal.

- *Why have both?* Write-heavy vs read-heavy workloads want different structures. The pluggable
  interface lets the same database swap engines — the layers above don't care.

### Atomic write batches
`write_batch([WriteOp...])` lands a group of keys all-or-nothing (the LSM frames them between WAL
`BEGIN`/`COMMIT`; recovery discards an un-committed trailing batch). This is what lets a row and its
index entries — or a versioned write and its intent deletion — commit together.

---

## Layer 2 — MVCC (multi-version concurrency control)

[`mvcc.py`](litedb-python/mvcc.py) turns the single-version KV store into a multi-version one, so
readers see a consistent snapshot while writers add new versions (no read locks).

### The version-key encoding — the detail to know cold
Each version of a user key is stored as **one physical key**:

```
  versionKey(userKey, ts) = userKey + SEP + format(MAX_TS - ts, "016x")     # SEP = '\0'
```

The suffix is `MAX_TS − commitTs` in fixed 16-hex, so **newer versions (larger ts) sort FIRST**
within a user key. That makes a snapshot read a single seek:

```
  read(key, readTs):  scan from versionKey(key, readTs)  →  take the first entry
                      = the newest version with commitTs ≤ readTs
```

Two subtleties worth being able to explain:
- The suffix is **fixed width (17 chars)** so parsing strips a fixed length rather than searching
  for `SEP` — necessary because a user key can *itself* contain `SEP` (the relational index keys are
  `value‖pk`).
- A delete is a **tombstone** value, not a missing key (so it shadows older versions at read time).

### Snapshot isolation + OCC
- A transaction reads at a fixed `read_ts` → it sees a stable snapshot even as others commit.
- On commit, an **optimistic** write-write conflict check: if any written key has a committed
  version newer than `read_ts`, abort (first-committer-wins). No locks on the read path.
- `vacuum(low_water_ts)` garbage-collects versions no live snapshot can still see.

---

## Layer 3 — Relational / SQL

[`relational_engine.py`](litedb-python/relational_engine.py) runs SQL over the MVCC engine. The key
idea: **catalog, rows, and indexes are all just versioned KV keys** under reserved prefixes:

```
  __catalog__/table/<t>            table schema
  __catalog__/index/<name>         index definition
  <table>/<pk>                     a row
  __idx__/<table>/<col>/<encVal>‖<pk>   a secondary-index entry
```

- **Query planner** ([`relational_engine.py`](litedb-python/relational_engine.py)) — if the `WHERE`
  column is indexed it does an index range-scan; otherwise a full scan. The `-- plan:` line in the
  output shows which.
- **Order-preserving typed encoding** ([`type_codec.py`](litedb-python/type_codec.py)) — so numeric
  ranges sort correctly as bytes: INT via sign-biased fixed-width hex (`x + 2^63`), FLOAT via an
  IEEE-754 sortable transform, TEXT as-is. Without this, `"10" < "9"` lexicographically would break
  range queries.
- Every statement runs in an MVCC transaction (auto-commit, or explicit `BEGIN`/`COMMIT`/`ROLLBACK`).

---

## Layer 4 — Distributed cluster

Many instances (one OS process each — a **NodeServer**, [`node.py`](litedb-python/node.py))
coordinate to form one partitioned, replicated, transactional database.

### Transport — [`rpc.py`](litedb-python/rpc.py)
Length-framed JSON over TCP. Raft messages, client requests, and 2PC all ride it. Persistent
connections with reconnect-once, so a peer that dies and restarts heals transparently.

### Partitioning — [`partition.py`](litedb-python/partition.py)
A **consistent-hash ring** maps a key → shard (the shards are placed on the ring; a key hashes to
the shard owning its arc). Placement maps shard → replica nodes. Consistent hashing means
adding/removing a shard remaps only ~1/N keys (vs ~all keys with `hash % N`).

### Replication — Multi-Raft, [`raft_node.py`](litedb-python/raft_node.py)
**Each shard is its own Raft group.** A node runs *many* Raft groups at once — leader of some
shards, follower of others. The proven Raft algorithm (election, log replication, the up-to-date
voting safety rule, commit advancement) with **persistent** term/vote/log (`fsync`) so a restarted
replica recovers. Committed entries apply deterministically to that shard's MVCC store
([`shard_store.py`](litedb-python/shard_store.py)) — so replicas converge byte-for-byte.

> **Multi-raft vs single-raft** — the decision most people miss. One Raft group for the whole
> cluster (etcd-style) means one leader serializes *every* write → a throughput ceiling. One group
> *per shard* means N independent leaders spread across nodes → writes to different shards commit in
> parallel. That's what makes sharding actually scale writes.

### Routing — [`node.py`](litedb-python/node.py), [`cluster_client.py`](litedb-python/cluster_client.py)
A client contacts *any* node. That node computes the key's shard (the same partition map everyone
shares), resolves the shard's leader, and either serves it or forwards. Node *addresses* come from
gossip (below), falling back to the static pool. With replication factor < node count, a node may not
host a shard at all — it then asks the shard's replica nodes for the leader. So routing works for any RF.

### Discovery — gossip membership, [`gossip.py`](litedb-python/gossip.py)
A node doesn't need the full node list to join — only a **seed** (one or two well-known addresses).
It runs a **SWIM/Cassandra-style gossip** loop: every ~1s it anti-entropy push-pulls its membership
table with a few random peers, merging each entry by `(generation, heartbeat)`. From a single seed it
**transitively** learns every node (the seed tells a newcomer about everyone it already knows), and
everyone learns the newcomer. **Liveness is derived locally** — if a peer's heartbeat stops advancing
it ages `alive → suspect → dead` (the Cassandra split: gossip carries heartbeats, the failure detector
is local); a restart bumps `generation`, so a returning node outranks its own stale heartbeat and is
re-adopted as alive automatically. This is deliberately **not Raft**: Raft gives strong consistency for
a *known* group; gossip discovers and disseminates *who* the group is — leaderless, eventually
consistent, partition-tolerant. The static address book is now just a *bootstrap seed source*, not the
source of truth. The dashboard renders a live membership matrix (what each node has discovered, and
each peer's alive/suspect/dead state).

### Cross-shard transactions — 2PC + HLC
A single Raft commit is atomic only *within* one shard. A write spanning shards runs **two-phase
commit**, coordinated by the node that received it:

1. **PREPARE** each shard's leader: conflict-check, then *replicate a PREPARE intent through that
   shard's Raft log*.
2. Once all vote YES, durably record the **COMMIT** decision (`fsync`), then **COMMIT** each shard
   (replicate a commit entry → apply the intent's writes as versioned puts at the commit timestamp).

Commit timestamps come from a **Hybrid Logical Clock** ([`hlc.py`](litedb-python/hlc.py)) — a
monotonic, globally-comparable timestamp, because the single-node timestamp oracle doesn't
generalize across processes. (It's nanosecond-resolution; an early millisecond version broke
cross-node snapshot isolation.)

### 2PC failure recovery
- **Coordinator crash** — the coordinator persists a transaction record
  ([`txn_log.py`](litedb-python/txn_log.py)); a periodic sweep on every node re-drives in-doubt
  transactions (`committing` → re-send COMMIT, `aborted` → re-send ABORT, stale `preparing` →
  ABORT), re-resolving the current leader each time. Idempotent.
- **Participant (shard-leader) crash** — because PREPARE intents live *in the Raft log*, a new
  leader **inherits** them. So a conflicting write is still rejected (isolation preserved) and the
  txn can still commit on the new leader. A newly-elected leader commits a **no-op** and is **not
  "ready"** to serve conflict-checked writes until it applies that no-op — guaranteeing it has
  applied every inherited intent first (Raft's commit-point rule).

### Dynamic membership + rebalancing — [`controller.py`](litedb-python/controller.py)
The cluster can grow and shrink online, with data moving. Two pieces:

- **Raft configuration changes** ([`raft_node.py`](litedb-python/raft_node.py)) — a group's
  membership is its *voter set*, carried in the log as `{op:config}` entries; the latest one wins
  (so config replicates like any entry). `reconfigure()` changes **one** voter at a time, so the old
  and new majorities always overlap (Raft's safety condition). Quorum, elections, and commit all
  count the *current* voters; a leader removed from the config keeps leading until that change
  **commits**, then steps down (so the new config is durable first).
- **A control plane** ([`controller.py`](litedb-python/controller.py), a simplified TiKV Placement
  Driver) — holds the authoritative shard→node placement, computes an even target when the node set
  changes, and applies the moves one membership change at a time: to add a replica it creates a
  follower on the target node and adds it to the config — the node then **catches up via Raft**
  (this *is* the data moving) — then drops the old replica. Adding a node rebalances shards onto it;
  removing one re-replicates its shards to restore RF.

The control plane is a single orchestrator (a SPOF for *control* operations, not the data plane —
shards keep serving through their Raft groups). Production replicates it (PD-as-Raft-group) or
decentralizes it (CockroachDB); see [ROADMAP.md](ROADMAP.md).

**Auto-heal on death.** The controller also runs a gossip-fed **failure detector**
(`start_failure_detector`): a reconcile loop that reads each node's gossip view via `status` and, when
a node is reported DEAD by a **majority** of its live peers and stays dead past a grace window, fires
`remove_node(dead=True)` — re-replicating its shards onto survivors to **restore RF, with no operator
action**. The majority rule rejects one node's false suspicion; an "act only while a majority is alive"
guard stops a partitioned minority from reaping the majority (and matches the fact that a Raft config
change needs a quorum to commit anyway). So a node death self-heals end-to-end: failover keeps it
available immediately, then gossip detection → reap → re-replication restores full redundancy.

### Watching it — [`events.py`](litedb-python/events.py) / [`dashboard.py`](litedb-python/dashboard.py)
Each node emits a human-readable event for every meaningful action (election, accepting a leader,
routing by hashing, replicating, applying, running 2PC). The dashboard shows cluster health, config,
the consistent-hash ring, the shard→node placement matrix, one feed per instance, and a merged
stream — so you can *watch* the reasoning.

---

## End-to-end flows

**Single-key write** — client → any node → compute shard → resolve leader → forward → leader
conflict-checks, assigns an HLC ts, proposes `{ts, writes}` to the shard's Raft → replicated to a
majority + applied as a versioned put → ack.

**Cross-shard transaction** — client → coordinator node → group keys by shard → PREPARE each leader
(replicate intent) → all YES → fsync the COMMIT decision → COMMIT each leader (apply intents) →
done. If the coordinator dies after the decision, its restart sweep finishes it.

**Participant-leader crash mid-transaction** — the prepared intent is already in the shard's Raft
log; the shard re-elects a new leader that inherits the intent; the new leader rejects conflicting
writes (until and after it's ready) and commits when the coordinator's COMMIT arrives.

**Add a node** — the controller computes a new even placement → for each shard moving onto the new
node, it creates a follower there and appends a config entry adding it (one server at a time) → the
follower catches up via Raft (the data moves) → the controller drops an old replica. Remove is the
mirror: add a replacement replica elsewhere to restore RF, then drop the departing one.

**Node death (auto-heal)** — a node dies → its shards' Raft groups re-elect leaders on the survivors
(immediate failover, data intact) → gossip ages the node to DEAD in its peers' views → the controller's
failure detector sees the majority-dead verdict past the grace window → fires `remove_node(dead)` →
the dead voter is dropped from each affected shard and a replacement replica is added on a survivor
(catch-up via Raft) → RF restored, all with no operator action.

---

## Design decisions (the interview-defensible set)

| Decision | Why | Tradeoff / what's missing |
|---|---|---|
| LSM **and** B+Tree, pluggable | write- vs read-optimized; swap per workload | LSM read amplification (no bloom filter / block cache yet) |
| MVCC version-key = `userKey‖(MAX−ts)` | newest version sorts first → snapshot read is one seek; order-preserving | fixed-width suffix needed because keys can contain the separator |
| Snapshot isolation + OCC | lock-free reads; first-committer-wins | aborts under high write contention; not serializable |
| SQL = KV keys (catalog/rows/indexes) | one storage substrate for everything (the TiKV model) | joins / aggregates are out of scope |
| Order-preserving typed encoding | numeric range scans work as byte ranges | per-type encoders to maintain |
| Multi-raft (group per shard) | N leaders → parallel writes, balanced load | more moving parts than single-raft |
| Consistent hashing | adding a shard remaps ~1/N keys | no range scans across shards; fixed shard set (no split/merge) |
| 2PC for cross-shard atomicity | single Raft commit is atomic only within a shard | blocking protocol; parallel-commit would cut latency |
| HLC for commit timestamps | cross-shard snapshots need a global clock | needs bounded clock skew across machines (NTP / TrueTime) |
| Replicated intents (Percolator) + readiness gate | prepared state survives a leader change → correct recovery | extra Raft round-trip per prepare |
| Raft config changes, one server at a time | safe membership change (majorities overlap); enables online add/remove + rebalancing | single-server only (no joint consensus); naive even-spread balancer |
| Single controller / placement driver | simple authority for placement + rebalancing | SPOF for control ops (not data); production replicates or decentralizes it |
| Gossip discovery (SWIM/Cassandra-style) | join from one seed, not a full static list; decentralized, partition-tolerant liveness | eventually consistent (not authoritative); cross-machine still needs a stable seed/DNS |
| Auto-heal: failure detector → re-replicate | a confirmed-dead node's shards re-replicate automatically to restore RF (no operator) | runs in the single controller (control-plane SPOF); needs majority-alive + a grace window; no auto re-add of a returning node |

---

## Honest scope

Everything above is **real** on a single machine: real RPC between real processes, real Raft, real
partitioning, real 2PC, real failure recovery. It is **not** hardened for the cross-machine failure
matrix — no parallel-commit, no Raft membership changes, log-based (not snapshot) catch-up, and it
is **not** Jepsen-tested. [ROADMAP.md](ROADMAP.md) maps exactly what's built vs. the ladder to a
production-grade, cross-machine database. That boundary is deliberate: the single-node engine is the
easy ~20%; the distributed + correctness layer is the ~80% that takes a team years.

## Explore it

```bash
# single-node engine
cd litedb-python && python relational_engine.py    # SQL over MVCC
python mvcc_demo.py                                 # snapshot isolation, conflict, GC

# distributed cluster (web UI)
python dashboard.py                                 # http://127.0.0.1:7080  (Java: Dashboard, :7180)
LITEDB_CLUSTER_RF=2 python dashboard.py             # replication factor 2

# headless proofs
pytest test_distributed.py
python cluster_smoke.py            # partitioning, multi-raft, 2PC, failover
python recovery_smoke.py           # 2PC coordinator-crash recovery
python participant_recovery_smoke.py   # 2PC participant-leader-crash recovery
python rebalance_smoke.py          # add a node (data rebalances on) then remove it (re-replicates)
python gossip_smoke.py             # seed-based discovery + failure detection (3 nodes, one seed)
LITEDB_CLUSTER_NODES=4 python autoheal_smoke.py   # kill a node → controller auto-re-replicates to restore RF
```
