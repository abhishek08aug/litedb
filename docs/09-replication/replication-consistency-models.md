# Module 9: Replication & Consistency Models

Replication is the practice of keeping copies of data on multiple machines. It is the foundation of high availability, fault tolerance, and read scalability. Because copies can diverge, consistency models define the guarantees a system provides about what those copies return.

---

## Why Replicate?

```
1. High Availability
   → If one node dies, others keep serving requests
   → No single point of failure

2. Read Scalability
   → Spread read traffic across multiple replicas
   → 1 primary + 5 replicas = 6x read throughput

3. Geographic Distribution
   → Put replicas close to users (low latency)
   → US users → US replica, EU users → EU replica

4. Disaster Recovery
   → Replica in different data center / region
   → Survive entire data center failure
```

---

## Replication Topologies

### 1. Single-Leader (Primary-Replica)

```
         ┌─────────┐
Writes → │  Leader │ (Primary / Master)
         └────┬────┘
              │ replication stream
    ┌─────────┼─────────┐
    ▼         ▼         ▼
┌────────┐ ┌────────┐ ┌────────┐
│Replica1│ │Replica2│ │Replica3│
└────────┘ └────────┘ └────────┘
    ↑         ↑         ↑
  Reads     Reads     Reads
```

**How it works:**
```
1. All writes go to the Leader
2. Leader writes to its WAL (Write-Ahead Log)
3. Leader streams WAL changes to replicas
4. Replicas apply changes in the same order
5. Reads can go to any replica (or leader)
```

**Used by:** PostgreSQL streaming replication, MySQL binlog replication, MongoDB replica sets

**Pros:**
```
✓ Simple to reason about (one source of truth)
✓ No write conflicts (only leader accepts writes)
✓ Strong consistency possible (read from leader)
```

**Cons:**
```
✗ Leader is write bottleneck
✗ Leader failure requires failover (brief downtime)
✗ Replicas may lag behind leader (replication lag)
```

---

### 2. Multi-Leader (Active-Active)

```
         ┌─────────┐         ┌─────────┐
Writes → │ Leader1 │ ←────→  │ Leader2 │ ← Writes
         └────┬────┘         └────┬────┘
              │                   │
         ┌────┴────┐         ┌────┴────┐
         │Replica1 │         │Replica2 │
         └─────────┘         └─────────┘
```

**How it works:**
```
Multiple nodes accept writes simultaneously.
Each leader replicates its writes to all other leaders.
Conflicts must be detected and resolved.
```

**Used by:** CouchDB, Cassandra (all nodes are equal), MySQL Group Replication, Google Docs (operational transforms)

**Pros:**
```
✓ Writes can go to nearest data center (low latency)
✓ No single write bottleneck
✓ Survives entire data center failure without failover
```

**Cons:**
```
✗ Write conflicts! (two leaders update same row simultaneously)
✗ Complex conflict resolution logic
✗ Hard to guarantee ordering of operations
```

---

### 3. Leaderless (Dynamo-Style)

```
Client writes to multiple nodes simultaneously:

         ┌────────┐
         │ Node A │
Client → ├────────┤ (write to W nodes, read from R nodes)
         │ Node B │
         ├────────┤
         │ Node C │
         └────────┘

No leader. Any node can accept reads and writes.
```

**Used by:** Cassandra, DynamoDB, Riak, Voldemort

**How it works:**
```
Write: Client sends write to ALL N replicas (or coordinator does)
       Success when W replicas confirm (W = write quorum)

Read:  Client reads from R replicas
       Takes the value with the highest version number
       Success when R replicas respond (R = read quorum)

Consistency: W + R > N → guaranteed to read latest write
```

---

## Synchronous vs Asynchronous Replication

This is the most important trade-off in replication:

### Synchronous Replication

```
Client                Leader              Replica
  │                     │                    │
  │──── WRITE ─────────►│                    │
  │                     │──── replicate ────►│
  │                     │◄─── ACK ───────────│
  │◄─── SUCCESS ────────│                    │
  │                     │                    │

Leader waits for replica to confirm before returning success.
```

**Guarantees:**
```
✓ If leader crashes after commit, replica has the data
✓ No data loss on failover
✓ Replica is always up-to-date (zero lag)
```

**Cost:**
```
✗ Write latency = leader latency + network round trip to replica
✗ If replica is slow/down → writes block!
✗ One slow replica can slow down ALL writes
```

**Used by:** PostgreSQL synchronous_standby_names, MySQL semi-sync replication

---

### Asynchronous Replication

```
Client                Leader              Replica
  │                     │                    │
  │──── WRITE ─────────►│                    │
  │◄─── SUCCESS ────────│                    │
  │                     │──── replicate ────►│  (happens later)
  │                     │                    │
```

**Guarantees:**
```
✓ Low write latency (don't wait for replica)
✓ Replica being slow/down doesn't affect writes
```

**Cost:**
```
✗ Replication lag: replica may be seconds/minutes behind
✗ Data loss on leader crash (committed writes not yet replicated)
✗ Reading from replica may return stale data
```

**Used by:** PostgreSQL default streaming replication, MySQL default binlog

---

### Semi-Synchronous (Compromise)

```
Wait for at least 1 replica to confirm, then return success.
Other replicas catch up asynchronously.

✓ At most 1 replica's worth of data loss on failure
✓ Better write latency than fully synchronous
✓ Used by: MySQL semi-sync, PostgreSQL with 1 sync standby
```

---

## Replication Lag: The Silent Killer

Asynchronous replication means replicas lag behind the leader. This causes subtle bugs:

### Problem 1: Read-Your-Own-Writes Violation

```
Scenario:
  1. User updates their profile photo (write → leader)
  2. User immediately refreshes page (read → replica)
  3. Replica hasn't caught up yet
  4. User sees their OLD photo!
  → "Did my update even work??"

Fix: Read-your-own-writes consistency
  After a write, route that user's reads to the leader
  (or wait until replica catches up)
  
  Implementation:
    Track last_write_timestamp per user in session
    If replica's lag > (now - last_write_timestamp) → read from leader
```

### Problem 2: Monotonic Reads Violation

```
Scenario:
  1. User reads comments (from Replica 1, lag=1s) → sees 10 comments
  2. User refreshes (from Replica 2, lag=5s) → sees 8 comments!
  → Comments appear to go backwards in time!

Fix: Monotonic reads consistency
  Route each user's reads to the SAME replica consistently
  (e.g., hash(user_id) % num_replicas → always same replica)
```

### Problem 3: Consistent Prefix Reads Violation

```
Scenario (causally related writes):
  Write 1: "How are you?" (by Alice)
  Write 2: "I'm fine!" (by Bob, in reply to Alice)

  Replica A has Write 2 but not Write 1 yet.
  User reads from Replica A:
    Sees: "I'm fine!" without "How are you?"
    → Reply appears before the question!

Fix: Causal consistency
  Ensure causally related writes are replicated in order
  Track causal dependencies (vector clocks)
```

---

## Conflict Resolution in Multi-Leader Systems

When two leaders accept conflicting writes to the same row:

```
Leader 1 (US): UPDATE title = "Hello"  at time T1
Leader 2 (EU): UPDATE title = "Bonjour" at time T2

Both committed locally. Now they replicate to each other.
Which value wins?
```

### Strategy 1: Last Write Wins (LWW)

```
Use timestamp: whichever write has the later timestamp wins.

"Hello" at T=1000ms → loses
"Bonjour" at T=1001ms → wins

Problems:
  ✗ Clock skew: clocks on different machines aren't perfectly synced
  ✗ Data loss: the "losing" write is silently discarded
  ✗ Used by Cassandra (default), but dangerous for financial data
```

### Strategy 2: Merge / CRDT

```
Design data structures that can be merged automatically:

Counters: just add them up
  Leader 1: counter += 5
  Leader 2: counter += 3
  Merge: counter = original + 5 + 3 ✓

Sets: union of both sets
  Leader 1: add "apple"
  Leader 2: add "banana"
  Merge: {"apple", "banana"} ✓

These are called CRDTs (Conflict-free Replicated Data Types)
Used by: Riak, Redis (some data types), collaborative editors
```

### Strategy 3: Application-Level Resolution

```
Database detects conflict, stores BOTH versions.
Application code decides which to keep.

Example (shopping cart):
  Leader 1: cart = ["shoes", "hat"]
  Leader 2: cart = ["shoes", "jacket"]
  Conflict detected → store both
  Application merges: cart = ["shoes", "hat", "jacket"]
  (union of both carts — Amazon Dynamo does this)
```

### Strategy 4: Avoid Conflicts

```
Best strategy: route all writes for a given record to the same leader.
  All writes for user_id=1001 → always go to Leader 1
  All writes for user_id=1002 → always go to Leader 2

No conflicts possible if same record never written to two leaders.
```

---

## Failover: When the Leader Dies

```
Steps in automatic failover (single-leader):

1. Detect failure
   → Replicas stop receiving heartbeats from leader
   → Wait for timeout (e.g., 30 seconds)

2. Elect new leader
   → Replica with most up-to-date data becomes new leader
   → Uses consensus algorithm (Raft/Paxos) or simple vote

3. Reconfigure clients
   → All clients must now send writes to new leader
   → DNS update, or service discovery (etcd/Consul)

4. Old leader rejoins (if it recovers)
   → Must become a replica (not leader)
   → Must discard any writes it accepted that weren't replicated
```

**Failover dangers:**
```
Split-brain:
  Old leader recovers, thinks it's still leader
  Two leaders accepting writes simultaneously → data corruption!
  Fix: STONITH (Shoot The Other Node In The Head)
       Old leader must be fenced/killed before new leader starts

Data loss:
  Async replication → some committed writes may not be on replica
  New leader doesn't have them → they're lost
  Fix: Semi-sync or sync replication for critical data

Wrong failover:
  Leader appears dead due to network issue (not actual crash)
  Failover happens → network recovers → now two leaders!
  Fix: Longer timeout before failover, fencing tokens
```

---

## Replication in Practice

### PostgreSQL Streaming Replication
```
Primary streams WAL (Write-Ahead Log) to standbys
Standbys replay WAL entries in order
Synchronous: wait for standby ACK before commit
Asynchronous: commit immediately, stream in background

Failover: pg_ctl promote (manual) or Patroni (automatic)
Read replicas: hot_standby = on → replicas accept SELECT queries
```

### MySQL Binlog Replication
```
Primary writes binary log (binlog) of all changes
Replicas pull binlog and replay
GTID (Global Transaction ID): each transaction has unique ID
  → Easy to track replication position
  → Easy to switch replicas to new primary

Semi-sync: primary waits for at least 1 replica ACK
Group Replication: multi-primary with Paxos-based consensus
```

### Cassandra Replication
```
Leaderless: any node accepts writes
Replication factor (RF): how many copies of each row
  RF=3: each row stored on 3 nodes

Consistency levels (per query):
  ANY:    write to any 1 node (weakest, fastest)
  ONE:    write to 1 replica
  QUORUM: write to majority (RF/2 + 1)
  ALL:    write to all replicas (strongest, slowest)

Read repair: when reading, if replicas disagree → fix the stale one
Hinted handoff: if replica is down, coordinator stores hint
                → delivers write when replica comes back
```

---

## Key Takeaways

```
Replication topologies:
  Single-leader: simple, no conflicts, leader is bottleneck
  Multi-leader:  write to any DC, but conflicts possible
  Leaderless:    no SPOF, tunable consistency (W+R>N)

Sync vs Async:
  Sync:  no data loss, higher latency, blocks on slow replica
  Async: low latency, possible data loss, replication lag

Replication lag anomalies:
  Read-your-own-writes: route user's reads to leader after write
  Monotonic reads:      always read from same replica
  Consistent prefix:    causal ordering of related writes

Conflict resolution (multi-leader):
  LWW: simple but lossy
  CRDT: automatic merge for counters/sets
  App-level: store both, app decides
  Avoid: route same record to same leader

Failover risks:
  Split-brain → fencing/STONITH
  Data loss → semi-sync replication
  False failover → longer timeouts
```

---

**Next: Module 10 — NoSQL Design Patterns**

The next module covers:
- Document, Key-Value, Column-Family, Graph databases
- Denormalization as a first-class design strategy
- Data modeling for Cassandra, MongoDB, DynamoDB
- When to use NoSQL vs SQL
- Final preparation before building a database from scratch