# Deep Dive: Database Replication - Master-Slave Synchronization

This document covers one of the most critical aspects of production databases: **replication** and **high availability**, and how databases keep replicas in sync.

## The Core Problem

A common assumption is that a master and its slave (read replica) are always in sync, so that if the master crashes the slave can take over without customer impact.

In practice, a master and its replicas are **not always perfectly in sync**. This is a fundamental trade-off in distributed systems.

There are different replication modes, each with different guarantees:
1. **Synchronous Replication** - Strong consistency, but slower
2. **Asynchronous Replication** - Fast, but can lose data
3. **Semi-Synchronous** - Middle ground

The sections below examine each in detail.

---

## The Replication Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CLIENT APPLICATIONS                      │
└─────────────────────────────────────────────────────────────┘
                    ↓ (writes)        ↑ (reads)
┌─────────────────────────────────────────────────────────────┐
│                    MASTER DATABASE                          │
│  - Accepts writes (INSERT, UPDATE, DELETE)                  │
│  - Generates WAL (Write-Ahead Log)                          │
│  - Sends WAL to replicas                                    │
└─────────────────────────────────────────────────────────────┘
                    ↓ (WAL stream)
        ┌───────────┴───────────┬───────────┐
        ↓                       ↓           ↓
┌───────────────┐      ┌───────────────┐   ┌───────────────┐
│   REPLICA 1   │      │   REPLICA 2   │   │   REPLICA 3   │
│  (Read-only)  │      │  (Read-only)  │   │  (Read-only)  │
│  - Applies WAL│      │  - Applies WAL│   │  - Applies WAL│
│  - Serves reads│     │  - Serves reads│  │  - Serves reads│
└───────────────┘      └───────────────┘   └───────────────┘
```

---

## Replication Mode 1: Asynchronous Replication (Default)

This is the **most common** mode, used by default in most databases.

### How It Works:

```
Timeline:

T1: Client sends: UPDATE accounts SET balance = 400 WHERE id = 1
    ↓
T2: Master writes to WAL
    ↓
T3: Master commits (fsync WAL)
    ↓
T4: Master returns SUCCESS to client ✓
    ↓
T5: Master sends WAL to replicas (asynchronously)
    ↓
T6: Replica 1 receives WAL
    ↓
T7: Replica 1 applies changes
    ↓
T8: Replica 2 receives WAL (might be delayed!)
    ↓
T9: Replica 2 applies changes
```

**Key Point:** Master returns success at T4, but replicas might not have the data until T7, T9, etc.

### The Problem: Replication Lag

```
State at different times:

T4 (Master committed):
Master:    balance = 400 ✓
Replica 1: balance = 500 (old value!)
Replica 2: balance = 500 (old value!)

T7 (Replica 1 caught up):
Master:    balance = 400 ✓
Replica 1: balance = 400 ✓
Replica 2: balance = 500 (still behind!)

T9 (All caught up):
Master:    balance = 400 ✓
Replica 1: balance = 400 ✓
Replica 2: balance = 400 ✓
```

**This delay is called "replication lag"** - typically milliseconds to seconds, but it can reach minutes under load.

### What Happens If Master Crashes?

**Scenario: Master crashes at T5 (after commit, before replicas receive WAL)**

```
State at crash:
Master:    balance = 400 (committed, but crashed!)
Replica 1: balance = 500 (didn't receive update yet)
Replica 2: balance = 500 (didn't receive update yet)

Failover to Replica 1:
- Replica 1 becomes new master
- Replica 1 has balance = 500 (old value!)
- Client's committed transaction is LOST! ❌
```

**This is called "data loss during failover"** - a real problem with async replication.

### Pros and Cons:

**✅ Pros:**
- Fast: the master does not wait for replicas
- High throughput
- Works even if replicas are slow or down

**❌ Cons:**
- Replication lag (replicas behind master)
- Data loss possible during failover
- Read-after-write inconsistency

---

## Replication Mode 2: Synchronous Replication

This ensures **zero data loss** but at a performance cost.

### How It Works:

```
Timeline:

T1: Client sends: UPDATE accounts SET balance = 400 WHERE id = 1
    ↓
T2: Master writes to WAL
    ↓
T3: Master commits (fsync WAL)
    ↓
T4: Master sends WAL to replicas
    ↓
T5: Master WAITS for replicas to acknowledge ⏳
    ↓
T6: Replica 1 receives WAL
    ↓
T7: Replica 1 applies changes
    ↓
T8: Replica 1 sends ACK to master ✓
    ↓
T9: Replica 2 receives WAL
    ↓
T10: Replica 2 applies changes
    ↓
T11: Replica 2 sends ACK to master ✓
    ↓
T12: Master returns SUCCESS to client ✓
```

**Key Point:** The master waits at T5 until replicas acknowledge. The client does not receive success until T12.

### The Guarantee: Zero Data Loss

```
State when client receives success (T12):
Master:    balance = 400 ✓
Replica 1: balance = 400 ✓
Replica 2: balance = 400 ✓

All in sync!
```

**If master crashes now:**
```
Failover to Replica 1:
- Replica 1 becomes new master
- Replica 1 has balance = 400 ✓
- No data loss ✓
```

### Pros and Cons:

**✅ Pros:**
- Zero data loss during failover
- Strong consistency guarantee
- Replicas always have latest committed data

**❌ Cons:**
- Slow: the master waits for replicas
- Lower throughput
- If a replica is down, the master blocks (an availability issue)

### PostgreSQL Configuration:

```sql
-- Enable synchronous replication
ALTER SYSTEM SET synchronous_commit = 'on';
ALTER SYSTEM SET synchronous_standby_names = 'replica1,replica2';

-- Master will wait for at least 1 replica
ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (replica1,replica2)';

-- Master will wait for ALL replicas
ALTER SYSTEM SET synchronous_standby_names = 'ALL (replica1,replica2)';
```

---

## Replication Mode 3: Semi-Synchronous Replication (Best of Both Worlds)

This is a **compromise** between async and sync.

### How It Works:

```
Timeline:

T1: Client sends: UPDATE accounts SET balance = 400 WHERE id = 1
    ↓
T2: Master writes to WAL
    ↓
T3: Master commits (fsync WAL)
    ↓
T4: Master sends WAL to replicas
    ↓
T5: Master waits for AT LEAST ONE replica ⏳
    ↓
T6: Replica 1 receives WAL
    ↓
T7: Replica 1 writes to its WAL (not necessarily applied yet!)
    ↓
T8: Replica 1 sends ACK to master ✓
    ↓
T9: Master returns SUCCESS to client ✓
    ↓
T10: Replica 2 receives WAL (later, asynchronously)
```

**Key Point:** The master waits for just ONE replica to acknowledge, then returns success.

### The Guarantee: Minimal Data Loss

```
State when client receives success (T9):
Master:    balance = 400 ✓
Replica 1: balance = 400 (in WAL, might not be applied yet)
Replica 2: balance = 500 (still behind)

At least one replica has the data!
```

**If master crashes:**
```
Failover to Replica 1:
- Replica 1 has the data in WAL ✓
- Can replay WAL to get balance = 400 ✓
- Minimal or no data loss ✓
```

### MySQL Configuration:

```sql
-- Enable semi-synchronous replication
INSTALL PLUGIN rpl_semi_sync_master SONAME 'semisync_master.so';
SET GLOBAL rpl_semi_sync_master_enabled = 1;

-- Wait for at least 1 replica
SET GLOBAL rpl_semi_sync_master_wait_for_slave_count = 1;

-- Timeout (fallback to async if replicas are slow)
SET GLOBAL rpl_semi_sync_master_timeout = 1000; -- 1 second
```

---

## The Replication Process: Under the Hood

The following steps show how WAL is actually replicated:

### Step 1: Master Generates WAL

```
Master's WAL:
┌─────────────────────────────────────────────────────────────┐
│ LSN 1000: [XID 100] BEGIN                                   │
│ LSN 1001: [XID 100] UPDATE accounts SET balance=400 WHERE..│
│ LSN 1002: [XID 100] COMMIT                                  │
│ LSN 1003: [XID 101] BEGIN                                   │
│ LSN 1004: [XID 101] INSERT INTO users VALUES...            │
└─────────────────────────────────────────────────────────────┘
```

### Step 2: Master Streams WAL to Replicas

```
Network Stream:
Master → Replica 1: [LSN 1000-1002] (transaction 100)
Master → Replica 2: [LSN 1000-1002] (transaction 100)
Master → Replica 1: [LSN 1003-1004] (transaction 101)
...
```

### Step 3: Replica Applies WAL

```
Replica's Process:
1. Receive WAL from master
2. Write to local WAL file (for durability)
3. Apply changes to database
4. Send ACK to master (if sync/semi-sync)
5. Update "replay position" (LSN 1002)
```

### Step 4: Monitoring Replication Lag

```sql
-- PostgreSQL: Check replication lag
SELECT 
    client_addr,
    state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    sync_state,
    pg_wal_lsn_diff(sent_lsn, replay_lsn) AS lag_bytes
FROM pg_stat_replication;

-- Result:
-- client_addr | state     | lag_bytes | sync_state
-- 10.0.1.2    | streaming | 0         | sync
-- 10.0.1.3    | streaming | 16384     | async
```

**Interpretation:**
- Replica 1 (10.0.1.2): No lag, synchronous
- Replica 2 (10.0.1.3): 16KB behind, asynchronous

---

## Failover: Promoting a Replica to Master

When the master crashes, a replica must be promoted:

### Automatic Failover Process:

```
1. Detection:
   ┌─────────────────────────────────────────┐
   │ Health Check: Master not responding     │
   │ Timeout: 30 seconds                     │
   └─────────────────────────────────────────┘
          ↓
2. Replica Selection:
   ┌─────────────────────────────────────────┐
   │ Choose replica with:                    │
   │ - Most up-to-date data (highest LSN)    │
   │ - Lowest replication lag                │
   │ - Healthy status                        │
   └─────────────────────────────────────────┘
          ↓
3. Promotion:
   ┌─────────────────────────────────────────┐
   │ Replica 1:                              │
   │ - Stop replication from old master      │
   │ - Apply any remaining WAL               │
   │ - Promote to master (accept writes)     │
   │ - Update DNS/load balancer              │
   └─────────────────────────────────────────┘
          ↓
4. Reconfiguration:
   ┌─────────────────────────────────────────┐
   │ Other replicas:                         │
   │ - Point to new master (Replica 1)       │
   │ - Start replicating from new master     │
   └─────────────────────────────────────────┘
```

### PostgreSQL Failover Example:

```bash
# On the replica to be promoted:
pg_ctl promote -D /var/lib/postgresql/data

# This creates a "trigger file" that tells the replica:
# "You are now the master!"
```

### The Problem: Split-Brain

**Dangerous scenario:**

```
Network partition:

┌──────────────┐              ┌──────────────┐
│  Old Master  │   X X X X    │  New Master  │
│  (isolated)  │   Network    │  (promoted)  │
│              │   Partition  │              │
└──────────────┘              └──────────────┘
       ↓                             ↓
  Accepts writes!              Accepts writes!
       ↓                             ↓
  Data diverges! ❌
```

**Solution: Fencing**
- Use a "witness" node to determine which is the real master
- Old master must be shut down before promoting new master
- Use distributed consensus (Raft, Paxos)

---

## Real-World Replication Strategies

### Strategy 1: One Synchronous + Multiple Async

```
Master
  ├─ Replica 1 (Synchronous) ← Zero data loss
  ├─ Replica 2 (Async)       ← Fast reads, can lag
  └─ Replica 3 (Async)       ← Fast reads, can lag
```

**Benefits:**
- Zero data loss (sync replica)
- High read throughput (async replicas)
- Good balance

### Strategy 2: Quorum-Based Replication

```
Master writes to 3 replicas
Wait for 2 out of 3 to acknowledge (quorum)
Then return success

Guarantees: Data exists on at least 2 nodes
```

**Used by:** Cassandra, MongoDB (with write concern)

### Strategy 3: Multi-Master Replication

```
┌──────────┐ ←→ ┌──────────┐
│ Master 1 │    │ Master 2 │
└──────────┘    └──────────┘
     ↕              ↕
Both accept writes!
```

**Challenge:** Conflict resolution
- What if both masters update the same row?
- Solutions: Last-write-wins, vector clocks, CRDTs

---

## Monitoring Replication Health

### Key Metrics to Monitor:

```sql
-- 1. Replication Lag (time-based)
SELECT 
    now() - pg_last_xact_replay_timestamp() AS replication_lag
FROM pg_stat_replication;

-- 2. Replication Lag (bytes-based)
SELECT 
    pg_wal_lsn_diff(sent_lsn, replay_lsn) AS lag_bytes
FROM pg_stat_replication;

-- 3. Replication State
SELECT 
    application_name,
    state,
    sync_state
FROM pg_stat_replication;
```

**Healthy replication:**
- Lag < 1 second (time)
- Lag < 1 MB (bytes)
- State = 'streaming'

**Unhealthy replication:**
- Lag > 10 seconds
- State = 'catchup' or 'stopped'
- Sync replica missing

---

## The CAP Theorem Connection

This relates to the **CAP theorem** (covered in detail in a later module):

```
CAP Theorem: You can only have 2 out of 3:
- Consistency (all nodes see same data)
- Availability (system always responds)
- Partition Tolerance (works despite network issues)
```

**Replication modes map to CAP:**

| Mode | Consistency | Availability | Partition Tolerance |
|------|-------------|--------------|---------------------|
| **Synchronous** | ✅ Strong | ❌ Lower | ❌ Blocks on partition |
| **Asynchronous** | ❌ Eventual | ✅ High | ✅ Works during partition |
| **Semi-Sync** | ⚠️ Medium | ⚠️ Medium | ⚠️ Medium |

---

## Key Takeaways

### ❌ Common Assumption: "Master and slave always in sync"

**Reality:** They are NOT always in sync.

**Replication modes:**
1. **Async (default)**: Fast, but replicas lag behind
2. **Sync**: Always in sync, but slower
3. **Semi-sync**: Middle ground

### ✅ Zero Data Loss Requires:

1. **Synchronous replication** to at least one replica
2. **Proper failover** process
3. **Fencing** to prevent split-brain
4. **Monitoring** replication lag

### ⚠️ Trade-offs:

```
Async Replication:
✅ Fast
✅ High availability
❌ Data loss possible
❌ Replication lag

Sync Replication:
✅ Zero data loss
✅ Strong consistency
❌ Slower
❌ Lower availability
```

### 🎯 Production Best Practice:

```
Use semi-synchronous replication:
- Wait for 1 replica (fast enough)
- Zero data loss (safe enough)
- Fallback to async if replica is down (available enough)
```

---

## Review Questions

**Question 1:** If the master commits a transaction with async replication, is it guaranteed to be on replicas?
**Answer:** No. Replicas might lag behind. Data could be lost if the master crashes before replicas receive it.

**Question 2:** Why is synchronous replication slower than asynchronous?
**Answer:** The master must wait for replicas to acknowledge before returning success to the client.

**Question 3:** What is "replication lag"?
**Answer:** The delay between the master committing data and replicas having that data. Measured in time (seconds) or bytes.

---

This topic is explored further in Module 10 (Replication and Consistency).