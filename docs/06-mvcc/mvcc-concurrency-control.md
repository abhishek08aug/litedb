# Module 6: Concurrency Control & MVCC Deep Dive

How does a database serve thousands of concurrent readers and writers without them stepping on each other? This module reveals the engine behind isolation levels — **Multi-Version Concurrency Control (MVCC)**.

---

## The Problem: Concurrent Access

```
Without concurrency control:

T1: READ balance = $1000
T2: READ balance = $1000
T1: WRITE balance = $1000 - $200 = $800
T2: WRITE balance = $1000 - $300 = $700   ← Lost T1's update!

Final balance: $700  (should be $500)
This is the "Lost Update" problem.
```

Two broad solutions exist:
1. **Pessimistic** — Lock data before accessing it (2PL)
2. **Optimistic** — Don't lock; detect conflicts at commit time (MVCC + OCC)

---

## What is MVCC?

**Multi-Version Concurrency Control** = keep multiple versions of each row simultaneously.

**Core idea:**
```
Writers never block readers.
Readers never block writers.
Each transaction sees a consistent snapshot of the database
as it existed at a point in time.
```

Used by: **PostgreSQL, MySQL InnoDB, Oracle, SQL Server, MongoDB**

---

## How MVCC Works: The Version Chain

Every row has hidden metadata columns:

### PostgreSQL Implementation

```
Physical row on disk (called a "tuple"):

┌──────────────────────────────────────────────────────────┐
│  xmin  │  xmax  │  data columns                         │
│  (created by transaction ID)                             │
│  (deleted/updated by transaction ID, 0 = still alive)   │
└──────────────────────────────────────────────────────────┘

xmin = transaction ID that INSERTED this version
xmax = transaction ID that DELETED/UPDATED this version
       (0 means this version is still current)
```

**Example — INSERT:**
```sql
-- T1 (txid=100): INSERT INTO accounts VALUES (1, 1000);

Row stored as:
  xmin=100, xmax=0, id=1, balance=1000
  (created by T100, not yet deleted)
```

**Example — UPDATE:**
```sql
-- T2 (txid=200): UPDATE accounts SET balance=800 WHERE id=1;

Old row: xmin=100, xmax=200, id=1, balance=1000  ← marked deleted by T200
New row: xmin=200, xmax=0,   id=1, balance=800   ← new version by T200

Both rows exist on disk simultaneously!
```

**Example — DELETE:**
```sql
-- T3 (txid=300): DELETE FROM accounts WHERE id=1;

Row: xmin=200, xmax=300, id=1, balance=800  ← marked deleted by T300
(Row not physically removed yet — VACUUM does that later)
```

---

## Transaction Snapshots: What Each Transaction Sees

When a transaction starts, PostgreSQL takes a **snapshot**:

```
Snapshot = {
  xmin:  lowest active txid (all txids below this are committed)
  xmax:  highest txid seen so far (all txids above this are invisible)
  xip:   list of in-progress transaction IDs
}

Visibility rule for a row version:
  VISIBLE if:
    row.xmin < snapshot.xmin  (created before snapshot, committed)
    OR row.xmin in committed txids AND row.xmin not in xip

  INVISIBLE if:
    row.xmin >= snapshot.xmax  (created after snapshot)
    OR row.xmin in xip         (created by in-progress transaction)
    OR row.xmax is committed and row.xmax < snapshot.xmax
       (deleted before snapshot)
```

**Concrete example:**

```
Timeline:
  T100: INSERT balance=1000  (committed)
  T200: UPDATE balance=800   (in progress when T300 starts)
  T300: SELECT balance       (starts now, takes snapshot)

T300's snapshot: xmin=101, xmax=301, xip=[200]

Rows on disk:
  Row A: xmin=100, xmax=200, balance=1000
  Row B: xmin=200, xmax=0,   balance=800

T300 evaluates Row A:
  xmin=100 < xmin=101 → committed before snapshot ✓
  xmax=200 is in xip=[200] → deleter is in-progress → NOT deleted yet ✓
  → Row A is VISIBLE to T300 → T300 sees balance=1000

T300 evaluates Row B:
  xmin=200 is in xip=[200] → created by in-progress transaction
  → Row B is INVISIBLE to T300

Result: T300 sees balance=1000 (the old value, before T200's update)
This is the "consistent snapshot" guarantee!
```

---

## MVCC and Isolation Levels

MVCC naturally implements different isolation levels by choosing WHEN to take the snapshot:

```
READ COMMITTED:
  Take a new snapshot at the start of EACH STATEMENT
  → Always sees latest committed data
  → Can see different data in different statements of same transaction
  → Non-repeatable reads possible

REPEATABLE READ / SNAPSHOT ISOLATION:
  Take snapshot ONCE at start of transaction
  → All statements in transaction see same snapshot
  → No non-repeatable reads
  → Phantom reads prevented (in PostgreSQL's implementation)

SERIALIZABLE (SSI):
  Snapshot isolation + conflict detection
  → Detects serialization anomalies at commit time
  → Aborts transactions that would violate serializability
```

---

## The Write Skew Problem (MVCC's Weakness)

MVCC with snapshot isolation does NOT prevent **write skew**:

```
Scenario: Hospital on-call system
  Rule: At least 1 doctor must be on call at all times
  Current state: Alice=on-call, Bob=on-call

T1 (Alice): reads → 2 doctors on call → sets Alice=off-call
T2 (Bob):   reads → 2 doctors on call → sets Bob=off-call

Both transactions read the same snapshot (2 doctors on call).
Both decide it's safe to go off-call.
Both commit successfully.

Result: 0 doctors on call! ← Write skew anomaly

MVCC snapshot isolation allows this because:
  - T1 and T2 wrote to DIFFERENT rows (Alice vs Bob)
  - No write-write conflict detected
  - But the combined effect violates the invariant
```

**Solution:** Use `SELECT FOR UPDATE` to lock the rows you read:
```sql
BEGIN;
SELECT COUNT(*) FROM doctors WHERE on_call = true FOR UPDATE;
-- Now locks all on-call doctor rows
UPDATE doctors SET on_call = false WHERE name = 'Alice';
COMMIT;
```

Or use `SERIALIZABLE` isolation level (PostgreSQL SSI detects this).

---

## MVCC in MySQL InnoDB: Undo Log

MySQL implements MVCC differently from PostgreSQL:

```
PostgreSQL:
  Old versions stored IN THE HEAP (same table file)
  VACUUM cleans them up later

MySQL InnoDB:
  Current version stored in clustered index (B+ Tree)
  Old versions stored in UNDO LOG (separate file)
  Undo log entries form a version chain via pointers
```

**MySQL version chain:**

```
Clustered Index (current version):
  id=1, balance=800, DB_TRX_ID=200, DB_ROLL_PTR=→undo_log_entry_1

Undo Log:
  undo_log_entry_1: balance=1000, DB_TRX_ID=100, DB_ROLL_PTR=→undo_log_entry_2
  undo_log_entry_2: (original insert, no previous version)

To read old version:
  Follow DB_ROLL_PTR chain until finding version visible to snapshot
```

**Comparison:**

```
                  PostgreSQL          MySQL InnoDB
Old versions:     In heap file        In undo log
Cleanup:          VACUUM              Purge thread
Read old version: Scan heap           Follow undo chain
Write:            Append new tuple    Update in-place + undo entry
```

---

## VACUUM: PostgreSQL's Garbage Collector

Because PostgreSQL keeps old row versions in the heap, it needs a cleanup process:

```
After many updates:
  Heap page:
    [xmin=100, xmax=200, balance=1000]  ← dead (T200 committed)
    [xmin=200, xmax=300, balance=800]   ← dead (T300 committed)
    [xmin=300, xmax=0,   balance=600]   ← live (current)

VACUUM process:
  1. Scan heap pages
  2. Find dead tuples (xmax is committed and no active snapshot needs them)
  3. Mark space as reusable (doesn't shrink file)
  4. Update visibility map (for index-only scans)

VACUUM FULL:
  Rewrites entire table, reclaims disk space
  Locks table during operation (use carefully!)

autovacuum:
  Background daemon that runs VACUUM automatically
  Triggered when: dead tuples > threshold (default 20% of table)
```

**Transaction ID Wraparound (Critical!):**
```
PostgreSQL uses 32-bit transaction IDs (max ~4 billion)
If txid wraps around → old rows become "future" rows → data loss!

Prevention: VACUUM must run regularly to freeze old txids
  VACUUM FREEZE marks old rows as permanently visible
  (sets xmin to special "frozen" value)

Monitor with:
  SELECT age(datfrozenxid) FROM pg_database;
  -- If > 1.5 billion → urgent! Run VACUUM FREEZE
```

---

## Optimistic Concurrency Control (OCC)

An alternative to MVCC for high-contention scenarios:

```
Three phases:

1. READ phase:
   Execute transaction, track all reads and writes
   Don't acquire any locks

2. VALIDATE phase (at commit time):
   Check: did any of my reads get modified by another committed transaction?
   If YES → abort and retry
   If NO → proceed to write

3. WRITE phase:
   Apply all writes atomically

Best for: Low-contention workloads (reads >> writes)
Bad for:  High-contention (many aborts and retries waste work)
```

**Application-level OCC (common pattern):**
```sql
-- Read with version number:
SELECT id, balance, version FROM accounts WHERE id = 1;
-- Returns: balance=1000, version=5

-- Update only if version hasn't changed:
UPDATE accounts
SET balance = 800, version = version + 1
WHERE id = 1 AND version = 5;

-- Check rows affected:
-- 1 row → success (no conflict)
-- 0 rows → conflict! Someone else updated. Retry.
```

---

## 2PL vs MVCC: The Fundamental Difference

```
2PL (Two-Phase Locking):
  Writers block readers
  Readers block writers
  High contention → lots of waiting
  Deadlocks possible
  Used by: older systems, some NoSQL

MVCC:
  Writers never block readers
  Readers never block writers
  Each transaction sees consistent snapshot
  More storage needed (multiple versions)
  Cleanup needed (VACUUM / purge)
  Used by: PostgreSQL, MySQL InnoDB, Oracle, MongoDB

Why MVCC wins for OLTP:
  Web apps are read-heavy (10:1 read:write ratio)
  MVCC lets reads proceed without any locking
  → Much higher throughput
```

---

## Key Takeaways

```
MVCC core idea:
  Keep multiple versions of each row
  Each transaction sees a consistent snapshot
  Writers and readers never block each other

PostgreSQL MVCC:
  xmin/xmax hidden columns on every row
  Old versions in heap, cleaned by VACUUM
  Snapshot taken at statement start (RC) or txn start (RR)

MySQL InnoDB MVCC:
  Current version in clustered index
  Old versions in undo log (version chain)
  Purge thread cleans old undo entries

Isolation levels via MVCC:
  READ COMMITTED    → new snapshot per statement
  REPEATABLE READ   → snapshot at transaction start
  SERIALIZABLE      → snapshot + conflict detection (SSI)

MVCC weakness:
  Write skew (snapshot isolation doesn't prevent it)
  Fix: SELECT FOR UPDATE or SERIALIZABLE isolation

Storage overhead:
  PostgreSQL: dead tuples bloat heap → need VACUUM
  MySQL: undo log grows with long-running transactions
```

---

**Next Up: Module 7 — Distributed Databases & CAP Theorem**

We'll explore:
- Why single-node databases hit limits
- CAP Theorem: Consistency, Availability, Partition Tolerance
- PACELC: the more nuanced model
- How distributed databases coordinate
- Consensus algorithms: Paxos and Raft