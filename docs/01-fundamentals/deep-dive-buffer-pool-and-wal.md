# Deep Dive: Buffer Pool, WAL, and Database Storage

A database manages data across three storage components, each with a distinct role. The sections below trace how the three interact, with particular attention to the nuances of the buffer pool.

## Storage Components

A database uses three storage components:
1. **Actual DB files** (on disk)
2. **WAL** (on disk)
3. **Buffer pool** (in memory)

The roles of the WAL and the DB files are relatively straightforward; the buffer pool carries the most important nuances and is the focus of the discussion that follows.

---

## The Complete Picture: Database Storage Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CLIENT QUERIES                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    BUFFER POOL (RAM)                        │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Cached Data Pages (both committed & uncommitted!)   │  │
│  │  - Page 1: Alice balance = $400 (uncommitted)        │  │
│  │  - Page 2: Bob balance = $400 (uncommitted)          │  │
│  │  - Page 3: Charlie balance = $1000 (committed)       │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  Each page is marked with:                                  │
│  - Transaction ID (XID)                                     │
│  - Dirty bit (modified but not yet on disk)                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  WRITE-AHEAD LOG (Disk)                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  [XID: 100] BEGIN                                     │  │
│  │  [XID: 100] UPDATE accounts SET balance=400 WHERE... │  │
│  │  [XID: 100] UPDATE accounts SET balance=400 WHERE... │  │
│  │  [XID: 100] COMMIT ✓                                 │  │
│  │  [XID: 101] BEGIN                                     │  │
│  │  [XID: 101] UPDATE accounts SET balance=500 WHERE... │  │
│  │  [CRASH - No COMMIT for XID 101]                     │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              ACTUAL DATABASE FILES (Disk)                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Data Pages (eventually consistent with WAL)         │  │
│  │  - May be behind the WAL (not yet flushed)           │  │
│  │  - Updated during checkpoint process                 │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## A Key Point About the Buffer Pool

### The buffer pool holds uncommitted values

A common assumption is that uncommitted values are not present in the buffer pool. This is not the case, and it is the most important point to understand.

**Reality:** The buffer pool contains **BOTH committed AND uncommitted data**.

**Reason:** The buffer pool is where **all modifications happen first**, regardless of commit status.

**Example:**
```sql
-- Transaction 1 (not yet committed)
BEGIN;
UPDATE accounts SET balance = 400 WHERE id = 1;
-- At this point, the buffer pool ALREADY has balance = 400
-- Even though the transaction hasn't committed yet!

-- Transaction 2 (different session)
SELECT balance FROM accounts WHERE id = 1;
-- What does Transaction 2 see?
```

**The answer depends on ISOLATION LEVEL**, but the key point is:
- The buffer pool has the uncommitted value (400)
- Other transactions may or may not see it (controlled by MVCC or locks)

### Buffer Pool Contains Everything

```
Buffer Pool (In Memory)
┌─────────────────────────────────────────────────┐
│  Page 1 (Dirty, Modified by XID 100)           │
│  ┌──────────────────────────────────────────┐  │
│  │ Row 1: Alice, balance = 400              │  │
│  │        XID: 100 (uncommitted)            │  │
│  │        Old version: 500 (for MVCC)       │  │
│  └──────────────────────────────────────────┘  │
│                                                 │
│  Page 2 (Clean, Already on Disk)               │
│  ┌──────────────────────────────────────────┐  │
│  │ Row 2: Bob, balance = 300                │  │
│  │        XID: 95 (committed)               │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

**Key Points:**
1. **Dirty pages** = Modified in memory, not yet written to disk
2. **Clean pages** = Match what's on disk
3. **Both committed and uncommitted data** exist in buffer pool
4. **Transaction visibility** is controlled separately (via MVCC or locks)

---

## How Transactions See Data: MVCC (Multi-Version Concurrency Control)

MVCC is how databases handle the fact that the buffer pool holds uncommitted data.

### PostgreSQL Example (MVCC):

```
Buffer Pool State:
┌─────────────────────────────────────────────────┐
│  Row: Alice's Account                           │
│  ┌──────────────────────────────────────────┐  │
│  │ Current Version:                         │  │
│  │   balance = 400                          │  │
│  │   xmin = 100 (created by transaction 100)│  │
│  │   xmax = NULL (not deleted)              │  │
│  │                                          │  │
│  │ Old Version (kept for MVCC):            │  │
│  │   balance = 500                          │  │
│  │   xmin = 95 (created by transaction 95) │  │
│  │   xmax = 100 (deleted by transaction 100)│  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘

Transaction 101 reads Alice's balance:
- Checks: Is transaction 100 committed?
  - If YES: See balance = 400
  - If NO: See balance = 500 (old version)
```

This is why multiple versions exist in the buffer pool.

---

## The Three Storage Components in Detail

### 1. Write-Ahead Log (WAL) - Disk

**Purpose:** Durability and atomicity

**Contains:**
- All operations (INSERT, UPDATE, DELETE)
- Transaction boundaries (BEGIN, COMMIT, ROLLBACK)
- Enough information to REDO or UNDO operations

**Behavior:**
- Written to disk before data changes
- Used for recovery after crash
- Uncommitted transactions are discarded on restart
- Committed transactions are replayed if not in DB files

**Example WAL entries:**
```
LSN 1000: [XID 100] BEGIN
LSN 1001: [XID 100] UPDATE accounts SET balance=400 WHERE id=1
          Old: balance=500, New: balance=400
LSN 1002: [XID 100] UPDATE accounts SET balance=400 WHERE id=2
          Old: balance=300, New: balance=400
LSN 1003: [XID 100] COMMIT
LSN 1004: [XID 101] BEGIN
LSN 1005: [XID 101] UPDATE accounts SET balance=500 WHERE id=3
          Old: balance=450, New: balance=500
[CRASH - No COMMIT for XID 101]
```

### 2. Buffer Pool - Memory

**Purpose:** Performance (avoid disk I/O)

**Contains:**
- **Cached data pages** from disk
- **Modified (dirty) pages** not yet written to disk
- **BOTH committed AND uncommitted data**
- **Multiple versions** of rows (for MVCC)

**Note:** Contrary to a common assumption, uncommitted values **are** present in the buffer pool.

**Contents in detail:**
```
Buffer Pool contains:
✅ Committed data (visible to all transactions)
✅ Uncommitted data (visible only to the transaction that made changes)
✅ Old versions (for MVCC - so other transactions see consistent data)
✅ Dirty pages (modified but not yet on disk)
✅ Clean pages (match what's on disk)
```

**How visibility is controlled:**
- **MVCC (PostgreSQL, MySQL InnoDB):** Multiple versions, each transaction sees appropriate version
- **Locking (older systems):** Locks prevent other transactions from reading uncommitted data

### 3. Database Files - Disk

**Purpose:** Persistent storage

**Contains:**
- The "source of truth" data
- Eventually consistent with WAL
- May lag behind WAL (updated during checkpoints)

**Behavior:**
- Once data is here, it is durable
- Updated asynchronously from buffer pool
- May not have latest committed changes immediately after commit

---

## Complete Transaction Flow

The following traces a transaction through all three storage layers:

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

### Step-by-Step:

**1. BEGIN Transaction (XID 100)**
```
WAL: [XID 100] BEGIN
Buffer Pool: No changes yet
DB Files: No changes yet
```

**2. First UPDATE**
```
WAL: [XID 100] UPDATE accounts id=1, old=500, new=400
Buffer Pool: 
  - Load page containing id=1 (if not already cached)
  - Modify balance: 500 → 400
  - Mark page as DIRTY
  - Mark with XID 100 (uncommitted)
  - Keep old version (500) for MVCC
DB Files: No changes yet (still 500)
```

**3. Second UPDATE**
```
WAL: [XID 100] UPDATE accounts id=2, old=300, new=400
Buffer Pool:
  - Load page containing id=2
  - Modify balance: 300 → 400
  - Mark page as DIRTY
  - Mark with XID 100 (uncommitted)
  - Keep old version (300) for MVCC
DB Files: No changes yet (still 300)
```

**4. COMMIT**
```
WAL: 
  - [XID 100] COMMIT
  - Flush WAL to disk (fsync) ← CRITICAL POINT
  - Return SUCCESS to client
  
Buffer Pool:
  - Mark XID 100 as committed
  - Pages still DIRTY (not yet written to disk)
  - Now visible to other transactions
  
DB Files: No changes yet!
```

**5. Later: Checkpoint Process**
```
Background process writes dirty pages to disk:

Buffer Pool → DB Files
  - Write page with id=1 (balance=400)
  - Write page with id=2 (balance=400)
  - Mark pages as CLEAN
  
Now DB Files match Buffer Pool
```

---

## What Happens During a Crash?

### Scenario 1: Crash BEFORE COMMIT

```
State at crash:
WAL: [XID 100] BEGIN, UPDATE, UPDATE [NO COMMIT]
Buffer Pool: Has uncommitted changes (lost - it's in RAM!)
DB Files: Old values (500, 300)

Recovery:
1. Read WAL
2. See no COMMIT for XID 100
3. Discard all XID 100 operations
4. Result: Database has (500, 300) ✓
```

### Scenario 2: Crash AFTER COMMIT, BEFORE Checkpoint

```
State at crash:
WAL: [XID 100] BEGIN, UPDATE, UPDATE, COMMIT ✓
Buffer Pool: Has committed changes (lost - it's in RAM!)
DB Files: Old values (500, 300)

Recovery:
1. Read WAL
2. See COMMIT for XID 100
3. REDO all XID 100 operations
4. Apply: 500→400, 300→400
5. Result: Database has (400, 400) ✓
```

### Scenario 3: Crash AFTER Checkpoint

```
State at crash:
WAL: [XID 100] BEGIN, UPDATE, UPDATE, COMMIT ✓
Buffer Pool: Lost (RAM)
DB Files: New values (400, 400) ✓

Recovery:
1. Read WAL
2. See COMMIT for XID 100
3. Check DB Files - already have correct values
4. No action needed
5. Result: Database has (400, 400) ✓
```

---

## Key Points Summary

### Uncommitted values in the buffer pool

Uncommitted values **are** present in the buffer pool; it is where ALL modifications happen first.

**How other transactions avoid seeing uncommitted data:**
- **MVCC:** Multiple versions exist; each transaction sees the appropriate version
- **Locking:** Locks prevent reading uncommitted data
- **Transaction IDs:** Each row is tagged with the transaction that modified it

### WAL contents

The WAL contains operations and commit information, and it is the source of truth for recovery.

### DB file durability

DB files are durable once persisted. Note, however, that a commit is considered successful once the WAL is on disk, even if the DB files are not updated yet.

---

## Visual Summary

```
Transaction Lifecycle:

1. BEGIN
   WAL: ✓ (BEGIN record)
   Buffer Pool: No changes
   DB Files: No changes

2. MODIFY DATA
   WAL: ✓ (Operation records)
   Buffer Pool: ✓ (Modified, DIRTY, UNCOMMITTED)
   DB Files: ✗ (Still old values)

3. COMMIT
   WAL: ✓ (COMMIT record, flushed to disk)
   Buffer Pool: ✓ (Modified, DIRTY, COMMITTED)
   DB Files: ✗ (Still old values)
   → Client receives SUCCESS

4. CHECKPOINT (later)
   WAL: ✓ (Already there)
   Buffer Pool: ✓ (Modified, CLEAN, COMMITTED)
   DB Files: ✓ (Finally updated!)
```

---

## Review Questions

**Question 1:** If a transaction modifies a row but has not committed yet, is that modification in the buffer pool?
**Answer:** Yes. It is in the buffer pool, marked with the transaction ID and uncommitted status.

**Question 2:** How can another transaction read the old value if the buffer pool has the new (uncommitted) value?
**Answer:** MVCC keeps multiple versions, or locks prevent reading until commit.

**Question 3:** Why is a transaction considered committed once the WAL is flushed, even if DB files are not updated?
**Answer:** Because the WAL can be replayed after a crash to reconstruct the committed state.

---

The key insight is that the buffer pool is a **working area** that contains everything (committed and uncommitted), and visibility is controlled separately through MVCC or locking mechanisms.