# Deep Dive: The Commit Guarantee - What If the WAL Isn't Flushed?

This topic gets to the heart of database durability guarantees.

## The Question

> What if a transaction commits but the WAL is not flushed to disk, and the database crashes — does the client end up holding an uncommitted value?

**Short Answer:** This scenario **cannot happen** in a properly implemented database.

**Reason:** The database **never** returns "commit successful" to the client until the WAL is safely on disk.

---

## The Commit Protocol

All ACID-compliant databases follow this rule:

```
┌─────────────────────────────────────────────────────────────┐
│  THE COMMIT GUARANTEE                                       │
│                                                             │
│  A database MUST NOT return "COMMIT SUCCESS" to the client │
│  until the WAL has been durably written to disk (fsync).   │
│                                                             │
│  If the database returns success, the transaction MUST     │
│  survive any subsequent crash.                             │
└─────────────────────────────────────────────────────────────┘
```

This is called **durability** - the "D" in ACID.

---

## What Actually Happens During COMMIT

The exact sequence is as follows:

```sql
BEGIN;
UPDATE accounts SET balance = 400 WHERE id = 1;
UPDATE accounts SET balance = 500 WHERE id = 2;
COMMIT;  -- What happens here?
```

### Step-by-Step COMMIT Process:

```
Client sends: COMMIT
       ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 1: Write COMMIT record to WAL buffer (in memory)      │
│         [XID 100] COMMIT                                    │
└─────────────────────────────────────────────────────────────┘
       ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 2: Flush WAL buffer to disk (fsync)                   │
│         ⚠️  BLOCKING OPERATION - Database WAITS here!      │
│         ⚠️  This can take milliseconds!                     │
│                                                             │
│         fsync() ensures:                                    │
│         - Data written to disk platters (not just cache)   │
│         - Survives power loss                              │
│         - Survives OS crash                                │
└─────────────────────────────────────────────────────────────┘
       ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 3: ONLY NOW - Return SUCCESS to client                │
│         Client receives: "COMMIT SUCCESSFUL"                │
└─────────────────────────────────────────────────────────────┘
       ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 4: (Later) Flush dirty pages from buffer pool to disk │
│         This can happen minutes later!                      │
└─────────────────────────────────────────────────────────────┘
```

**Key Point:** The database **blocks** at Step 2 until fsync() completes. The client does not receive success until the WAL is safely on disk.

---

## The fsync() System Call

```c
// Pseudocode of what happens inside the database
int commit_transaction(Transaction* txn) {
    // 1. Write COMMIT record to WAL buffer
    wal_buffer_append(txn->id, "COMMIT");
    
    // 2. Flush to disk - THIS IS THE CRITICAL STEP
    int result = fsync(wal_file_descriptor);
    
    if (result != SUCCESS) {
        // Disk error! Cannot guarantee durability
        return ERROR_CANNOT_COMMIT;
    }
    
    // 3. ONLY if fsync succeeded, return success
    return COMMIT_SUCCESS;
}
```

**What fsync() does:**
- Tells the operating system: "Write everything to physical disk NOW"
- Bypasses OS disk cache
- Waits for disk controller to confirm write
- Returns only when data is on physical platters

This is why commits can be slow: each commit requires a disk write.

---

## Scenario Analysis: Can the Client Get Wrong Information?

The possible scenarios are examined below.

### Scenario 1: Crash BEFORE fsync() completes

```
Timeline:
T1: Client sends COMMIT
T2: Database writes COMMIT to WAL buffer (memory)
T3: Database calls fsync()
T4: [CRASH!] - fsync() hasn't completed yet
T5: Client is still waiting for response...

State after crash:
- WAL on disk: [BEGIN, UPDATE, UPDATE] - NO COMMIT record!
- Client: Never received success (connection dropped)

Recovery:
- Database sees no COMMIT record in WAL
- Rolls back transaction
- Client knows commit failed (connection error)

Result: ✅ CORRECT - Client never got false success
```

### Scenario 2: Crash AFTER fsync() completes, BEFORE client receives response

```
Timeline:
T1: Client sends COMMIT
T2: Database writes COMMIT to WAL buffer
T3: Database calls fsync()
T4: fsync() completes successfully ✓
T5: [CRASH!] - Before sending response to client
T6: Client connection drops

State after crash:
- WAL on disk: [BEGIN, UPDATE, UPDATE, COMMIT] ✓
- Client: Doesn't know if commit succeeded (connection error)

Recovery:
- Database sees COMMIT record in WAL
- Replays transaction
- Data is committed ✓

Result: ✅ CORRECT - Transaction is durable
        ⚠️  Client is uncertain (must retry or check)
```

### Scenario 3: Client receives success, THEN crash

```
Timeline:
T1: Client sends COMMIT
T2: Database writes COMMIT to WAL buffer
T3: Database calls fsync()
T4: fsync() completes successfully ✓
T5: Database sends "SUCCESS" to client
T6: Client receives "SUCCESS" ✓
T7: [CRASH!]

State after crash:
- WAL on disk: [BEGIN, UPDATE, UPDATE, COMMIT] ✓
- Client: Knows commit succeeded ✓

Recovery:
- Database sees COMMIT record in WAL
- Replays transaction
- Data is committed ✓

Result: ✅ CORRECT - Everything consistent
```

### ❌ Scenario 4: The Concern - Can This Happen?

```
Timeline:
T1: Client sends COMMIT
T2: Database returns "SUCCESS" to client ❌
T3: Database tries to fsync()
T4: [CRASH!] - Before fsync() completes

The concern: Client has success, but WAL not on disk!
```

**Answer: This cannot happen in a correct database implementation.**

**Reason:** Step 2 and Step 3 are in the wrong order. A correct database must:
1. fsync() first
2. Return success second

A database that returned success before fsync() would be **violating the ACID durability guarantee** and would be considered buggy.

---

## Real-World Example: PostgreSQL's Commit Process

The actual PostgreSQL code flow (simplified) looks like this:

```c
// PostgreSQL commit process (simplified)
bool CommitTransaction(void) {
    TransactionId xid = GetCurrentTransactionId();
    
    // 1. Write COMMIT record to WAL buffer
    XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT);
    
    // 2. Force WAL to disk - BLOCKS HERE!
    XLogFlush(XactLastRecEnd);  // This calls fsync()
    
    // 3. Only after flush succeeds, mark as committed
    ProcArrayEndTransaction(MyProc, xid);
    
    // 4. Return success to client
    return true;
}
```

**Key observation:** `XLogFlush()` (which does fsync) happens BEFORE returning true.

---

## Performance Implications

This guarantee has performance costs:

### The Cost of Durability

```
Without fsync (DANGEROUS):
- Commit time: ~0.01ms (just memory write)
- Throughput: 100,000 commits/second

With fsync (SAFE):
- Commit time: ~5-10ms (disk write)
- Throughput: ~100-200 commits/second
```

This is why commits are slow: each commit requires a physical disk write.

### Optimization: Group Commit

Databases optimize this with **group commit**:

```
Multiple transactions waiting to commit:
┌─────────────────────────────────────────┐
│ Transaction 100: Waiting for fsync      │
│ Transaction 101: Waiting for fsync      │
│ Transaction 102: Waiting for fsync      │
│ Transaction 103: Waiting for fsync      │
└─────────────────────────────────────────┘
       ↓
Database batches them:
┌─────────────────────────────────────────┐
│ Single fsync() for all 4 transactions!  │
│ [COMMIT 100, COMMIT 101, COMMIT 102,    │
│  COMMIT 103]                            │
└─────────────────────────────────────────┘
       ↓
All 4 clients receive success together
```

**Benefit:** Amortize the cost of fsync across multiple transactions.

---

## Configuration Options (Trade-offs)

Some databases allow durability to be weakened for performance:

### PostgreSQL: synchronous_commit

```sql
-- Default: Full durability (safe)
SET synchronous_commit = on;  -- fsync before returning success

-- Faster but risky: Async commit
SET synchronous_commit = off;  -- Return success immediately
                               -- fsync happens later
```

**With `synchronous_commit = off`:**
```
Timeline:
T1: Client sends COMMIT
T2: Database returns "SUCCESS" immediately ⚠️
T3: Database writes to WAL buffer
T4: [CRASH!] - Before fsync()

Result: ❌ Client thinks commit succeeded, but data is lost
```

**When to use async commit:**
- Non-critical data (logs, analytics)
- Can tolerate losing last few seconds of data
- Need maximum throughput

**When NOT to use:**
- Financial transactions
- User data
- Anything where data loss is unacceptable

### MySQL: innodb_flush_log_at_trx_commit

```sql
-- Full durability (default)
SET GLOBAL innodb_flush_log_at_trx_commit = 1;  -- fsync on every commit

-- Flush once per second
SET GLOBAL innodb_flush_log_at_trx_commit = 2;  -- Can lose 1 second of data

-- No fsync (dangerous!)
SET GLOBAL innodb_flush_log_at_trx_commit = 0;  -- Can lose data
```

---

## The Durability Guarantee: Summary

```
┌─────────────────────────────────────────────────────────────┐
│  ACID Durability Guarantee:                                 │
│                                                             │
│  IF client receives "COMMIT SUCCESS"                        │
│  THEN data MUST survive any subsequent crash               │
│                                                             │
│  This is achieved by:                                       │
│  1. Writing COMMIT to WAL                                   │
│  2. Calling fsync() to flush to disk                        │
│  3. ONLY THEN returning success to client                   │
│                                                             │
│  The database BLOCKS during fsync() - this is intentional   │
└─────────────────────────────────────────────────────────────┘
```

---

## What About Network Failures?

There is one edge case to consider:

```
Timeline:
T1: Database commits successfully (fsync done)
T2: Database sends "SUCCESS" to client
T3: [NETWORK FAILURE] - Message lost
T4: Client never receives response

State:
- Database: Transaction committed ✓
- Client: Uncertain (timeout error)
```

**This is called "uncertain commit":**
- Transaction IS committed in database
- Client doesn't know
- Client must either:
  - Retry (might duplicate if using auto-increment IDs)
  - Query to check if transaction succeeded
  - Use idempotency tokens

**This is not a database bug** - it is an inherent distributed systems problem.

---

## Key Takeaways

1. **Database NEVER returns success before WAL is on disk**
   - This is the durability guarantee
   - Enforced by fsync() system call

2. **If client receives success, data WILL survive crash**
   - WAL is already on disk
   - Can be replayed during recovery

3. **If crash happens before client receives response**
   - Transaction might be committed (if fsync completed)
   - Client is uncertain (must handle this case)

4. **Commits are slow because of fsync()**
   - Typical: 5-10ms per commit
   - Optimized with group commit
   - Can be weakened for performance (risky)

5. **The concern scenario cannot happen in a correct implementation**
   - Client getting success with WAL not flushed = bug
   - Would violate ACID durability guarantee

---

## Review Questions

**Question 1:** Why does the database block during fsync() instead of returning success immediately?
**Answer:** To guarantee durability - if it returned success before fsync(), a crash could lose committed data.

**Question 2:** What happens if fsync() fails (disk error)?
**Answer:** The database returns an error to the client - the commit fails. Failing is preferable to misreporting durability.

**Question 3:** Can a transaction be committed in the database without the client knowing?
**Answer:** Yes, if the network fails after fsync() but before the client receives the response. This is an "uncertain commit."

---

The key insight is that **durability is enforced by the order of operations** - fsync() must complete before returning success to the client.