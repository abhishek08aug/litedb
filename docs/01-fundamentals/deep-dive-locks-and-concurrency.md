# Deep Dive: Database Locks and Concurrency Control

Excellent question! This is one of the most critical mechanisms in databases. Let me explain how locks prevent concurrent transactions from corrupting data.

## Your Question

> "How do locks work in databases to ensure no two transactions can change the same row?"

**Short Answer:** Databases use **locks** to control access to data. When a transaction wants to modify a row, it acquires a lock on that row, preventing other transactions from modifying it until the lock is released.

But there's much more to it! Let's dive deep.

---

## The Problem: Concurrent Transactions

Without locks, concurrent transactions can cause serious problems:

### Problem 1: Lost Update

```
Initial state: balance = 500

Transaction 1:                Transaction 2:
READ balance (500)            
                              READ balance (500)
balance = 500 + 100 = 600     
                              balance = 500 - 50 = 450
WRITE balance = 600           
                              WRITE balance = 450
COMMIT                        
                              COMMIT

Final state: balance = 450 ❌
Transaction 1's update is LOST!
```

### Problem 2: Dirty Read

```
Transaction 1:                Transaction 2:
BEGIN                         BEGIN
UPDATE balance = 600          
                              READ balance (600) ← Reads uncommitted data!
ROLLBACK (undo to 500)        
                              Uses 600 in calculation ❌
                              COMMIT

Transaction 2 read data that was never committed!
```

### Problem 3: Non-Repeatable Read

```
Transaction 1:                Transaction 2:
BEGIN                         BEGIN
READ balance (500)            
                              UPDATE balance = 600
                              COMMIT
READ balance (600) ← Different!
COMMIT

Same query returns different results within one transaction!
```

**Locks solve all these problems!**

---

## Lock Types: The Building Blocks

Databases use different types of locks:

### 1. Shared Lock (S-Lock) - Read Lock

```
Purpose: Allow multiple readers, block writers

Transaction 1: SELECT * FROM accounts WHERE id = 1;
               ↓
               Acquires SHARED lock on row 1
               ↓
Transaction 2: SELECT * FROM accounts WHERE id = 1;
               ↓
               Also acquires SHARED lock ✓ (allowed!)
               ↓
Transaction 3: UPDATE accounts SET balance = 600 WHERE id = 1;
               ↓
               Tries to acquire EXCLUSIVE lock
               ↓
               BLOCKED! ⏳ (waits for shared locks to release)
```

**Rule:** Multiple transactions can hold shared locks simultaneously, but no exclusive lock can be acquired.

### 2. Exclusive Lock (X-Lock) - Write Lock

```
Purpose: Block all other access (readers and writers)

Transaction 1: UPDATE accounts SET balance = 600 WHERE id = 1;
               ↓
               Acquires EXCLUSIVE lock on row 1
               ↓
Transaction 2: SELECT * FROM accounts WHERE id = 1;
               ↓
               Tries to acquire SHARED lock
               ↓
               BLOCKED! ⏳ (waits for exclusive lock to release)
               ↓
Transaction 3: UPDATE accounts SET balance = 700 WHERE id = 1;
               ↓
               Tries to acquire EXCLUSIVE lock
               ↓
               BLOCKED! ⏳ (waits for exclusive lock to release)
```

**Rule:** Only one transaction can hold an exclusive lock, and no other locks (shared or exclusive) can be acquired.

### Lock Compatibility Matrix

```
┌─────────────┬─────────────┬─────────────┐
│             │ Shared (S)  │ Exclusive(X)│
├─────────────┼─────────────┼─────────────┤
│ Shared (S)  │     ✓       │      ✗      │
├─────────────┼─────────────┼─────────────┤
│ Exclusive(X)│     ✗       │      ✗      │
└─────────────┴─────────────┴─────────────┘

✓ = Compatible (can coexist)
✗ = Incompatible (must wait)
```

---

## Lock Granularity: What Gets Locked?

Databases can lock at different levels:

### 1. Row-Level Locks (Most Common)

```
Table: accounts
┌────┬─────────┬─────────┐
│ ID │  Name   │ Balance │
├────┼─────────┼─────────┤
│ 1  │ Alice   │ 500     │ ← Transaction 1 locks this row
├────┼─────────┼─────────┤
│ 2  │ Bob     │ 300     │ ← Transaction 2 can lock this row
├────┼─────────┼─────────┤
│ 3  │ Charlie │ 1000    │ ← Transaction 3 can lock this row
└────┴─────────┴─────────┘

Fine-grained: High concurrency!
```

**Pros:**
- ✅ High concurrency (different rows can be modified simultaneously)
- ✅ Minimal blocking

**Cons:**
- ❌ More memory overhead (one lock per row)
- ❌ More complex to manage

### 2. Page-Level Locks

```
Table: accounts (stored in pages)
┌─────────────────────────────────┐
│ Page 1 (8KB)                    │ ← Transaction 1 locks entire page
│ ├─ Row 1: Alice, 500            │
│ ├─ Row 2: Bob, 300              │
│ └─ Row 3: Charlie, 1000         │
├─────────────────────────────────┤
│ Page 2 (8KB)                    │ ← Transaction 2 can lock this page
│ ├─ Row 4: David, 750            │
│ └─ Row 5: Eve, 900              │
└─────────────────────────────────┘

Medium-grained: Balance between concurrency and overhead
```

### 3. Table-Level Locks

```
Table: accounts
┌─────────────────────────────────┐
│ Entire table locked!            │ ← Transaction 1 locks whole table
│ All rows blocked for others     │
└─────────────────────────────────┘

Coarse-grained: Low concurrency, low overhead
```

**When used:**
- DDL operations (ALTER TABLE, DROP TABLE)
- Bulk operations (TRUNCATE)
- Some NoSQL databases (simpler implementation)

---

## How Locks Work: Step-by-Step

Let's trace two concurrent transactions:

### Scenario: Two Transactions Updating Different Rows

```
Transaction 1:                          Transaction 2:
BEGIN;                                  BEGIN;
UPDATE accounts                         UPDATE accounts
SET balance = 600                       SET balance = 400
WHERE id = 1;                           WHERE id = 2;
  ↓                                       ↓
Acquire X-lock on row 1 ✓               Acquire X-lock on row 2 ✓
  ↓                                       ↓
Modify row 1 in buffer pool             Modify row 2 in buffer pool
  ↓                                       ↓
COMMIT;                                 COMMIT;
  ↓                                       ↓
Release X-lock on row 1 ✓               Release X-lock on row 2 ✓

Result: Both succeed! No conflict.
```

### Scenario: Two Transactions Updating Same Row

```
Transaction 1:                          Transaction 2:
BEGIN;                                  BEGIN;
UPDATE accounts                         UPDATE accounts
SET balance = 600                       SET balance = 700
WHERE id = 1;                           WHERE id = 1;
  ↓                                       ↓
Acquire X-lock on row 1 ✓               Try to acquire X-lock on row 1
  ↓                                       ↓
Modify row 1 in buffer pool             BLOCKED! ⏳ (waiting for lock)
  ↓                                       ↓
... working ...                         ... waiting ...
  ↓                                       ↓
COMMIT;                                 ... still waiting ...
  ↓                                       ↓
Release X-lock on row 1 ✓               Lock acquired! ✓
                                          ↓
                                        Modify row 1 (sees value 600)
                                          ↓
                                        COMMIT;
                                          ↓
                                        Release X-lock on row 1 ✓

Result: Serialized execution - Transaction 2 waits for Transaction 1
```

---

## Lock Implementation: The Lock Manager

Every database has a **Lock Manager** component:

```
┌─────────────────────────────────────────────────────────────┐
│                    LOCK MANAGER                             │
│                                                             │
│  Lock Table (Hash Table):                                  │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ Resource ID │ Lock Type │ Transaction ID │ Status   │  │
│  ├─────────────┼───────────┼────────────────┼──────────┤  │
│  │ Row 1       │ X-Lock    │ TXN-100        │ GRANTED  │  │
│  │ Row 2       │ S-Lock    │ TXN-101        │ GRANTED  │  │
│  │ Row 2       │ S-Lock    │ TXN-102        │ GRANTED  │  │
│  │ Row 1       │ X-Lock    │ TXN-103        │ WAITING  │  │
│  └─────────────┴───────────┴────────────────┴──────────┘  │
│                                                             │
│  Wait Queue:                                                │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ TXN-103 waiting for Row 1 (held by TXN-100)        │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Lock Manager Operations:

```python
class LockManager:
    def __init__(self):
        self.lock_table = {}  # {resource_id: [Lock objects]}
        self.wait_queue = {}  # {transaction_id: waiting_for}
    
    def acquire_lock(self, txn_id, resource_id, lock_type):
        # Check if lock can be granted
        if self.is_compatible(resource_id, lock_type):
            # Grant lock immediately
            self.lock_table[resource_id].append(
                Lock(txn_id, lock_type, status='GRANTED')
            )
            return True
        else:
            # Add to wait queue
            self.wait_queue[txn_id] = resource_id
            self.lock_table[resource_id].append(
                Lock(txn_id, lock_type, status='WAITING')
            )
            # Block transaction (wait)
            return False
    
    def release_lock(self, txn_id, resource_id):
        # Remove lock from table
        self.lock_table[resource_id].remove_by_txn(txn_id)
        
        # Wake up waiting transactions
        self.wake_up_waiters(resource_id)
    
    def is_compatible(self, resource_id, lock_type):
        existing_locks = self.lock_table.get(resource_id, [])
        
        for lock in existing_locks:
            if not self.compatible(lock.type, lock_type):
                return False
        
        return True
    
    def compatible(self, lock1, lock2):
        # Compatibility matrix
        if lock1 == 'S' and lock2 == 'S':
            return True  # Shared locks are compatible
        return False  # All other combinations incompatible
```

---

## Deadlocks: When Locks Go Wrong

**Deadlock** occurs when transactions wait for each other in a cycle:

### Classic Deadlock Example:

```
Transaction 1:                          Transaction 2:
BEGIN;                                  BEGIN;
UPDATE accounts                         UPDATE accounts
SET balance = 600                       SET balance = 400
WHERE id = 1;                           WHERE id = 2;
  ↓                                       ↓
Acquire X-lock on row 1 ✓               Acquire X-lock on row 2 ✓
  ↓                                       ↓
UPDATE accounts                         UPDATE accounts
SET balance = 700                       SET balance = 800
WHERE id = 2;                           WHERE id = 1;
  ↓                                       ↓
Try to acquire X-lock on row 2          Try to acquire X-lock on row 1
  ↓                                       ↓
BLOCKED! (waiting for TXN-2)            BLOCKED! (waiting for TXN-1)
  ↓                                       ↓
⏳ Waiting forever...                    ⏳ Waiting forever...

DEADLOCK! 💀
```

**Wait-for graph:**
```
TXN-1 → (waiting for) → TXN-2
  ↑                        ↓
  └────────────────────────┘
  
Cycle detected = Deadlock!
```

### Deadlock Detection and Resolution:

```
Deadlock Detector (runs periodically):
1. Build wait-for graph
2. Detect cycles
3. Choose a victim transaction
4. Abort victim (ROLLBACK)
5. Release its locks
6. Other transaction can proceed

Example:
- Detect: TXN-1 and TXN-2 in deadlock
- Choose victim: TXN-2 (younger transaction)
- Abort TXN-2
- TXN-1 acquires lock on row 2 and completes
- TXN-2 gets error: "Deadlock detected, transaction aborted"
- Application must retry TXN-2
```

### Deadlock Prevention Strategies:

**1. Lock Ordering:**
```
Rule: Always acquire locks in the same order

Transaction 1:                          Transaction 2:
Lock row 1 first                        Lock row 1 first (waits)
Lock row 2 second                       Lock row 2 second

No deadlock! ✓
```

**2. Timeout:**
```
If transaction waits > 30 seconds:
  Abort transaction
  Return error to client
  Client retries
```

**3. Wait-Die / Wound-Wait Schemes:**
```
Wait-Die: Older transactions wait, younger die
Wound-Wait: Older transactions force younger to abort
```

---

## Two-Phase Locking (2PL): The Protocol

To ensure **serializability** (transactions appear to execute one at a time), databases use **Two-Phase Locking**:

### The Two Phases:

```
Phase 1: GROWING PHASE
- Transaction can acquire locks
- Transaction CANNOT release locks

Phase 2: SHRINKING PHASE
- Transaction can release locks
- Transaction CANNOT acquire new locks
```

### Example:

```
Transaction:
BEGIN;
  ↓
SELECT * FROM accounts WHERE id = 1;  ← Acquire S-lock on row 1
  ↓ (Growing phase)
UPDATE accounts SET balance = 600 WHERE id = 1;  ← Upgrade to X-lock
  ↓ (Still growing)
UPDATE accounts SET balance = 700 WHERE id = 2;  ← Acquire X-lock on row 2
  ↓ (Still growing)
COMMIT;  ← Release ALL locks at once
  ↓ (Shrinking phase)
All locks released ✓
```

**Why 2PL works:**
- Ensures serializability
- Prevents cascading rollbacks
- Standard in most databases

**Strict 2PL (most common):**
- Hold ALL locks until COMMIT or ROLLBACK
- Release all locks at once
- Prevents dirty reads

---

## Lock Escalation: Managing Lock Overhead

When too many row locks are acquired, databases **escalate** to coarser locks:

```
Scenario: Transaction updates 10,000 rows

Initial: 10,000 row-level X-locks
  ↓
Lock Manager: "Too many locks! Memory overhead high!"
  ↓
Escalation: Convert to 1 table-level X-lock
  ↓
Result: Less memory, but lower concurrency
```

**PostgreSQL Example:**
```sql
-- Check lock escalation settings
SHOW max_locks_per_transaction;  -- Default: 64

-- If transaction exceeds limit, escalation occurs
```

---

## Modern Alternative: MVCC (Multi-Version Concurrency Control)

Many modern databases use **MVCC** instead of (or in addition to) locks:

### How MVCC Works:

```
Instead of locking, keep multiple versions of each row!

Row versions:
┌─────────────────────────────────────────────────────────────┐
│ Row 1, Version 1: balance = 500, created by TXN-100        │
│ Row 1, Version 2: balance = 600, created by TXN-101        │
│ Row 1, Version 3: balance = 700, created by TXN-102        │
└─────────────────────────────────────────────────────────────┘

Transaction reads appropriate version based on:
- Transaction start time
- Isolation level
- Visibility rules
```

**Benefits:**
- ✅ Readers don't block writers
- ✅ Writers don't block readers
- ✅ Higher concurrency

**Used by:** PostgreSQL, MySQL InnoDB, Oracle, SQL Server

**We'll cover MVCC in detail in Module 6!**

---

## Practical Examples

### Example 1: Bank Transfer (Correct Locking)

```sql
-- Transaction 1: Transfer $100 from Alice to Bob
BEGIN;

-- Acquire X-lock on Alice's row
UPDATE accounts 
SET balance = balance - 100 
WHERE name = 'Alice';

-- Acquire X-lock on Bob's row
UPDATE accounts 
SET balance = balance + 100 
WHERE name = 'Bob';

COMMIT;  -- Release all locks

-- No other transaction can modify these rows until commit!
```

### Example 2: SELECT FOR UPDATE (Explicit Locking)

```sql
-- Transaction 1: Reserve a seat
BEGIN;

-- Explicitly acquire X-lock (even though it's a SELECT!)
SELECT * FROM seats 
WHERE seat_number = 'A1' 
FOR UPDATE;

-- Check if seat is available
-- If yes, book it
UPDATE seats 
SET status = 'BOOKED', user_id = 123 
WHERE seat_number = 'A1';

COMMIT;

-- Other transactions trying to book A1 will wait!
```

### Example 3: Lock Timeout

```sql
-- Set lock timeout
SET lock_timeout = '5s';

BEGIN;
UPDATE accounts SET balance = 600 WHERE id = 1;
-- If lock not acquired within 5 seconds:
-- ERROR: lock timeout exceeded
ROLLBACK;
```

---

## Key Takeaways

### How Locks Prevent Concurrent Modifications:

1. **Transaction acquires lock** before modifying data
2. **Lock Manager** checks compatibility
3. **Incompatible locks** cause transaction to wait
4. **Lock released** on COMMIT or ROLLBACK
5. **Waiting transactions** wake up and proceed

### Lock Types:
- **Shared (S)**: Multiple readers allowed
- **Exclusive (X)**: Only one writer, no readers

### Lock Granularity:
- **Row-level**: High concurrency, more overhead
- **Page-level**: Medium concurrency
- **Table-level**: Low concurrency, less overhead

### Problems Locks Solve:
- ✅ Lost updates
- ✅ Dirty reads
- ✅ Non-repeatable reads
- ✅ Phantom reads (with range locks)

### Problems Locks Create:
- ❌ Deadlocks (solved by detection/prevention)
- ❌ Reduced concurrency (solved by MVCC)
- ❌ Lock contention (solved by better design)

---

## Test Your Understanding

**Question 1:** Can two transactions hold shared locks on the same row simultaneously?
**Answer:** YES! Shared locks are compatible with each other.

**Question 2:** What happens if Transaction A holds an exclusive lock and Transaction B tries to acquire a shared lock on the same row?
**Answer:** Transaction B blocks (waits) until Transaction A releases the exclusive lock.

**Question 3:** How does a database detect deadlocks?
**Answer:** By building a wait-for graph and detecting cycles. When a cycle is found, one transaction is aborted.

---

This is a fundamental concept! We'll explore this more in Module 6 (Concurrency Control) where we'll cover MVCC, isolation levels, and advanced locking strategies. Ready for more questions?