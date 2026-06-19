# Deep Dive: How Databases Support Atomicity

Atomicity is one of the most critical properties of databases. The sections below describe how it works under the hood.

## What is Atomicity?

**Atomicity** means a transaction is "all or nothing":
- Either **all operations** in a transaction succeed, or
- **None of them** take effect

A bank transfer illustrates the concept:
```
Transaction: Transfer $100 from Alice to Bob
  Step 1: Deduct $100 from Alice's account
  Step 2: Add $100 to Bob's account
```

**Without atomicity:** If the system crashes after Step 1, Alice loses $100 but Bob does not receive it.

**With atomicity:** Either both steps complete, or neither does. No money disappears.

---

## How Relational Databases Implement Atomicity

Relational databases use several mechanisms to ensure atomicity:

### 1. Write-Ahead Logging (WAL)

This is the **most fundamental technique** for atomicity.

**How it works:**

```
Step-by-Step Process:
┌─────────────────────────────────────────────────────────┐
│ 1. Transaction Begins                                   │
│    BEGIN TRANSACTION;                                   │
└─────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────┐
│ 2. Write to WAL (Log) FIRST - before changing data     │
│    Log Entry: "Deduct $100 from Alice (Account #123)"  │
│    Log Entry: "Add $100 to Bob (Account #456)"         │
│    ✓ Flushed to disk (durable)                         │
└─────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────┐
│ 3. Modify Data in Memory (Buffer Pool)                 │
│    Alice's balance: $500 → $400                         │
│    Bob's balance: $300 → $400                           │
│    (Not yet written to disk!)                           │
└─────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────┐
│ 4. COMMIT                                               │
│    Write "COMMIT" record to WAL                         │
│    ✓ Flushed to disk                                   │
└─────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────┐
│ 5. Eventually write data pages to disk                  │
│    (Can happen later, asynchronously)                   │
└─────────────────────────────────────────────────────────┘
```

**Key Principle:** "Log before data" - Always write to the log before modifying actual data.

**Why this ensures atomicity:**

**Scenario 1: Crash BEFORE commit**
```
WAL Log:
  [START TRANSACTION]
  [Deduct $100 from Alice]
  [Add $100 to Bob]
  [CRASH! - No COMMIT record]

Recovery Process:
  - Database sees no COMMIT record
  - ROLLBACK: Undo all changes
  - Result: Transaction never happened ✓
```

**Scenario 2: Crash AFTER commit**
```
WAL Log:
  [START TRANSACTION]
  [Deduct $100 from Alice]
  [Add $100 to Bob]
  [COMMIT] ✓
  [CRASH! - Data pages not yet written to disk]

Recovery Process:
  - Database sees COMMIT record
  - REDO: Replay all operations from log
  - Result: Transaction is completed ✓
```

### 2. Shadow Paging (Alternative Approach)

Some databases use **shadow paging** instead of WAL:

```
Original Data Page          Shadow (Copy) Page
┌──────────────┐           ┌──────────────┐
│ Alice: $500  │           │ Alice: $400  │ ← Modified copy
│ Bob:   $300  │           │ Bob:   $400  │
└──────────────┘           └──────────────┘
       ↑                          ↑
       │                          │
   Current Page              Shadow Page
   Pointer                   (being modified)
```

**Process:**
1. Create a copy of the data page
2. Modify the copy
3. On COMMIT: Atomically switch pointer to the new page
4. On ROLLBACK: Discard the copy

**Atomic pointer switch:**
```c
// This is an atomic operation at the OS level
current_page_pointer = shadow_page_pointer;
```

### 3. UNDO/REDO Logs

Databases maintain two types of log records:

**UNDO Log:** How to reverse an operation
```
UNDO: Alice's balance was $500 (before deduction)
UNDO: Bob's balance was $300 (before addition)
```

**REDO Log:** How to replay an operation
```
REDO: Set Alice's balance to $400
REDO: Set Bob's balance to $400
```

**Usage:**
- **ROLLBACK** (transaction aborted): Use UNDO log to reverse changes
- **RECOVERY** (after crash): Use REDO log to replay committed transactions

### 4. Two-Phase Commit (2PC) for Distributed Transactions

When a transaction spans multiple databases:

```
Coordinator Database
        │
        ├─────────────┬─────────────┐
        ↓             ↓             ↓
   Database A    Database B    Database C
```

**Phase 1: PREPARE**
```
Coordinator: "Can you all commit this transaction?"
Database A: "Yes, I'm ready" ✓
Database B: "Yes, I'm ready" ✓
Database C: "Yes, I'm ready" ✓
```

**Phase 2: COMMIT**
```
Coordinator: "Everyone commit now!"
Database A: Commits ✓
Database B: Commits ✓
Database C: Commits ✓
```

**If any database says "No" in Phase 1:**
```
Coordinator: "Everyone ROLLBACK!"
All databases: Rollback ✓
```

This ensures atomicity across multiple databases.

---

## Real Example: PostgreSQL's Atomicity Implementation

The following traces a real transaction in PostgreSQL:

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

**What happens internally:**

```
1. BEGIN
   - Assign Transaction ID (XID): 12345
   - Start recording in WAL

2. First UPDATE
   WAL Record:
   {
     "xid": 12345,
     "operation": "UPDATE",
     "table": "accounts",
     "old_value": {"id": 1, "balance": 500},
     "new_value": {"id": 1, "balance": 400}
   }
   - Write to WAL buffer
   - Modify page in shared buffer (memory)

3. Second UPDATE
   WAL Record:
   {
     "xid": 12345,
     "operation": "UPDATE",
     "table": "accounts",
     "old_value": {"id": 2, "balance": 300},
     "new_value": {"id": 2, "balance": 400}
   }
   - Write to WAL buffer
   - Modify page in shared buffer

4. COMMIT
   - Flush WAL buffer to disk (fsync)
   - Write COMMIT record to WAL
   - Mark transaction as committed
   - Return success to client

5. Background Process (later)
   - Checkpoint: Write dirty pages to disk
   - Can happen minutes later!
```

**Key insight:** The COMMIT is considered successful once the WAL is on disk, even if data pages are not written yet.

---

## How NoSQL Databases Handle Atomicity

NoSQL databases have **varying levels** of atomicity support.

### 1. MongoDB (Document Store)

**Single Document Atomicity: ✅ YES**

```javascript
// This is atomic - all fields updated together or none
db.users.updateOne(
  { _id: "user1" },
  {
    $set: { name: "Alice Updated" },
    $inc: { loginCount: 1 },
    $push: { tags: "premium" }
  }
);
```

**Multi-Document Transactions: ✅ YES (since v4.0)**

```javascript
// Multi-document ACID transaction
const session = client.startSession();
session.startTransaction();

try {
  await accounts.updateOne(
    { _id: "alice" },
    { $inc: { balance: -100 } },
    { session }
  );
  
  await accounts.updateOne(
    { _id: "bob" },
    { $inc: { balance: 100 } },
    { session }
  );
  
  await session.commitTransaction();
} catch (error) {
  await session.abortTransaction();
  throw error;
} finally {
  session.endSession();
}
```

**How MongoDB implements atomicity:**
- Uses **Write-Ahead Logging** (similar to relational DBs)
- Journal files record all operations
- On crash: Replay journal to recover

### 2. Cassandra (Column-Family Store)

**Row-Level Atomicity: ✅ YES**

```sql
-- All columns in a row are updated atomically
UPDATE users 
SET name = 'Alice', email = 'alice@new.com', age = 29
WHERE user_id = 'user1';
```

**Multi-Row Atomicity: ❌ NO (by design)**

```sql
-- These are NOT atomic together!
UPDATE accounts SET balance = balance - 100 WHERE user_id = 'alice';
UPDATE accounts SET balance = balance + 100 WHERE user_id = 'bob';
-- If crash happens between these, data is inconsistent!
```

**Reason:** Cassandra prioritizes **availability** over consistency (AP in CAP theorem).

**Workaround: Lightweight Transactions (LWT)**
```sql
-- Uses Paxos consensus algorithm
UPDATE accounts 
SET balance = 400 
WHERE user_id = 'alice' 
IF balance = 500;  -- Conditional update
```

However, LWT is **slow** and defeats Cassandra's performance benefits.

### 3. Redis (Key-Value Store)

**Single Command Atomicity: ✅ YES**

```redis
# Atomic increment
INCR counter

# Atomic multi-field update
HMSET user:1 name "Alice" age 28 email "alice@example.com"
```

**Multi-Command Atomicity: ✅ YES (with MULTI/EXEC)**

```redis
MULTI
DECRBY account:alice 100
INCRBY account:bob 100
EXEC
```

**How Redis implements atomicity:**
- **Single-threaded** event loop (no concurrency issues)
- MULTI/EXEC creates a transaction queue
- All commands execute atomically when EXEC is called

### 4. DynamoDB (Key-Value Store)

**Single Item Atomicity: ✅ YES**

```javascript
// Atomic conditional update
await dynamodb.updateItem({
  TableName: 'Accounts',
  Key: { userId: 'alice' },
  UpdateExpression: 'SET balance = balance - :amount',
  ConditionExpression: 'balance >= :amount',
  ExpressionAttributeValues: {
    ':amount': 100
  }
});
```

**Multi-Item Transactions: ✅ YES (TransactWriteItems)**

```javascript
await dynamodb.transactWriteItems({
  TransactItems: [
    {
      Update: {
        TableName: 'Accounts',
        Key: { userId: 'alice' },
        UpdateExpression: 'SET balance = balance - :amount',
        ExpressionAttributeValues: { ':amount': 100 }
      }
    },
    {
      Update: {
        TableName: 'Accounts',
        Key: { userId: 'bob' },
        UpdateExpression: 'SET balance = balance + :amount',
        ExpressionAttributeValues: { ':amount': 100 }
      }
    }
  ]
});
```

---

## Comparison Table: Atomicity Support

| Database | Single Record | Multi-Record | Implementation |
|----------|--------------|--------------|----------------|
| **PostgreSQL** | ✅ Yes | ✅ Yes | WAL + MVCC |
| **MySQL** | ✅ Yes | ✅ Yes | WAL (InnoDB) |
| **MongoDB** | ✅ Yes | ✅ Yes (v4.0+) | WAL + Journal |
| **Cassandra** | ✅ Yes | ❌ No* | Commit Log |
| **Redis** | ✅ Yes | ✅ Yes (MULTI) | Single-threaded |
| **DynamoDB** | ✅ Yes | ✅ Yes | Distributed transactions |
| **CouchDB** | ✅ Yes | ❌ No | MVCC |

*Cassandra has LWT for multi-row atomicity, but it's slow and rarely used.

---

## Key Takeaways

### Relational Databases (SQL):
1. **Strong atomicity guarantees** for all transactions
2. Use **Write-Ahead Logging (WAL)** as the primary mechanism
3. Support **distributed transactions** (2PC)
4. Atomicity is **non-negotiable** - always enforced

### NoSQL Databases:
1. **Varies by database type** and design goals
2. Usually support **single-document/row atomicity**
3. **Multi-document atomicity** is optional or limited
4. Trade-off: **Performance/Availability vs. Atomicity**

### The Fundamental Technique:
**Write-Ahead Logging (WAL)** is the universal solution:
```
1. Write operation to log (durable)
2. Modify data in memory
3. Write COMMIT to log
4. Later: Flush data to disk
```

This ensures that even if the system crashes, the database can:
- **REDO** committed transactions (replay from log)
- **UNDO** uncommitted transactions (rollback from log)

---

## Practical Example: Building a Simple Atomic Operation

The following pseudocode shows one way to implement atomicity:

```python
class Transaction:
    def __init__(self):
        self.operations = []
        self.wal = WriteAheadLog()
        
    def update(self, key, value):
        # Record operation in WAL first
        self.wal.write({
            'operation': 'UPDATE',
            'key': key,
            'old_value': self.get_current_value(key),
            'new_value': value
        })
        
        # Add to transaction operations
        self.operations.append(('UPDATE', key, value))
    
    def commit(self):
        # Write COMMIT record to WAL
        self.wal.write({'operation': 'COMMIT'})
        self.wal.flush_to_disk()  # Ensure durability
        
        # Now apply all operations
        for op, key, value in self.operations:
            self.apply_to_database(key, value)
        
        return True
    
    def rollback(self):
        # Write ROLLBACK record to WAL
        self.wal.write({'operation': 'ROLLBACK'})
        
        # Undo all operations using old values from WAL
        for entry in reversed(self.wal.get_entries()):
            if entry['operation'] == 'UPDATE':
                self.apply_to_database(
                    entry['key'], 
                    entry['old_value']
                )
```

---

## Review Questions

1. Why must the WAL be written to disk before returning success to the client?
2. What happens if a crash occurs between writing two operations to the WAL but before COMMIT?
3. Why can NoSQL databases like Cassandra sacrifice multi-row atomicity?