# Module 2: Complete ACID Properties - Consistency & Durability

**Atomicity** (A) and **Isolation** (I) are covered in depth in Module 1. This module completes the picture with **Consistency** (C) and **Durability** (D).

---

## Quick Recap: ACID

```
A - Atomicity:    All or nothing (covered in Module 1)
C - Consistency:  Data always moves from one valid state to another
I - Isolation:    Concurrent transactions don't interfere (covered in Module 1)
D - Durability:   Committed data survives crashes (partially covered)
```

---

## Part 1: CONSISTENCY

### What is Consistency?

**Consistency** means the database always moves from one **valid state** to another valid state. A transaction can never leave the database in a broken or invalid state.

```
Valid State A  →  [Transaction]  →  Valid State B

Example:
State A: Alice has $500, Bob has $300 (total = $800)
Transaction: Transfer $100 from Alice to Bob
State B: Alice has $400, Bob has $400 (total = $800)

Total is still $800 ✓ - Consistent!
```

**Invalid state example:**
```
State A: Alice has $500, Bob has $300 (total = $800)
Transaction: Deduct $100 from Alice (but crash before adding to Bob)
State B: Alice has $400, Bob has $300 (total = $700)

Total is $700 ❌ - Inconsistent! $100 vanished!
```

---

### Two Types of Consistency

#### 1. Database-Level Consistency (Enforced by the DB)

The database itself enforces rules that data must follow:

**a) Primary Key Constraints**
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,  -- Must be unique, cannot be NULL
    email VARCHAR(255)
);

INSERT INTO users VALUES (1, 'alice@example.com');
INSERT INTO users VALUES (1, 'bob@example.com');  -- ❌ ERROR: Duplicate key!
-- Database REJECTS this - maintains consistency
```

**b) Foreign Key Constraints**
```sql
CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT REFERENCES users(id),  -- Must exist in users table
    amount DECIMAL
);

INSERT INTO orders VALUES (1, 999, 100.00);  -- ❌ ERROR: user_id 999 doesn't exist!
-- Database REJECTS this - maintains referential integrity
```

**c) NOT NULL Constraints**
```sql
CREATE TABLE accounts (
    id INT PRIMARY KEY,
    balance DECIMAL NOT NULL CHECK (balance >= 0)  -- Cannot be NULL or negative
);

INSERT INTO accounts VALUES (1, -100);  -- ❌ ERROR: Check constraint violated!
UPDATE accounts SET balance = -50 WHERE id = 1;  -- ❌ ERROR!
```

**d) UNIQUE Constraints**
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255) UNIQUE  -- No two users can have same email
);
```

**e) CHECK Constraints**
```sql
CREATE TABLE employees (
    id INT PRIMARY KEY,
    age INT CHECK (age >= 18 AND age <= 100),
    salary DECIMAL CHECK (salary > 0)
);
```

#### 2. Application-Level Consistency (Enforced by Application Code)

Some rules are too complex for the database to enforce:

```python
# Business rule: A user cannot transfer more than their balance
def transfer_money(from_id, to_id, amount):
    with transaction():
        sender = get_account(from_id)
        
        # Application-level consistency check
        if sender.balance < amount:
            raise InsufficientFundsError("Cannot transfer more than balance!")
        
        deduct(from_id, amount)
        add(to_id, amount)
        # Database constraints ensure no negative balances
```

---

### How Consistency is Enforced: The Constraint Check Flow

```
Transaction executes:
    ↓
Modify data in buffer pool
    ↓
Before COMMIT:
    ↓
┌─────────────────────────────────────────────────────────────┐
│  CONSTRAINT CHECKER                                         │
│                                                             │
│  1. Check PRIMARY KEY constraints                          │
│  2. Check FOREIGN KEY constraints                          │
│  3. Check NOT NULL constraints                             │
│  4. Check UNIQUE constraints                               │
│  5. Check CHECK constraints                                │
│  6. Run TRIGGERS (if any)                                  │
└─────────────────────────────────────────────────────────────┘
    ↓
All pass? → COMMIT ✓
Any fail? → ROLLBACK ❌ (transaction aborted)
```

### Deferred Constraints

Some constraints can be checked at COMMIT time instead of immediately:

```sql
-- Immediate constraint (default): Checked after each statement
ALTER TABLE orders ADD CONSTRAINT fk_user
    FOREIGN KEY (user_id) REFERENCES users(id)
    DEFERRABLE INITIALLY IMMEDIATE;

-- Deferred constraint: Checked only at COMMIT
ALTER TABLE orders ADD CONSTRAINT fk_user
    FOREIGN KEY (user_id) REFERENCES users(id)
    DEFERRABLE INITIALLY DEFERRED;
```

**Why deferred constraints?**
```sql
-- Scenario: Circular foreign keys
-- users.manager_id → users.id (a user's manager must also be a user)

BEGIN;
-- Insert CEO (no manager yet)
INSERT INTO users VALUES (1, 'CEO', NULL);
-- Insert Manager (reports to CEO)
INSERT INTO users VALUES (2, 'Manager', 1);
-- Update CEO to report to... themselves? Or a board member
UPDATE users SET manager_id = 2 WHERE id = 1;
COMMIT;  -- Constraints checked here, all valid ✓
```

---

### Consistency in NoSQL Databases

NoSQL databases often **relax consistency** for performance:

```
MongoDB (default):
- No foreign key constraints
- No schema enforcement (schemaless)
- Application must ensure consistency

MongoDB with Schema Validation:
db.createCollection("users", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["name", "email"],
            properties: {
                age: { bsonType: "int", minimum: 0 }
            }
        }
    }
});
```

---

## Part 2: DURABILITY

### What is Durability?

**Durability** means once a transaction is committed, it **permanently survives** - even if:
- The database crashes
- The server loses power
- The OS crashes
- Hardware fails

```
Client receives "COMMIT SUCCESS"
    ↓
Data MUST survive:
    ✓ Database process crash
    ✓ Server power loss
    ✓ OS kernel panic
    ✓ Hardware failure (with proper setup)
```

The basics (WAL + fsync) are introduced in Module 1. The sections below go deeper.

---

### Durability Layers: Defense in Depth

Durability is achieved through **multiple layers**:

```
Layer 1: Write-Ahead Log (WAL)
    - Every change logged before applied
    - Survives process crash

Layer 2: fsync()
    - Forces data to physical disk
    - Survives power loss

Layer 3: Replication
    - Data on multiple machines
    - Survives single machine failure

Layer 4: Backups
    - Point-in-time snapshots
    - Survives data center failure

Layer 5: Geographic Distribution
    - Data in multiple regions
    - Survives regional disasters
```

---

### Layer 1: WAL (Write-Ahead Log) - Deep Dive

WAL records changes before applying them. Its structure is as follows:

```
WAL File Structure:
┌─────────────────────────────────────────────────────────────┐
│  WAL Segment File (16MB default in PostgreSQL)             │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ WAL Record 1:                                        │  │
│  │   LSN: 0/1000000 (Log Sequence Number)              │  │
│  │   XID: 100 (Transaction ID)                         │  │
│  │   Type: HEAP_INSERT                                  │  │
│  │   Data: {table: accounts, row: {id:1, balance:500}} │  │
│  │   CRC: 0xABCD1234 (checksum for integrity)          │  │
│  └─────────────────────────────────────────────────────┘  │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ WAL Record 2:                                        │  │
│  │   LSN: 0/1000100                                     │  │
│  │   XID: 100                                           │  │
│  │   Type: XACT_COMMIT                                  │  │
│  │   Timestamp: 2024-01-15 10:30:00                    │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**LSN (Log Sequence Number):**
- Monotonically increasing number
- Uniquely identifies each WAL record
- Used to determine what needs to be replayed after crash

### WAL Recovery Process

```
Database starts after crash:
    ↓
1. Find last checkpoint LSN
   (Checkpoint = point where all dirty pages were flushed to disk)
    ↓
2. Read WAL from checkpoint LSN onwards
    ↓
3. REDO phase: Replay all committed transactions
   ┌─────────────────────────────────────────────────────┐
   │ For each WAL record after checkpoint:               │
   │   If COMMIT found → Apply changes to DB files       │
   │   If no COMMIT → Skip (will be rolled back)         │
   └─────────────────────────────────────────────────────┘
    ↓
4. UNDO phase: Roll back uncommitted transactions
   ┌─────────────────────────────────────────────────────┐
   │ For each transaction without COMMIT:                │
   │   Apply UNDO records (reverse the changes)          │
   └─────────────────────────────────────────────────────┘
    ↓
5. Database is consistent and ready!
```

---

### Layer 2: Checkpoints - Limiting Recovery Time

Without checkpoints, recovery would replay the ENTIRE WAL history (could be days!).

**Checkpoints** periodically flush dirty pages to disk and record a "safe point":

```
WAL Timeline:
─────────────────────────────────────────────────────────────→
LSN: 1000    2000    3000    4000    5000    6000    [CRASH]
              ↑               ↑
         Checkpoint 1    Checkpoint 2

Recovery after crash:
- Start from Checkpoint 2 (LSN 3000)
- Only replay LSN 3000 → 6000
- Much faster than replaying from LSN 1000!
```

**Checkpoint Process:**
```
1. Write CHECKPOINT_START to WAL
2. Flush all dirty buffer pool pages to disk
3. Write CHECKPOINT_END to WAL (with list of dirty pages)
4. Old WAL segments before checkpoint can be archived/deleted
```

**PostgreSQL Checkpoint Configuration:**
```sql
-- How often to checkpoint (time-based)
SHOW checkpoint_timeout;  -- Default: 5 minutes

-- How much WAL to accumulate before checkpoint (size-based)
SHOW max_wal_size;  -- Default: 1GB

-- Spread checkpoint I/O over time (reduce I/O spikes)
SHOW checkpoint_completion_target;  -- Default: 0.9 (90% of interval)
```

---

### Layer 3: Replication for Durability

Even with WAL + fsync, a single disk failure can lose data. Replication adds another durability layer:

```
Durability Levels:

Level 1: Single node, WAL on disk
  - Survives: Process crash, OS crash
  - Fails: Disk failure, power loss (if battery-backed write cache fails)

Level 2: Single node, WAL + fsync
  - Survives: Process crash, OS crash, power loss
  - Fails: Disk failure, hardware failure

Level 3: Synchronous replication (2+ nodes)
  - Survives: Single node failure, disk failure
  - Fails: Simultaneous failure of all nodes

Level 4: Multi-region replication
  - Survives: Data center failure
  - Fails: Global catastrophe (unlikely!)
```

---

### Durability vs Performance: The Trade-off

Every durability guarantee has a performance cost:

```
┌─────────────────────────────────────────────────────────────┐
│ Configuration          │ Durability │ Performance           │
├─────────────────────────────────────────────────────────────┤
│ No WAL, no fsync       │ ❌ None    │ 🚀 Fastest            │
│ WAL, no fsync          │ ⚠️ Partial │ 🏃 Fast               │
│ WAL + fsync            │ ✅ Good    │ 🚶 Moderate           │
│ WAL + fsync + sync rep │ ✅✅ Strong │ 🐢 Slower             │
│ WAL + fsync + multi-AZ │ ✅✅✅ Best │ 🐌 Slowest            │
└─────────────────────────────────────────────────────────────┘
```

**PostgreSQL Durability Settings:**
```sql
-- Full durability (default)
synchronous_commit = on          -- fsync before returning success
wal_sync_method = fdatasync      -- How to fsync

-- Reduced durability (faster)
synchronous_commit = off         -- Return success before fsync
-- Risk: Up to wal_writer_delay (200ms) of data loss on crash

-- No durability (testing only!)
fsync = off                      -- NEVER use in production!
```

---

### ARIES: The Algorithm Behind WAL Recovery

Most modern databases use **ARIES** (Algorithm for Recovery and Isolation Exploiting Semantics):

```
ARIES Three Phases:

Phase 1: ANALYSIS
  - Scan WAL from last checkpoint
  - Build "dirty page table" (pages modified but not on disk)
  - Build "transaction table" (active transactions at crash)

Phase 2: REDO
  - Replay ALL changes from checkpoint (even uncommitted!)
  - Restore database to exact state at crash
  - Why? Some uncommitted changes might be on disk already

Phase 3: UNDO
  - Roll back all uncommitted transactions
  - Apply UNDO records in reverse order
  - Ensures atomicity
```

**Why REDO uncommitted changes first?**
```
Scenario:
- Transaction A (uncommitted) modified page P
- Checkpoint flushed page P to disk (with uncommitted data!)
- Crash occurs

Without REDO: Page P has uncommitted data on disk
With REDO: Replay to get exact crash state, then UNDO removes it

ARIES ensures correctness regardless of when pages are flushed!
```

---

## Putting It All Together: ACID in Action

The following traces a complete bank transfer through all four ACID properties:

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;  -- Alice
UPDATE accounts SET balance = balance + 100 WHERE id = 2;  -- Bob
COMMIT;
```

### Atomicity (A):
```
Both updates succeed → COMMIT
Either fails → ROLLBACK both
WAL ensures this survives crashes
```

### Consistency (C):
```
Before: Alice=500, Bob=300, Total=800
After:  Alice=400, Bob=400, Total=800

Constraints checked:
✓ balance >= 0 (CHECK constraint)
✓ No NULL values
✓ Foreign keys valid
Total money preserved ✓
```

### Isolation (I):
```
Other transactions cannot see:
- Alice's balance as 400 until committed (READ COMMITTED)
- Intermediate state where Alice=400 but Bob=300
Controlled by isolation level + locks/MVCC
```

### Durability (D):
```
After COMMIT:
1. WAL flushed to disk (fsync)
2. Client receives SUCCESS
3. Even if crash occurs now:
   - WAL has COMMIT record
   - Recovery replays changes
   - Data is preserved ✓
```

---

## ACID in NoSQL: The Relaxed Model

NoSQL databases often relax ACID for scalability:

```
MongoDB (pre-4.0):
- Single document: ACID ✓
- Multi-document: No ACID ❌

MongoDB (4.0+):
- Multi-document transactions: ACID ✓ (with performance cost)

Cassandra:
- Single partition: Lightweight transactions (LWT)
- Multi-partition: No ACID (eventual consistency)

Redis:
- MULTI/EXEC: Atomic batch (not full ACID)
- No rollback on errors within MULTI block
```

---

## Key Takeaways

### Consistency:
- Database enforces rules via constraints (PK, FK, NOT NULL, CHECK)
- Application enforces business rules
- Transactions always move DB from valid state to valid state
- NoSQL often relaxes consistency for performance

### Durability:
- WAL + fsync = survives crashes
- Checkpoints = limit recovery time
- Replication = survives hardware failure
- ARIES algorithm = correct recovery in all scenarios
- Every durability guarantee has a performance cost

### The ACID Trade-off:
```
More ACID guarantees = More overhead = Lower performance
Less ACID guarantees = Less overhead = Higher performance

Choose based on the use case:
- Financial systems: Full ACID required
- Social media feeds: Can relax some guarantees
- Analytics: Eventual consistency often fine
```

---

## Review Questions

**Question 1:** What's the difference between database-level and application-level consistency?
**Answer:** Database-level consistency is enforced by constraints (PK, FK, CHECK). Application-level consistency is enforced by application code (business rules like "balance can't go negative" beyond what CHECK constraints cover).

**Question 2:** Why do databases use checkpoints?
**Answer:** To limit recovery time after a crash. Without checkpoints, the database would need to replay the entire WAL history. Checkpoints create a "safe point" from which recovery can start.

**Question 3:** What are the three phases of ARIES recovery?
**Answer:** Analysis (scan WAL, build dirty page table), REDO (replay all changes to restore crash state), UNDO (roll back uncommitted transactions).

**Question 4:** Why does ARIES REDO uncommitted transactions before undoing them?
**Answer:** Because some uncommitted changes might already be on disk (flushed before crash). REDO restores the exact crash state, then UNDO cleanly removes uncommitted changes.

---

**Next Up: Module 3 - Storage Engine Internals**

Topics covered:
- How data is physically stored on disk
- B-Trees: The data structure powering most SQL databases
- LSM Trees: The data structure powering Cassandra, RocksDB, LevelDB
- Why these choices matter for performance