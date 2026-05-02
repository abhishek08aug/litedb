# Deep Dive: How Databases Enforce, Store & Manage Locks

---

## Where Are Locks Stored?

**Locks live entirely in RAM** — specifically in a dedicated **Lock Manager** data structure in the database process memory.

```
Database Process Memory:
┌──────────────────────────────────────────────────────┐
│  Buffer Pool (data pages)                            │
│  Query Executor                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │  LOCK MANAGER                                  │  │
│  │  ┌──────────────────────────────────────────┐  │  │
│  │  │  Lock Hash Table (in shared memory)      │  │  │
│  │  │  Lock Wait Queue                         │  │  │
│  │  │  Deadlock Detector                       │  │  │
│  │  └──────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘

⚠️ Locks are NOT written to disk.
   If the DB crashes, all locks are gone — and that's fine,
   because all transactions are rolled back on restart anyway.
```

---

## The Lock Table: Core Data Structure

The Lock Manager maintains a **hash table** keyed by the locked resource:

```
Lock Hash Table:

Key: (database_id, table_id, page_id, row_id)
         ↓
┌─────────────────────────────────────────────────────┐
│  Lock Entry for resource (db=1, table=5, row=42)   │
│                                                     │
│  granted_list:                                      │
│    → TxID=100, mode=SHARED,    status=GRANTED       │
│    → TxID=101, mode=SHARED,    status=GRANTED       │
│                                                     │
│  wait_list:                                         │
│    → TxID=102, mode=EXCLUSIVE, status=WAITING       │
│    → TxID=103, mode=SHARED,    status=WAITING       │
└─────────────────────────────────────────────────────┘
```

**Each lock entry tracks:**
- Which transaction holds/wants it
- Lock mode (SHARED, EXCLUSIVE, etc.)
- Status (GRANTED or WAITING)

---

## Lock Compatibility Matrix

Before granting a lock, the Lock Manager checks this matrix:

```
              Requested →
Held ↓        SHARED(S)   EXCLUSIVE(X)
─────────────────────────────────────
SHARED (S)  │   ✅ OK    │   ❌ WAIT  │
EXCLUSIVE(X)│   ❌ WAIT  │   ❌ WAIT  │

Rule: Multiple readers OK. Any writer blocks everyone.
```

---

## Lock Lifecycle: Step by Step

```
Transaction wants to lock row #42:
         ↓
1. Hash(db, table, row) → find bucket in lock table
         ↓
2. Lock entry exists?
   NO  → Create new entry, grant lock immediately ✓
   YES → Check compatibility with granted_list
         ↓
3. Compatible?
   YES → Add to granted_list, return ✓
   NO  → Add to wait_list, suspend transaction ⏳
         ↓
4. When blocking transaction COMMITs/ROLLBACKs:
   → Remove from granted_list
   → Wake up next waiter in wait_list
   → Grant lock to waiter ✓
```

---

## Lock Granularity: Hierarchy

Databases use a **lock hierarchy** to avoid locking millions of rows individually:

```
DATABASE
   └── TABLE
          └── PAGE (8KB block of rows)
                 └── ROW

Coarse-grained (TABLE lock):
  + Fast to acquire (1 lock)
  - Blocks all other transactions on that table

Fine-grained (ROW lock):
  + High concurrency (only blocks specific row)
  - Overhead: thousands of locks for bulk operations
```

**Intention Locks** solve the hierarchy problem:

```
Before locking a ROW with EXCLUSIVE:
  → Set IX (Intention Exclusive) on TABLE
  → Set IX on PAGE
  → Set X on ROW

This tells other transactions:
"Someone has an exclusive lock somewhere inside this table"
without scanning every row.

Compatibility:
        IS    IX    S     SIX   X
IS  │   ✅  │  ✅  │  ✅  │  ✅  │  ❌  │
IX  │   ✅  │  ✅  │  ❌  │  ❌  │  ❌  │
S   │   ✅  │  ❌  │  ✅  │  ❌  │  ❌  │
SIX │   ✅  │  ❌  │  ❌  │  ❌  │  ❌  │
X   │   ❌  │  ❌  │  ❌  │  ❌  │  ❌  │
```

---

## Deadlock Detection

When Transaction A waits for B, and B waits for A — deadlock!

```
Deadlock:
TxA holds lock on Row 1, wants Row 2
TxB holds lock on Row 2, wants Row 1

Wait-For Graph:
  TxA → TxB → TxA  (cycle = deadlock!)
```

**How databases detect it:**

```
Approach 1: Periodic cycle detection (PostgreSQL, MySQL)
  - Every ~1 second, scan the wait-for graph
  - Find cycles → pick a victim → ROLLBACK it
  - Victim selection: usually the youngest/cheapest transaction

Approach 2: Timeout (simple fallback)
  - If waiting > N seconds → assume deadlock → ROLLBACK
  - Less precise but always works

Approach 3: Wait-Die / Wound-Wait (distributed systems)
  - Older transaction always wins
  - Prevents deadlock by design (no cycles possible)
```

---

## Key Facts to Remember

| Fact | Detail |
|------|--------|
| Lock storage | RAM only (shared memory segment) |
| Lock structure | Hash table keyed by resource ID |
| Lock on crash | All locks released, transactions rolled back |
| Row lock cost | ~40-100 bytes per lock in memory |
| Deadlock check | Periodic graph cycle detection (~1s interval) |
| Lock escalation | Row locks → Table lock when too many row locks |

---

## PostgreSQL vs MySQL: Lock Implementation Differences

```
PostgreSQL:
- Lock table in shared memory (configurable via max_locks_per_transaction)
- Uses MVCC heavily → readers never block writers
- Row-level locks stored in the heap page itself (not just lock table)
- Advisory locks available for application-level locking

MySQL InnoDB:
- Lock table in buffer pool memory
- Uses both MVCC and traditional locking
- Gap locks + Next-key locks to prevent phantom reads
- Lock escalation: row → table if too many row locks
```

---

## Summary

```
Locks are:
  ✓ Stored in RAM (shared memory hash table)
  ✓ Managed by the Lock Manager
  ✓ Hierarchical (DB → Table → Page → Row)
  ✓ Checked via compatibility matrix before granting
  ✓ Released on COMMIT or ROLLBACK
  ✓ Monitored for deadlocks via wait-for graph

Locks are NOT:
  ✗ Written to disk
  ✗ Persisted across restarts
  ✗ Used by MVCC reads (readers don't take locks)