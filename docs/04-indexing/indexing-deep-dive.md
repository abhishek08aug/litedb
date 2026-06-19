# Module 4: Indexing Deep Dive

Indexes are the single most impactful tool for database performance. A query that takes 10 minutes without an index can take 10 milliseconds with one. Indexes also carry costs, which the sections below cover in full.

---

## What is an Index?

An index is a **separate data structure** (almost always a B+ Tree) that maintains a sorted copy of one or more columns, with pointers back to the actual rows.

```
Without index:
  SELECT * FROM users WHERE email = 'alice@example.com';
  → Scan ALL rows one by one (Full Table Scan)
  → 1 million rows = 1 million comparisons ❌ slow

With index on email:
  → Traverse B+ Tree: 3-4 steps → found!
  → 1 million rows = ~20 comparisons ✓ fast
```

**The trade-off:**
```
Index benefits:  Faster reads (SELECT, WHERE, JOIN, ORDER BY)
Index costs:     Slower writes (INSERT/UPDATE/DELETE must update index too)
                 Extra disk space
                 Extra memory (buffer pool caches index pages)
```

---

## Types of Indexes

### 1. Primary Index (Clustered Index)

The index that **determines the physical order of rows** on disk.

```
MySQL InnoDB — Table IS the primary index:

Primary Key B+ Tree:
  Leaf nodes contain FULL ROW DATA

  [PK=1: {name="Alice", age=25, email="a@x.com"}]
  [PK=2: {name="Bob",   age=30, email="b@x.com"}]
  [PK=3: {name="Carol", age=22, email="c@x.com"}]

Rules:
  - Only ONE clustered index per table (it IS the table)
  - If no PK defined → InnoDB creates a hidden 6-byte row ID
  - Rows are physically stored in PK order
```

**Implication:** Inserting rows in non-sequential PK order causes **page splits** (expensive!). This is why UUIDs as primary keys are bad for InnoDB:

```
Sequential PKs (1, 2, 3, 4...):
  Always insert at the END of the B+ Tree → no splits ✓

Random UUIDs (f3a2..., 1b9c..., e7d4...):
  Insert anywhere in the tree → frequent page splits ❌
  → 50% page utilization (wasted space)
  → Much slower inserts
```

---

### 2. Secondary Index (Non-Clustered Index)

Any index that is NOT the primary index.

```
CREATE INDEX idx_email ON users(email);

Secondary Index B+ Tree:
  Leaf nodes contain: email → PRIMARY KEY value

  ["a@x.com" → PK=1]
  ["b@x.com" → PK=2]
  ["c@x.com" → PK=3]

To fetch full row:
  Step 1: Traverse secondary index → get PK=1
  Step 2: Traverse primary index with PK=1 → get full row
  (This second lookup is called a "bookmark lookup" or "key lookup")
```

**PostgreSQL difference:**
```
PostgreSQL secondary index leaf nodes contain:
  email → (page_number, slot_number)  ← heap pointer

  ["a@x.com" → (page=3, slot=2)]

To fetch full row:
  Step 1: Traverse index → get heap pointer
  Step 2: Go directly to heap page 3, slot 2 → get full row
  (Still 2 lookups, but no second B+ Tree traversal)
```

---

### 3. Composite Index (Multi-Column Index)

An index on multiple columns together.

```sql
CREATE INDEX idx_name_age ON users(last_name, first_name, age);
```

```
Composite Index B+ Tree — sorted by (last_name, first_name, age):

  ["Brown", "Alice", 25] → PK=5
  ["Brown", "Bob",   30] → PK=8
  ["Brown", "Carol", 22] → PK=2
  ["Smith", "Alice", 28] → PK=1
  ["Smith", "Dave",  35] → PK=9
```

**The Left-Prefix Rule** — the most important rule for composite indexes:

```
Index: (last_name, first_name, age)

Queries that CAN use this index:
  ✅ WHERE last_name = 'Smith'
  ✅ WHERE last_name = 'Smith' AND first_name = 'Alice'
  ✅ WHERE last_name = 'Smith' AND first_name = 'Alice' AND age = 28
  ✅ WHERE last_name = 'Smith' AND age = 28  (partial — uses last_name only)

Queries that CANNOT use this index:
  ❌ WHERE first_name = 'Alice'           (skips last_name)
  ❌ WHERE age = 28                        (skips both)
  ❌ WHERE first_name = 'Alice' AND age=28 (skips last_name)

Rule: Must start from the leftmost column.
      Can skip trailing columns but NOT leading ones.
```

**Why?** Because the index is sorted by `last_name` first. Without knowing `last_name`, the B+ Tree cannot be navigated — the whole structure would have to be scanned.

---

### 4. Covering Index

An index that contains ALL columns needed by a query — so the database never needs to touch the actual table.

```sql
-- Query:
SELECT first_name, age FROM users WHERE last_name = 'Smith';

-- Index:
CREATE INDEX idx_covering ON users(last_name, first_name, age);
```

```
Index leaf node already has: last_name, first_name, age
Query needs:                 last_name (filter), first_name, age (return)

→ Index has everything! No need to look up the actual row.
→ This is called an "Index-Only Scan" (PostgreSQL) or "Covering Index" (MySQL)
→ Can be 2-5x faster than a regular index scan
```

**PostgreSQL EXPLAIN output:**
```
Index Only Scan using idx_covering on users
  Index Cond: (last_name = 'Smith')
  Heap Fetches: 0   ← never touched the actual table!
```

---

### 5. Partial Index

An index on a **subset of rows** (with a WHERE clause).

```sql
-- Only index active users (not the millions of deleted ones)
CREATE INDEX idx_active_users ON users(email) WHERE status = 'active';

-- Only index non-null values
CREATE INDEX idx_phone ON users(phone) WHERE phone IS NOT NULL;
```

```
Benefits:
  ✓ Much smaller index (only active users)
  ✓ Faster to build and maintain
  ✓ Fits better in buffer pool cache

Use case: If 90% of rows are 'deleted' and only 'active' rows are queried,
          a partial index is 10x smaller and faster.
```

---

### 6. Expression / Functional Index

An index on the **result of a function**, not a raw column.

```sql
-- Without functional index — can't use index:
SELECT * FROM users WHERE LOWER(email) = 'alice@example.com';
-- ↑ Function on column = index unusable!

-- Create functional index:
CREATE INDEX idx_lower_email ON users(LOWER(email));

-- Now this query uses the index:
SELECT * FROM users WHERE LOWER(email) = 'alice@example.com'; ✓
```

---

## How the Query Planner Decides to Use an Index

The query planner uses **statistics** to decide whether an index is worth using:

```
Decision factors:

1. Selectivity: How many rows does the condition match?
   - High selectivity (few rows) → use index ✓
   - Low selectivity (many rows) → full table scan faster ✗

   Example:
   WHERE status = 'deleted'  → 90% of rows → full scan faster
   WHERE email = 'alice@x.com' → 1 row → index is perfect

2. Table size:
   - Small table (< 1000 rows) → full scan often faster
   - Large table → index almost always better

3. Index statistics (pg_stats in PostgreSQL):
   - DB tracks column value distribution
   - Updated by ANALYZE command
   - Stale stats → bad query plans!
```

**Force index usage (for debugging):**
```sql
-- PostgreSQL: disable sequential scan to force index
SET enable_seqscan = off;
EXPLAIN SELECT * FROM users WHERE email = 'alice@x.com';

-- MySQL: force specific index
SELECT * FROM users FORCE INDEX (idx_email) WHERE email = 'alice@x.com';
```

---

## Index Internals: What Happens on INSERT/UPDATE/DELETE

Every write must maintain ALL indexes on the table:

```sql
-- Table has 3 indexes: PK, idx_email, idx_name_age
INSERT INTO users VALUES (10, 'Dave', 'Smith', 35, 'dave@x.com');

What happens:
  1. Write to WAL
  2. Insert row into heap/clustered index
  3. Insert (email → PK=10) into idx_email B+ Tree
  4. Insert (Smith, Dave, 35 → PK=10) into idx_name_age B+ Tree
  5. Each B+ Tree insert may cause page splits

Cost: 1 logical insert = 3 B+ Tree modifications + WAL records
```

**This is why too many indexes hurt write performance:**
```
Table with 10 indexes:
  1 INSERT → 10 B+ Tree modifications
  1 UPDATE → up to 20 B+ Tree modifications (delete old + insert new)
  1 DELETE → 10 B+ Tree modifications

Rule of thumb:
  OLTP tables: 3-5 indexes max
  Read-only/analytics tables: as many as needed
```

---

## Common Indexing Mistakes

### Mistake 1: Index on Low-Cardinality Column

```sql
-- BAD: Only 2 distinct values (M/F) — index useless
CREATE INDEX idx_gender ON users(gender);

-- Query returns 50% of table → full scan is faster!
SELECT * FROM users WHERE gender = 'M';
```

### Mistake 2: Function on Indexed Column

```sql
-- BAD: Index on `created_at` is NOT used here
SELECT * FROM orders WHERE YEAR(created_at) = 2024;

-- GOOD: Rewrite to use the index
SELECT * FROM orders
WHERE created_at >= '2024-01-01' AND created_at < '2025-01-01';
```

### Mistake 3: Wrong Column Order in Composite Index

```sql
-- Query pattern: filter by status, sort by created_at
SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at;

-- BAD index order:
CREATE INDEX idx_bad ON orders(created_at, status);
-- Can't use for status filter efficiently

-- GOOD index order:
CREATE INDEX idx_good ON orders(status, created_at);
-- Filter by status (equality) first, then sort by created_at
```

### Mistake 4: Implicit Type Conversion

```sql
-- Column `user_id` is INT, but passing string
SELECT * FROM users WHERE user_id = '123';
-- DB converts '123' to INT → index might not be used!

-- Always match types:
SELECT * FROM users WHERE user_id = 123;
```

### Mistake 5: Leading Wildcard

```sql
-- BAD: Leading % means can't use index (must scan all values)
SELECT * FROM users WHERE name LIKE '%alice%';

-- GOOD: Trailing % can use index
SELECT * FROM users WHERE name LIKE 'alice%';
```

---

## Index Design Strategy

```
Step 1: Identify the most frequent / slowest queries
Step 2: Look at WHERE, JOIN ON, ORDER BY, GROUP BY columns
Step 3: Apply the rules:

  For equality filters:    Put these FIRST in composite index
  For range filters:       Put these AFTER equality columns
  For ORDER BY / GROUP BY: Put these LAST (if same direction)
  For SELECT columns:      Add to index for covering index benefit

Example query:
  SELECT name, email
  FROM users
  WHERE status = 'active'        ← equality
    AND age > 25                 ← range
  ORDER BY created_at            ← sort

Optimal index:
  CREATE INDEX ON users(status, age, created_at, name, email);
  --                    ^^^^^^  ^^^  ^^^^^^^^^^^  ^^^^^^^^^^^
  --                    equal  range    sort       covering
```

---

## Key Takeaways

| Concept | Key Point |
|---------|-----------|
| Primary/Clustered | Table IS the B+ Tree (InnoDB). Only one per table. |
| Secondary | Separate B+ Tree → PK or heap pointer. Multiple allowed. |
| Composite | Left-prefix rule. Column order matters enormously. |
| Covering | All needed columns in index → no table lookup needed. |
| Partial | Index subset of rows → smaller, faster. |
| Functional | Index on expression result, not raw column. |
| Too many indexes | Slows writes. 3-5 for OLTP tables. |
| Low cardinality | Don't index boolean/gender columns alone. |
| Functions on columns | Break index usage. Rewrite the query instead. |

---

**Next Up: Module 5 — Query Processing & Optimization**

Topics covered:
- How the database parses and plans a SQL query
- The query optimizer: cost-based vs rule-based
- Join algorithms: Nested Loop, Hash Join, Merge Join
- EXPLAIN / EXPLAIN ANALYZE — reading query plans
- How to diagnose and fix slow queries