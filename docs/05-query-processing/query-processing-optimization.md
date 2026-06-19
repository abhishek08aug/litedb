# Module 5: Query Processing & Optimization

Understanding how a database turns a SQL string into actual results is essential for writing fast queries and diagnosing performance problems.

---

## The Journey of a SQL Query

```
SQL string
      ↓
1. PARSER         → Syntax check, build parse tree
      ↓
2. ANALYZER       → Semantic check (do tables/columns exist?)
      ↓
3. REWRITER       → Apply rules (expand views, rewrite subqueries)
      ↓
4. PLANNER/       → Generate candidate plans, estimate costs,
   OPTIMIZER        pick the cheapest plan
      ↓
5. EXECUTOR       → Execute the chosen plan, return results
      ↓
Results
```

---

## Step 1 & 2: Parser + Analyzer

```sql
SELECT u.name, COUNT(o.id)
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.status = 'active'
GROUP BY u.name
HAVING COUNT(o.id) > 5;
```

**Parser** checks syntax and builds an Abstract Syntax Tree (AST):
```
SELECT
  ├── columns: [u.name, COUNT(o.id)]
  ├── FROM: users AS u
  ├── JOIN: orders AS o ON u.id = o.user_id
  ├── WHERE: u.status = 'active'
  ├── GROUP BY: u.name
  └── HAVING: COUNT(o.id) > 5
```

**Analyzer** checks:
- Do tables `users` and `orders` exist?
- Do columns `name`, `status`, `id`, `user_id` exist?
- Are types compatible? (u.id = o.user_id — both INT?)
- Does the user have permission?

---

## Step 3: Query Rewriter

The rewriter applies **logical transformations** before planning:

```sql
-- Original query with subquery:
SELECT * FROM users
WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);

-- Rewriter converts to JOIN (often faster):
SELECT DISTINCT u.*
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.amount > 100;
```

Other rewrites:
```sql
-- View expansion: replace view with its definition
SELECT * FROM active_users;
-- becomes:
SELECT * FROM users WHERE status = 'active';

-- Predicate pushdown: move WHERE closer to data source
SELECT * FROM (SELECT * FROM orders) sub WHERE amount > 100;
-- becomes:
SELECT * FROM orders WHERE amount > 100;
-- (filter BEFORE the subquery, not after)
```

---

## Step 4: The Query Optimizer (The Heart)

The optimizer's job: **find the cheapest execution plan** from potentially thousands of options.

### Cost-Based Optimization (CBO)

Modern databases use **cost-based optimization** — they estimate the cost (I/O + CPU) of each possible plan and pick the cheapest.

```
For a 3-table join, possible orderings = 3! = 6
For a 5-table join, possible orderings = 5! = 120
For a 10-table join, possible orderings = 10! = 3,628,800

The optimizer uses dynamic programming + heuristics
to avoid evaluating all possibilities.
```

**Cost estimation uses statistics:**
```sql
-- PostgreSQL statistics (updated by ANALYZE):
SELECT tablename, attname, n_distinct, correlation
FROM pg_stats
WHERE tablename = 'users';

-- n_distinct: number of distinct values in column
-- correlation: how sorted the column is on disk (1.0 = perfectly sorted)
```

### What the Optimizer Decides

1. **Which indexes to use** (or full table scan)
2. **Join order** (which table to scan first)
3. **Join algorithm** (Nested Loop, Hash Join, Merge Join)
4. **Parallelism** (use multiple CPU cores?)

---

## Join Algorithms: The Big Three

### 1. Nested Loop Join

```
For each row in Table A:
  For each row in Table B:
    If join condition matches → output row

Pseudocode:
  for row_a in table_A:
    for row_b in table_B:
      if row_a.id == row_b.user_id:
        yield (row_a, row_b)

Cost: O(N × M)  where N = rows in A, M = rows in B
```

**When it's used:**
```
✓ Small tables (inner table fits in memory)
✓ When inner table has an index on the join column
✓ LIMIT queries (can stop early)

Example: users (1000 rows) JOIN orders (1M rows) with index on user_id
  → 1000 index lookups into orders = fast

Bad for: Large tables without indexes (1M × 1M = 1 trillion comparisons)
```

### 2. Hash Join

```
Phase 1 (Build):
  Read smaller table, build hash table in memory:
  hash_table = {}
  for row in small_table:
    hash_table[row.join_key] = row

Phase 2 (Probe):
  Read larger table, probe hash table:
  for row in large_table:
    if row.join_key in hash_table:
      yield (hash_table[row.join_key], row)

Cost: O(N + M)  — linear! Much better than nested loop
```

**When it's used:**
```
✓ Large tables without useful indexes
✓ Equi-joins only (=, not <, >, LIKE)
✓ When smaller table fits in memory (work_mem in PostgreSQL)

Bad for: Range joins, non-equi joins, very large tables that don't fit in memory
         (spills to disk → much slower)
```

**Memory spill:**
```
If hash table > work_mem:
  → Partition both tables to disk
  → Process partition by partition
  → Much slower (disk I/O)

PostgreSQL default work_mem = 4MB (often too small)
SET work_mem = '256MB';  -- for complex analytical queries
```

### 3. Merge Join (Sort-Merge Join)

```
Phase 1 (Sort):
  Sort both tables by join key (if not already sorted)

Phase 2 (Merge):
  Use two pointers, advance through both sorted lists:

  Table A (sorted): [1, 2, 3, 5, 7, 9]
  Table B (sorted): [1, 1, 3, 3, 5, 8]

  Pointer A=1, Pointer B=1 → match! output
  Pointer A=1, Pointer B=1 → match! output (duplicate in B)
  Pointer A=2, Pointer B=3 → advance A
  Pointer A=3, Pointer B=3 → match! output
  ...

Cost: O(N log N + M log M) for sorting, then O(N + M) for merge
```

**When it's used:**
```
✓ Both tables already sorted (e.g., joining on indexed columns)
✓ Large tables where hash table won't fit in memory
✓ Range joins and inequality joins
✓ When result needs to be sorted anyway

Bad for: When sorting cost is high and no indexes exist
```

### Join Algorithm Summary

```
┌──────────────────┬──────────────────┬──────────────────┬──────────────────┐
│ Algorithm        │ Best For         │ Cost             │ Memory           │
├──────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Nested Loop      │ Small tables,    │ O(N × M)         │ Low              │
│                  │ indexed joins    │                  │                  │
├──────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Hash Join        │ Large tables,    │ O(N + M)         │ High             │
│                  │ no index, equi   │                  │ (spills to disk) │
├──────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Merge Join       │ Pre-sorted data, │ O(N log N +      │ Medium           │
│                  │ range joins      │  M log M)        │                  │
└──────────────────┴──────────────────┴──────────────────┴──────────────────┘
```

---

## Reading EXPLAIN / EXPLAIN ANALYZE

`EXPLAIN` shows the query plan. `EXPLAIN ANALYZE` actually runs it and shows real timings.

```sql
EXPLAIN ANALYZE
SELECT u.name, COUNT(o.id)
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.status = 'active'
GROUP BY u.name;
```

**Sample output:**
```
HashAggregate  (cost=1520.00..1545.00 rows=500 width=36)
               (actual time=45.2..46.1 rows=487 loops=1)
  Group Key: u.name
  ->  Hash Join  (cost=450.00..1470.00 rows=10000 width=28)
                 (actual time=12.3..38.7 rows=9823 loops=1)
        Hash Cond: (o.user_id = u.id)
        ->  Seq Scan on orders o  (cost=0..350.00 rows=10000 width=8)
                                  (actual time=0.1..15.2 rows=10000 loops=1)
        ->  Hash  (cost=400.00..400.00 rows=4000 width=24)
                  (actual time=11.8..11.8 rows=4000 loops=1)
              ->  Seq Scan on users u  (cost=0..400.00 rows=4000 width=24)
                                       (actual time=0.1..8.3 rows=4000 loops=1)
                    Filter: (status = 'active')
                    Rows Removed by Filter: 6000
Planning Time: 0.8 ms
Execution Time: 46.5 ms
```

**How to read it:**
```
Read BOTTOM-UP (innermost operations execute first)

cost=X..Y:
  X = startup cost (before first row returned)
  Y = total cost (all rows returned)
  Units are arbitrary "cost units" (not milliseconds)

actual time=X..Y:
  X = time to first row (ms)
  Y = total time (ms)

rows=N:
  estimated rows (from statistics)

actual rows=N:
  real rows (only in EXPLAIN ANALYZE)

If estimated rows ≠ actual rows by a lot → stale statistics.
Run ANALYZE to update them.
```

**Key nodes to recognize:**
```
Seq Scan        → Full table scan (no index used)
Index Scan      → B+ Tree traversal + heap lookup
Index Only Scan → B+ Tree traversal only (covering index)
Bitmap Scan     → Multiple index scans combined
Hash Join       → Hash join algorithm
Nested Loop     → Nested loop join
Merge Join      → Sort-merge join
HashAggregate   → GROUP BY using hash table
Sort            → ORDER BY (check whether it spills to disk)
```

---

## Diagnosing Slow Queries: A Systematic Approach

### Step 1: Find slow queries

```sql
-- PostgreSQL: enable slow query logging
log_min_duration_statement = 1000  -- log queries > 1 second

-- PostgreSQL: pg_stat_statements extension
SELECT query, calls, total_exec_time, mean_exec_time, rows
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;
```

### Step 2: Run EXPLAIN ANALYZE

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT ...the slow query...;
```

### Step 3: Look for these red flags

```
🚩 Seq Scan on large table
   → Missing index? Add one.

🚩 estimated rows = 100, actual rows = 100,000
   → Stale statistics. Run: ANALYZE table_name;

🚩 Hash Join with "Batches: 8" (spilled to disk)
   → Increase work_mem: SET work_mem = '256MB';

🚩 Sort with "Sort Method: external merge Disk"
   → Sort spilled to disk. Increase work_mem or add index.

🚩 Nested Loop with large outer table and no index
   → Add index on inner table's join column.

🚩 Filter: Rows Removed by Filter: 999,000
   → Index would help here. Or rewrite query.
```

### Step 4: Common fixes

```sql
-- Fix 1: Add missing index
CREATE INDEX ON orders(user_id);

-- Fix 2: Update statistics
ANALYZE users;
ANALYZE orders;

-- Fix 3: Increase memory for complex queries
SET work_mem = '256MB';

-- Fix 4: Rewrite correlated subquery as JOIN
-- BAD (runs subquery for every row):
SELECT * FROM users u
WHERE (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) > 5;

-- GOOD (single pass):
SELECT u.*
FROM users u
JOIN (
  SELECT user_id, COUNT(*) as cnt
  FROM orders
  GROUP BY user_id
  HAVING COUNT(*) > 5
) o ON u.id = o.user_id;

-- Fix 5: Use covering index to avoid table lookup
CREATE INDEX ON users(status, name, email);
-- Now: SELECT name, email FROM users WHERE status='active'
-- → Index Only Scan (never touches table)
```

---

## Query Optimization Rules of Thumb

```
1. Filter early (predicate pushdown)
   → WHERE before JOIN when possible

2. Join order matters
   → Join smallest result sets first
   → Let the optimizer do this, but hint if needed

3. Avoid SELECT *
   → Fetch only the required columns
   → Enables covering index usage

4. Avoid functions on indexed columns in WHERE
   → WHERE YEAR(created_at) = 2024  ← breaks index
   → WHERE created_at >= '2024-01-01'  ← uses index

5. Use LIMIT with ORDER BY + index
   → SELECT * FROM orders ORDER BY created_at DESC LIMIT 10
   → With index on created_at: reads only 10 rows

6. Pagination: use keyset pagination, not OFFSET
   → OFFSET 100000 still reads 100,000 rows and discards them
   → WHERE id > last_seen_id LIMIT 10  ← reads only 10 rows
```

---

## Key Takeaways

```
Query lifecycle:
  Parse → Analyze → Rewrite → Plan → Execute

Optimizer:
  Cost-based: estimates I/O + CPU for each plan
  Uses statistics (ANALYZE keeps them fresh)
  Decides: index vs scan, join order, join algorithm

Join algorithms:
  Nested Loop → small tables, indexed joins
  Hash Join   → large tables, equi-joins, needs memory
  Merge Join  → pre-sorted data, range joins

EXPLAIN ANALYZE:
  Read bottom-up
  Compare estimated vs actual rows (stale stats = bad plans)
  Look for Seq Scans, spills to disk, row estimate mismatches
```

---

**Modules Remaining:**
- ✅ Module 1-4 complete
- ✅ Module 5 (this one)
- [ ] Module 6: Concurrency Control & MVCC Deep Dive
- [ ] Module 7: Distributed Databases & CAP Theorem
- [ ] Module 8: Sharding & Partitioning
- [ ] Module 9: Replication & Consistency Models
- [ ] Module 10: NoSQL Design Patterns
- [ ] Module 11: Build Our Own Database (Final Project)

**6 modules remaining** after this one.