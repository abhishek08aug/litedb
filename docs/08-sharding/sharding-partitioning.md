# Module 8: Sharding & Partitioning

Sharding is how databases scale horizontally — splitting data across multiple machines so no single node becomes a bottleneck. It's also one of the hardest engineering problems to get right.

---

## Partitioning vs Sharding

```
Partitioning: Splitting data within a single database instance
  → Multiple tables/files on one machine
  → Transparent to the application
  → Examples: PostgreSQL table partitioning, MySQL partitioning

Sharding: Splitting data across multiple database instances
  → Each shard is a separate database server
  → Application (or middleware) must know which shard to query
  → Examples: Cassandra, MongoDB sharding, Vitess for MySQL
```

In practice, "sharding" and "horizontal partitioning" are used interchangeably. We'll use "sharding" for cross-machine splits.

---

## Why Shard?

```
Single node limits:
  Storage:    1 machine = max ~100TB
  Write TPS:  1 machine = max ~100K writes/sec
  Read TPS:   1 machine = max ~500K reads/sec (with replicas)

With 10 shards:
  Storage:    ~1PB
  Write TPS:  ~1M writes/sec
  Read TPS:   ~5M reads/sec

With 100 shards (Cassandra, DynamoDB scale):
  Storage:    ~10PB
  Write TPS:  ~10M writes/sec
```

---

## Sharding Strategies

### 1. Range-Based Sharding

Divide data by ranges of the shard key:

```
Shard key: user_id

Shard 1: user_id 1        – 1,000,000
Shard 2: user_id 1,000,001 – 2,000,000
Shard 3: user_id 2,000,001 – 3,000,000
Shard 4: user_id 3,000,001 – 4,000,000

Query: SELECT * FROM users WHERE user_id = 1,500,000
→ Route to Shard 2 (1M–2M range)
```

**Pros:**
```
✓ Range queries are efficient (all data in one shard)
  SELECT * WHERE user_id BETWEEN 1M AND 1.5M → only Shard 2
✓ Easy to understand and implement
✓ Easy to add new shards at the end
```

**Cons:**
```
✗ Hotspots: new users always go to the last shard
  (all writes hit Shard 4 while 1-3 are idle)
✗ Uneven data distribution if data isn't uniform
  (some ranges may have 10x more data than others)
```

**Used by:** HBase, MongoDB (range sharding option), Google Bigtable

---

### 2. Hash-Based Sharding

Apply a hash function to the shard key, use result to pick shard:

```
Shard key: user_id
Number of shards: 4

shard_number = hash(user_id) % 4

user_id=1001: hash(1001) % 4 = 1 → Shard 1
user_id=1002: hash(1002) % 4 = 2 → Shard 2
user_id=1003: hash(1003) % 4 = 3 → Shard 3
user_id=1004: hash(1004) % 4 = 0 → Shard 0
```

**Pros:**
```
✓ Even data distribution (hash spreads data uniformly)
✓ No hotspots for writes
✓ Simple to implement
```

**Cons:**
```
✗ Range queries are terrible:
  SELECT * WHERE user_id BETWEEN 1000 AND 2000
  → Must query ALL shards and merge results (scatter-gather)

✗ Resharding is painful:
  Add shard 5: hash(user_id) % 5 → almost ALL data moves!
  (This is why consistent hashing was invented)
```

---

### 3. Consistent Hashing

The solution to hash sharding's resharding problem. Used by **Cassandra, DynamoDB, Riak**.

```
Concept: Hash ring (0 to 2^32 - 1, wraps around)

Place nodes on the ring at hash(node_id) positions:
  Node A: position 0
  Node B: position 90
  Node C: position 180
  Node D: position 270

         0 (Node A)
        /           \
  270 (D)           90 (B)
        \           /
         180 (Node C)

To find which node owns a key:
  1. Hash the key → position on ring
  2. Walk clockwise → first node you hit owns the key

  hash("user:1001") = 45  → walk clockwise → Node B owns it
  hash("user:1002") = 120 → walk clockwise → Node C owns it
  hash("user:1003") = 200 → walk clockwise → Node D owns it
```

**Adding a new node (resharding):**
```
Add Node E at position 135:

         0 (Node A)
        /           \
  270 (D)           90 (B)
        \      135(E)  /
         180 (Node C)

Only keys between 90 and 135 move from C to E!
(~25% of C's data, not ALL data like in simple hash sharding)

With N nodes, adding 1 node moves only 1/N of total data.
```

**Virtual nodes (vnodes):**
```
Problem: With few physical nodes, distribution can be uneven.
Solution: Each physical node gets multiple positions on the ring.

Node A: positions [10, 150, 280]
Node B: positions [50, 190, 320]
Node C: positions [90, 230, 360]

Benefits:
  ✓ More even distribution
  ✓ When a node fails, its load spreads across ALL other nodes
    (not just its neighbor)
  ✓ New nodes can take vnodes from multiple existing nodes
```

**Cassandra's implementation:**
```
Default: 256 vnodes per physical node
Token range: 0 to 2^64 - 1
Replication factor: N (each key stored on N consecutive nodes)

Write with RF=3:
  hash(key) → position → Node B (coordinator)
  Node B writes to itself + next 2 nodes (C, D)
  Returns success when W nodes confirm (W = write quorum)
```

---

### 4. Directory-Based Sharding

A lookup service maps keys to shards:

```
Shard Directory (separate service):
  user_id 1-1000    → Shard 1
  user_id 1001-5000 → Shard 2
  user_id 5001-9000 → Shard 3
  user_id 9001+     → Shard 4

Query flow:
  1. App asks directory: "Where is user_id=3000?"
  2. Directory returns: "Shard 2"
  3. App queries Shard 2 directly
```

**Pros:**
```
✓ Maximum flexibility (can move any key to any shard)
✓ Easy resharding (just update directory)
✓ Can use any sharding logic
```

**Cons:**
```
✗ Directory is a single point of failure (must be replicated)
✗ Extra network hop for every query
✗ Directory can become a bottleneck
```

**Used by:** MongoDB's config servers (mongos router), Vitess

---

## Choosing a Shard Key

The shard key decision is **the most important architectural decision** in a sharded system. Getting it wrong is very expensive to fix.

### Good Shard Key Properties

```
1. High cardinality
   → Many distinct values → even distribution
   ✓ user_id (millions of users)
   ✗ status ('active'/'inactive' → only 2 values)

2. Even distribution
   → Data spread uniformly across shards
   ✓ user_id (if users are created uniformly)
   ✗ country (US has 100x more users than Luxembourg)

3. Low frequency of change
   → Changing shard key = moving data between shards (expensive!)
   ✓ user_id (never changes)
   ✗ email (users change emails)

4. Aligns with query patterns
   → Most queries should hit ONE shard (not scatter-gather)
   ✓ Shard by user_id if most queries are "get all data for user X"
   ✗ Shard by user_id if most queries are "get all orders in region Y"
```

### Common Shard Key Mistakes

```
Mistake 1: Monotonically increasing key (e.g., auto-increment ID)
  All new writes go to the last shard → hotspot!
  Fix: Use random UUID, or hash the ID

Mistake 2: Low cardinality key (e.g., status, country)
  Uneven distribution → some shards overloaded
  Fix: Use composite key (country + user_id)

Mistake 3: Key that doesn't match query patterns
  Most queries need data from multiple shards → scatter-gather
  Fix: Denormalize data, or choose different shard key

Mistake 4: Shard key that changes
  Moving data between shards is expensive
  Fix: Use immutable identifiers
```

---

## Cross-Shard Queries: The Hard Part

Sharding breaks many SQL features:

```
Single-shard query (fast):
  SELECT * FROM orders WHERE user_id = 1001
  → user_id=1001 is on Shard 2 → query only Shard 2 ✓

Cross-shard query (slow, complex):
  SELECT COUNT(*) FROM orders WHERE status = 'pending'
  → Must query ALL shards, sum the counts
  → Scatter-gather: O(shards) network calls

Cross-shard JOIN (very hard):
  SELECT u.name, o.total
  FROM users u JOIN orders o ON u.id = o.user_id
  WHERE o.created_at > '2024-01-01'
  → users on Shard A, orders on Shard B
  → Must fetch from both shards and join in application layer
  → Or: denormalize (store user data with each order)

Cross-shard transactions (hardest):
  BEGIN;
  UPDATE accounts SET balance = balance - 100 WHERE id = 1; -- Shard 1
  UPDATE accounts SET balance = balance + 100 WHERE id = 2; -- Shard 3
  COMMIT;
  → Requires 2-Phase Commit (2PC) across shards
  → Very slow, complex, often avoided
```

**Solutions:**
```
1. Design queries to be single-shard (best)
   → Choose shard key that aligns with query patterns

2. Denormalization
   → Store redundant data to avoid cross-shard joins
   → orders table stores user_name (not just user_id)

3. Scatter-gather with parallel queries
   → Query all shards in parallel, merge results
   → Works for aggregations, not for JOINs

4. Two-Phase Commit (2PC) for cross-shard transactions
   → Coordinator asks all shards to "prepare"
   → If all say yes → coordinator tells all to "commit"
   → Slow (2 round trips) and blocks on failure
```

---

## Resharding: The Hardest Problem

What happens when you need to add more shards?

```
Current: 4 shards, each with 1TB data
Problem: Each shard is getting full, need 8 shards

Naive approach:
  1. Stop all writes (maintenance window)
  2. Move data to new shards
  3. Update routing logic
  4. Resume writes

Problem: Moving 4TB of data takes hours → unacceptable downtime!
```

**Online resharding (zero downtime):**

```
Step 1: Add new shards (empty)
Step 2: Start dual-writing (write to old AND new shard)
Step 3: Backfill: copy existing data from old to new shards
Step 4: Verify data consistency between old and new
Step 5: Switch reads to new shards
Step 6: Stop writing to old shards
Step 7: Decommission old shards

This takes days/weeks for large datasets.
Cassandra and DynamoDB handle this automatically.
MySQL/PostgreSQL require manual work or tools like Vitess.
```

---

## Real-World Sharding Examples

### Cassandra
```
Consistent hashing with vnodes
Replication factor: typically 3
Tunable consistency: ANY, ONE, QUORUM, ALL
No cross-shard transactions (by design)
Resharding: add nodes, run nodetool repair
```

### MongoDB
```
Range or hash sharding
mongos router handles routing transparently
Config servers store shard metadata
Supports cross-shard transactions (since v4.0, slow)
Resharding: automatic chunk migration in background
```

### DynamoDB
```
Hash-based partitioning (internal, managed by AWS)
Partition key = shard key
Sort key = range within partition
Automatic resharding (splits hot partitions)
No cross-partition transactions (use TransactWriteItems for limited support)
```

### Vitess (MySQL sharding)
```
Sharding layer on top of MySQL
VTGate: query router
VTTablet: per-shard MySQL wrapper
Supports resharding with zero downtime
Used by: YouTube, Slack, GitHub
```

---

## Key Takeaways

```
Sharding strategies:
  Range:      Good for range queries, bad for hotspots
  Hash:       Good distribution, bad for range queries, hard to reshard
  Consistent: Good distribution, easy resharding (only 1/N data moves)
  Directory:  Maximum flexibility, extra lookup overhead

Shard key rules:
  High cardinality, even distribution, immutable, matches query patterns

Cross-shard problems:
  JOINs → denormalize
  Aggregations → scatter-gather
  Transactions → 2PC (avoid if possible)

Resharding:
  The hardest operational problem
  Consistent hashing minimizes data movement
  Online resharding requires dual-write + backfill

Choose sharding only when you need it:
  Single node → replicas → read replicas → then shard
  Premature sharding adds enormous complexity
```

---

**Next Up: Module 9 — Replication & Consistency Models**

We'll explore:
- Single-leader, multi-leader, and leaderless replication
- Synchronous vs asynchronous replication
- Replication lag and its consequences
- Conflict resolution in multi-leader systems
- Read-your-own-writes, monotonic reads guarantees