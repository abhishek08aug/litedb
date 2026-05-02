# Module 3: Storage Engine Internals — B-Trees & LSM Trees

This is where databases get really interesting. The **storage engine** is the heart of any database — it decides how data is physically laid out on disk, how fast reads/writes are, and what trade-offs you make.

---

## The Fundamental Problem

```
RAM:  Fast (nanoseconds), small, volatile
Disk: Slow (milliseconds), large, persistent

A database must:
1. Store data on disk (persistence)
2. Make reads/writes as fast as possible
3. Handle millions of operations per second

The storage engine is the solution to this problem.
```

---

## Two Dominant Storage Engines

```
┌──────────────────────────────────────────────────────────────┐
│  B+ Tree                        │  LSM Tree                  │
│  ──────────────────────         │  ──────────────────────    │
│  PostgreSQL, MySQL, Oracle      │  Cassandra, RocksDB,       │
│  SQLite, SQL Server             │  LevelDB, MongoDB (WT)     │
│                                 │  HBase, ScyllaDB           │
│  Optimized for: READS           │  Optimized for: WRITES     │
│  Random access: Fast            │  Sequential writes: Fast   │
│  Write amplification: Low       │  Read amplification: High  │
└──────────────────────────────────────────────────────────────┘
```

---

## Part 1: B+ Trees

### What is a B+ Tree?

A **B+ Tree** is a self-balancing tree where:
- All data lives in **leaf nodes**
- Internal nodes only store **keys** (for routing)
- Leaf nodes are **linked** (for range scans)
- Every path from root to leaf is the **same length**

```
B+ Tree (order 3, max 3 keys per node):

                    [30 | 70]                        ← Root (internal)
                   /    |    \
          [10|20]     [40|60]     [80|90]            ← Internal nodes
          /  |  \     /  |  \     /  |  \
        [5] [15] [25][35][55][65][75][85][95]        ← Leaf nodes
         ↔   ↔   ↔   ↔   ↔   ↔   ↔   ↔   ↔
        (linked list for range scans →)
```

**Key properties:**
- **Branching factor** (order): typically 100–1000 in real databases
- **Height**: log_b(N) — a tree with 1 billion rows is only ~3 levels deep!
- **Leaf nodes**: contain actual data (or pointer to data row)

---

### Why B+ Tree, not Binary Tree?

```
Binary Tree (order 2):
  - 1 billion rows → height = log2(1B) = 30 levels
  - 30 disk reads to find one row ❌ (too slow!)

B+ Tree (order 1000):
  - 1 billion rows → height = log1000(1B) = 3 levels
  - 3 disk reads to find one row ✓ (fast!)

Each node = 1 disk page (8KB or 16KB)
Fewer levels = fewer disk I/Os = faster queries
```

---

### B+ Tree: How a READ Works

```sql
SELECT * FROM users WHERE id = 55;
```

```
Step 1: Read root page from disk (or buffer pool cache)
        Root: [30 | 70]
        55 > 30 and 55 < 70 → go to middle child

Step 2: Read internal node [40 | 60]
        55 > 40 and 55 < 60 → go to middle child

Step 3: Read leaf node [55]
        Found! Return row data.

Total disk reads: 3  (for a billion-row table!)
```

---

### B+ Tree: How a WRITE Works (INSERT)

```sql
INSERT INTO users VALUES (45, 'Alice');
```

```
Step 1: Find the correct leaf node (same traversal as read)
        Navigate to leaf [40 | 50]

Step 2: Insert into leaf
        Leaf becomes [40 | 45 | 50] ✓  (fits, done)

Step 3 (if leaf is FULL): SPLIT
        [40 | 45 | 50 | 55] → too many keys!
        Split into [40 | 45] and [50 | 55]
        Push middle key (50) up to parent

Step 4: Parent might also split (cascades up to root)
        Worst case: root splits → tree grows one level taller
```

**Write amplification in B+ Trees:**
```
Insert 1 row → might rewrite:
  - 1 leaf page
  - 1+ internal pages (if splits cascade)
  - WAL records for all modified pages

Typical: 1 logical write = 2–5 physical page writes
```

---

### B+ Tree: Range Scans (The Killer Feature)

```sql
SELECT * FROM users WHERE age BETWEEN 25 AND 35;
```

```
Step 1: Find leaf node containing age=25
Step 2: Follow linked list of leaf nodes →
        [25] → [26] → [27] → ... → [35]
Step 3: Stop when age > 35

Sequential disk reads = very fast (OS prefetches pages ahead)
```

---

### B+ Tree: Page Structure on Disk

Each node is stored as a fixed-size **page** (typically 8KB or 16KB):

```
B+ Tree Page (8KB):
┌──────────────────────────────────────────────────────┐
│  Page Header (24 bytes)                              │
│    page_id:           1042                           │
│    page_type:         LEAF / INTERNAL                │
│    num_keys:          127                            │
│    free_space:        512 bytes                      │
│    right_sibling_ptr: 1043  (leaf nodes only)        │
├──────────────────────────────────────────────────────┤
│  Key-Value Pairs (sorted):                           │
│    [key=10, ptr/value] [key=15, ptr/value] ...       │
├──────────────────────────────────────────────────────┤
│  Free Space                                          │
└──────────────────────────────────────────────────────┘
```

---

## Part 2: LSM Trees (Log-Structured Merge Trees)

### The Problem B+ Trees Have with Writes

B+ Trees do **random writes** — each insert/update touches a specific page anywhere on disk. On HDDs (spinning disks), random writes are ~100x slower than sequential writes. Even on SSDs, random writes cause wear and write amplification.

**LSM Trees solve this by converting random writes into sequential writes.**

---

### LSM Tree Architecture

```
WRITE PATH:

  Client Write
       ↓
  1. WAL (sequential write, for durability)
       ↓
  2. MemTable (in-memory sorted structure, e.g. Red-Black Tree)
       ↓  (when MemTable fills up ~64MB)
  3. Flush → SSTable file on disk  (sequential write!)
       ↓  (background compaction)
  4. Merge SSTables across levels

DISK LAYOUT (Leveled Compaction):

  Level 0 (newest, overlapping):  [SST1][SST2][SST3]
  Level 1 (10MB, non-overlapping):[────────SST4────────]
  Level 2 (100MB):                [──────────────SST5──────────────]
  Level 3 (1GB):                  [─────────────────────────SST6──────]

  Each level is ~10x larger than the previous.
```

---

### Key Components

#### 1. MemTable (In-Memory Write Buffer)

```
All writes land here first (fast, in RAM):

  INSERT (key=5,  val="Alice")   → MemTable
  INSERT (key=3,  val="Bob")     → MemTable
  UPDATE (key=5,  val="Alice2")  → MemTable (new entry added)
  DELETE (key=3)                 → MemTable (tombstone marker)

MemTable (sorted by key):
  key=3 → TOMBSTONE
  key=5 → "Alice2"

When full → flushed to disk as an immutable SSTable file.
```

#### 2. SSTable (Sorted String Table)

```
SSTable file (immutable, sorted by key):
┌──────────────────────────────────────────────────────┐
│  Data Block:                                         │
│    key=3, TOMBSTONE                                  │
│    key=5, "Alice2"                                   │
│    key=7, "Charlie"                                  │
│    ...                                               │
├──────────────────────────────────────────────────────┤
│  Index Block:                                        │
│    key=3  → byte offset 0                            │
│    key=50 → byte offset 4096                         │
│    key=99 → byte offset 8192                         │
├──────────────────────────────────────────────────────┤
│  Bloom Filter:                                       │
│    "Is key=42 in this file?" → Probably NO           │
│    Avoids reading the file if key is definitely gone │
├──────────────────────────────────────────────────────┤
│  Footer: index offset, bloom filter offset, checksum │
└──────────────────────────────────────────────────────┘

SSTables are IMMUTABLE — never modified after written!
```

#### 3. Compaction (Background Merge)

```
Problem: Many SSTables accumulate, same key in multiple files.

Before compaction:
  SST1 (oldest): key=5 → "Alice"
  SST2:          key=5 → "Alice2"
  SST3 (newest): key=5 → "Alice3"

After compaction (merge-sort all files):
  SST_merged:    key=5 → "Alice3"   ← only latest kept

Benefits:
  ✓ Fewer files to search during reads
  ✓ Reclaim space from deleted/updated keys
  ✓ Maintains sorted order
```

---

### LSM Tree: How a READ Works

```sql
SELECT * FROM users WHERE id = 5;
```

```
Step 1: Check MemTable (in memory)         → not found
Step 2: Check Level 0 SSTables (newest first):
          Check Bloom Filter of SST3       → "definitely not here" → skip
          Check Bloom Filter of SST2       → "maybe here"
          Binary search SST2 index         → found at offset 4096
          Read data block                  → return "Alice2" ✓

Worst case: key not found anywhere
  → Check MemTable + ALL SSTables across ALL levels
  → Read amplification is LSM's main weakness
```

**Bloom Filter magic:**
```
Bloom Filter = probabilistic bit array
  "Definitely NOT in this file" → skip file entirely (saves I/O!)
  "Probably in this file"       → read file to confirm

False positive rate: ~1% (tunable)
False negative rate: 0% (never misses a key that exists)

Without Bloom Filters: read every SSTable file
With Bloom Filters:    skip ~99% of files on average
```

---

### LSM Tree: How a WRITE Works

```
INSERT key=42, val="Dave":

1. Append to WAL:  [LSN=500, INSERT, key=42, val="Dave"]  (sequential)
2. Insert into MemTable (Red-Black Tree):  O(log n), in memory
3. Return SUCCESS to client  ← already done!

No disk random write needed for the actual data.
WAL is sequential → very fast.
```

**Write amplification in LSM Trees:**
```
A key might be written multiple times as it moves through levels:
  MemTable → L0 → L1 → L2 → L3

Each compaction rewrites the data.
Typical write amplification: 10–30x
(But each write is sequential, so still faster than B-Tree random writes)
```

---

## B+ Tree vs LSM Tree: Head-to-Head

```
┌─────────────────────┬──────────────────────┬──────────────────────┐
│ Operation           │ B+ Tree              │ LSM Tree             │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Point Read          │ ✅ Fast (3-4 I/Os)   │ ⚠️ Slower (multi-   │
│                     │                      │    file check)       │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Range Scan          │ ✅ Excellent          │ ⚠️ OK (merge needed) │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Write (single row)  │ ⚠️ Random I/O        │ ✅ Sequential I/O    │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Write (bulk)        │ ⚠️ Many page splits  │ ✅ Very fast         │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Space efficiency    │ ✅ ~70% utilization  │ ⚠️ Space amplif.     │
│                     │                      │    (old versions)    │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Write amplification │ Low (2-5x)           │ High (10-30x)        │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Read amplification  │ Low (3-4 I/Os)       │ High (many files)    │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Background work     │ Minimal              │ Compaction (CPU/I/O) │
├─────────────────────┼──────────────────────┼──────────────────────┤
│ Best for            │ Read-heavy OLTP      │ Write-heavy workloads│
│                     │ Range queries        │ Time-series, logs    │
└─────────────────────┴──────────────────────┴──────────────────────┘
```

---

## Real-World Storage Engine Choices

```
PostgreSQL:
  - Storage: B+ Tree (heap files + index files)
  - Each table = heap file (unordered rows)
  - Indexes = separate B+ Tree files
  - MVCC: old row versions stored in heap (VACUUM cleans them)

MySQL InnoDB:
  - Storage: B+ Tree (clustered index)
  - Table IS the B+ Tree (rows stored in primary key order)
  - Secondary indexes point to primary key (not row address)
  - Advantage: Primary key lookups = 1 tree traversal

Cassandra:
  - Storage: LSM Tree
  - Optimized for write-heavy time-series data
  - Compaction strategies: STCS, LCS, TWCS

RocksDB (used by MySQL MyRocks, TiKV, CockroachDB):
  - Storage: LSM Tree
  - Facebook's fork of LevelDB
  - Highly tunable compaction

MongoDB WiredTiger:
  - Storage: B+ Tree (default) or LSM Tree (optional)
  - Switched from MMAP to WiredTiger in v3.2
```

---

## The Amplification Trade-offs Triangle

Every storage engine must balance three amplifications:

```
         Write Amplification
               /\
              /  \
             /    \
            /      \
           /________\
  Read              Space
  Amplification     Amplification

You can optimize for at most TWO of the three.

B+ Tree:   Low read amp  + Low write amp  → Higher space amp
LSM Tree:  Low write amp + Low space amp  → Higher read amp
```

---

## Key Takeaways

### B+ Tree:
- Self-balancing tree, all data in leaf nodes
- Leaf nodes linked → excellent range scans
- Height = log_b(N) → only 3-4 disk reads for billions of rows
- Writes cause random I/O (page splits)
- Best for: read-heavy OLTP, range queries

### LSM Tree:
- Writes go to MemTable (RAM) → flushed as immutable SSTables
- Compaction merges SSTables in background
- Bloom Filters avoid unnecessary file reads
- Writes are always sequential → very fast
- Reads must check multiple files → slower
- Best for: write-heavy workloads, time-series, event logs

### Choose based on your workload:
```
Read-heavy?   → B+ Tree (PostgreSQL, MySQL)
Write-heavy?  → LSM Tree (Cassandra, RocksDB)
Mixed?        → B+ Tree with tuning, or hybrid engines
```

---

## Test Your Understanding

**Q1:** Why does a B+ Tree with branching factor 1000 only need 3 levels for 1 billion rows?
**A:** log₁₀₀₀(1,000,000,000) = 3. Each level multiplies capacity by 1000.

**Q2:** Why are LSM Tree writes faster than B+ Tree writes?
**A:** LSM writes are always sequential (WAL + MemTable flush). B+ Tree writes are random I/O (must find and modify specific pages on disk).

**Q3:** What is a Bloom Filter and why does LSM Tree need it?
**A:** A probabilistic data structure that answers "is this key in this file?" with zero false negatives. LSM Trees need it because a key might be in any of many SSTable files — Bloom Filters let you skip files that definitely don't contain the key.

**Q4:** What is compaction and why is it necessary?
**A:** Compaction merges multiple SSTable files into one, keeping only the latest version of each key and removing tombstones. Without it, reads would get slower over time as more files accumulate.

---

**Next Up: Module 4 — Indexing Deep Dive**

We'll explore:
- How indexes are built on top of B+ Trees
- Primary vs Secondary indexes
- Composite indexes and how query planners use them
- Index-only scans, covering indexes
- When indexes hurt performance