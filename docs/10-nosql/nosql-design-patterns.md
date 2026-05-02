# Module 10: NoSQL Design Patterns

NoSQL databases aren't just "SQL without the SQL" — they require a fundamentally different way of thinking about data modeling. The core shift: **model your data around your queries, not around your entities**.

---

## The 4 NoSQL Families

```
┌─────────────────┬──────────────────────────────────────────────────────┐
│ Type            │ Examples                  │ Best For                 │
├─────────────────┼───────────────────────────┼──────────────────────────┤
│ Key-Value       │ Redis, DynamoDB, Riak     │ Sessions, caches, simple │
│                 │                           │ lookups by ID            │
├─────────────────┼───────────────────────────┼──────────────────────────┤
│ Document        │ MongoDB, CouchDB,         │ Semi-structured data,    │
│                 │ Firestore                 │ flexible schemas         │
├─────────────────┼───────────────────────────┼──────────────────────────┤
│ Column-Family   │ Cassandra, HBase,         │ Time-series, write-heavy,│
│                 │ ScyllaDB                  │ wide rows                │
├─────────────────┼───────────────────────────┼──────────────────────────┤
│ Graph           │ Neo4j, Amazon Neptune,    │ Relationships, social    │
│                 │ JanusGraph                │ networks, recommendations│
└─────────────────┴───────────────────────────┴──────────────────────────┘
```

---

## The Fundamental Shift: Query-Driven Design

### SQL (Relational) Approach
```
1. Model your entities (normalize data)
2. Define relationships (foreign keys)
3. Write queries to join them at read time

Schema first, queries later.
```

### NoSQL Approach
```
1. Define your access patterns (what queries will you run?)
2. Model your data to serve those queries directly
3. Denormalize aggressively

Queries first, schema follows.
```

**Example:**

```
SQL approach:
  users(id, name, email)
  posts(id, user_id, title, content)
  comments(id, post_id, user_id, text)

  Query: "Get post with author name and all comments"
  → JOIN users + posts + comments (3 tables)

NoSQL approach (MongoDB):
  {
    _id: "post:123",
    title: "Hello World",
    author: { id: "user:1", name: "Alice" },  ← embedded
    comments: [                                 ← embedded array
      { user: "Bob", text: "Great post!" },
      { user: "Carol", text: "Thanks!" }
    ]
  }

  Query: db.posts.findOne({_id: "post:123"})
  → Single document read, no joins needed
```

---

## Key-Value Stores (Redis, DynamoDB)

The simplest NoSQL model: `key → value`

```
SET user:1001 '{"name":"Alice","email":"alice@example.com"}'
GET user:1001

SET session:abc123 '{"user_id":1001,"expires":1714000000}'
GET session:abc123
```

### Redis Data Structures

Redis goes beyond simple strings — it has rich data types:

```
String:   SET counter 0 / INCR counter / GET counter
Hash:     HSET user:1001 name "Alice" email "alice@example.com"
          HGET user:1001 name
List:     LPUSH feed:1001 "post:500"  (prepend)
          LRANGE feed:1001 0 9        (get first 10)
Set:      SADD followers:1001 "user:2" "user:3"
          SISMEMBER followers:1001 "user:2"
Sorted Set: ZADD leaderboard 9500 "alice"
            ZRANGE leaderboard 0 9 WITHSCORES REV  (top 10)
```

### DynamoDB Key Design

DynamoDB has two key components:
```
Partition Key (PK): determines which shard/partition
Sort Key (SK):      orders items within a partition

Table: UserActivity
  PK=user:1001, SK=2024-01-15T10:00:00  → activity record
  PK=user:1001, SK=2024-01-15T11:00:00  → activity record
  PK=user:1001, SK=2024-01-16T09:00:00  → activity record

Query: "Get all activity for user:1001 in January 2024"
  PK = "user:1001"
  SK BETWEEN "2024-01-01" AND "2024-01-31"
  → Single partition scan, very fast!
```

**Single-table design (DynamoDB best practice):**
```
Store ALL entity types in ONE table using generic PK/SK:

PK              SK              Data
──────────────────────────────────────────────────────
USER#1001       PROFILE         {name, email, created}
USER#1001       ORDER#5001      {total, status, date}
USER#1001       ORDER#5002      {total, status, date}
ORDER#5001      ITEM#1          {product, qty, price}
ORDER#5001      ITEM#2          {product, qty, price}

Access patterns:
  "Get user profile"     → PK=USER#1001, SK=PROFILE
  "Get user's orders"    → PK=USER#1001, SK begins_with ORDER#
  "Get order items"      → PK=ORDER#5001, SK begins_with ITEM#
```

---

## Document Stores (MongoDB)

Documents are JSON-like objects. The key design decisions: **embed vs reference**.

### Embed When:
```
✓ Data is always accessed together
✓ Child data doesn't exist independently
✓ Array won't grow unboundedly
✓ 1:1 or 1:few relationships

Example: Blog post with comments
{
  _id: ObjectId("..."),
  title: "My Post",
  body: "...",
  author: { name: "Alice", avatar: "..." },  ← embed (always shown with post)
  tags: ["tech", "databases"],               ← embed (small, fixed)
  comments: [                                ← embed (shown with post)
    { user: "Bob", text: "Great!", date: "2024-01-15" }
  ]
}
```

### Reference When:
```
✓ Data is accessed independently
✓ Many-to-many relationships
✓ Array could grow very large (unbounded)
✓ Data is shared across many documents

Example: User with orders (orders accessed independently)
// users collection:
{ _id: "user:1001", name: "Alice", email: "alice@example.com" }

// orders collection:
{ _id: "order:5001", user_id: "user:1001", total: 99.99, items: [...] }
{ _id: "order:5002", user_id: "user:1001", total: 49.99, items: [...] }

// Query orders for user:
db.orders.find({ user_id: "user:1001" })
```

### The Unbounded Array Anti-Pattern

```
WRONG: Embedding unbounded arrays
{
  _id: "user:1001",
  name: "Alice",
  posts: ["post:1", "post:2", ... "post:100000"]  ← DANGER!
}

Problems:
  MongoDB document size limit = 16MB
  Loading user loads ALL post IDs (wasteful)
  Adding a post = update the user document (contention)

RIGHT: Reference from the child side
// posts collection:
{ _id: "post:1", author_id: "user:1001", title: "..." }
{ _id: "post:2", author_id: "user:1001", title: "..." }

// Query posts by user:
db.posts.find({ author_id: "user:1001" })
```

---

## Column-Family Stores (Cassandra)

Cassandra's data model is the most different from SQL. Understanding it is critical.

### Cassandra's Physical Storage

```
Table: user_activity

Partition Key: user_id
Clustering Key: activity_time (determines sort order within partition)

Physical storage (one partition = one row on disk):

user_id=1001:
  [2024-01-15 10:00] → {type: "login",  ip: "1.2.3.4"}
  [2024-01-15 11:30] → {type: "purchase", amount: 99}
  [2024-01-16 09:00] → {type: "login",  ip: "1.2.3.5"}

user_id=1002:
  [2024-01-15 08:00] → {type: "login",  ip: "5.6.7.8"}
  [2024-01-15 14:00] → {type: "logout"}
```

**Key insight:** All data for one partition key is stored together on disk. Queries within a partition are extremely fast (sequential read). Queries across partitions require scatter-gather.

### Cassandra Data Modeling Rules

```
Rule 1: Model around your queries
  Define access patterns FIRST, then design tables.

Rule 2: Denormalize aggressively
  Duplicate data across multiple tables to serve different queries.
  Storage is cheap. Cross-partition queries are expensive.

Rule 3: One table per query pattern
  If you have 3 different access patterns → 3 tables (possibly)

Rule 4: Avoid unbounded partitions
  A partition that grows forever → performance degrades
  Add a time bucket to the partition key:
    PK = (user_id, year_month)  instead of just user_id
```

### Cassandra Modeling Example

**Requirement:** Social media app
```
Access patterns:
  1. Get user profile by user_id
  2. Get all posts by a user (newest first)
  3. Get posts in a user's feed (posts from people they follow)
  4. Get comments on a post
```

**Table 1: User profiles**
```sql
CREATE TABLE users (
  user_id UUID PRIMARY KEY,
  name TEXT,
  email TEXT,
  bio TEXT
);
-- Access pattern 1: SELECT * FROM users WHERE user_id = ?
```

**Table 2: Posts by user**
```sql
CREATE TABLE posts_by_user (
  user_id UUID,
  created_at TIMESTAMP,
  post_id UUID,
  content TEXT,
  PRIMARY KEY (user_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
-- Access pattern 2: SELECT * FROM posts_by_user WHERE user_id = ?
-- Returns posts newest first (clustering order)
```

**Table 3: User feed**
```sql
CREATE TABLE user_feed (
  viewer_id UUID,
  created_at TIMESTAMP,
  post_id UUID,
  author_id UUID,
  author_name TEXT,   ← denormalized! (copied from users table)
  content TEXT,       ← denormalized! (copied from posts table)
  PRIMARY KEY (viewer_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
-- Access pattern 3: SELECT * FROM user_feed WHERE viewer_id = ?
-- When Alice posts → write to feed of ALL her followers
-- (fan-out on write)
```

**Table 4: Comments by post**
```sql
CREATE TABLE comments_by_post (
  post_id UUID,
  created_at TIMESTAMP,
  comment_id UUID,
  user_id UUID,
  user_name TEXT,   ← denormalized
  text TEXT,
  PRIMARY KEY (post_id, created_at)
) WITH CLUSTERING ORDER BY (created_at ASC);
-- Access pattern 4: SELECT * FROM comments_by_post WHERE post_id = ?
```

**Notice:** `author_name` and `content` are duplicated across tables. This is intentional and correct in Cassandra.

---

## Graph Databases (Neo4j)

When your data IS the relationships:

```
SQL for "friends of friends who like the same movies":
  SELECT DISTINCT u3.name
  FROM users u1
  JOIN friendships f1 ON u1.id = f1.user_id
  JOIN users u2 ON f1.friend_id = u2.id
  JOIN friendships f2 ON u2.id = f2.user_id
  JOIN users u3 ON f2.friend_id = u3.id
  JOIN movie_likes ml1 ON u1.id = ml1.user_id
  JOIN movie_likes ml2 ON u3.id = ml2.user_id
  WHERE u1.id = 1001
    AND ml1.movie_id = ml2.movie_id
    AND u3.id != u1.id;

Neo4j Cypher for the same query:
  MATCH (me:User {id: 1001})-[:FRIEND]->()-[:FRIEND]->(fof:User)
  WHERE (me)-[:LIKES]->(:Movie)<-[:LIKES]-(fof)
  RETURN DISTINCT fof.name

Graph traversal is O(relationships) not O(table size).
For deep relationship queries, graphs are 100-1000x faster than SQL.
```

**Use graph databases for:**
```
✓ Social networks (friends, followers, connections)
✓ Recommendation engines ("people who bought X also bought Y")
✓ Fraud detection (transaction networks)
✓ Knowledge graphs
✓ Access control (role hierarchies)
```

---

## When to Use What

```
Use SQL (PostgreSQL, MySQL) when:
  ✓ Complex queries with JOINs across many entities
  ✓ Strong ACID transactions required
  ✓ Schema is well-defined and stable
  ✓ Reporting / analytics queries
  ✓ Team knows SQL well
  Examples: Financial systems, ERP, e-commerce backend

Use Key-Value (Redis, DynamoDB) when:
  ✓ Simple lookups by ID
  ✓ Caching layer
  ✓ Session storage
  ✓ Rate limiting, counters
  ✓ Need extreme speed (Redis: microsecond latency)

Use Document (MongoDB) when:
  ✓ Semi-structured or variable schema
  ✓ Data naturally fits in hierarchical documents
  ✓ Rapid iteration / schema changes expected
  ✓ Content management, catalogs, user profiles

Use Column-Family (Cassandra) when:
  ✓ Write-heavy workloads (millions of writes/sec)
  ✓ Time-series data (IoT, metrics, logs, events)
  ✓ Need linear horizontal scalability
  ✓ Access patterns are well-defined and stable
  ✓ Can tolerate eventual consistency

Use Graph (Neo4j) when:
  ✓ Data is highly connected
  ✓ Queries traverse relationships (depth > 2)
  ✓ Relationship types are first-class data
```

---

## The Polyglot Persistence Pattern

Real production systems use MULTIPLE databases:

```
E-commerce platform:
  PostgreSQL  → Orders, payments, inventory (ACID required)
  MongoDB     → Product catalog (flexible schema, nested attributes)
  Redis       → Sessions, cart, rate limiting (speed)
  Cassandra   → User activity, clickstream (write-heavy, time-series)
  Elasticsearch → Product search (full-text, faceted search)
  Neo4j       → Product recommendations (graph traversal)

Each database does what it's best at.
The application integrates them.
```

---

## Key Takeaways

```
NoSQL core principle:
  Model data around queries, not entities
  Denormalization is a feature, not a bug

Key-Value:
  Simplest model, fastest access
  Redis: rich data structures (lists, sets, sorted sets)
  DynamoDB: single-table design with PK+SK

Document:
  Embed for data always accessed together
  Reference for independent or unbounded data
  Avoid unbounded arrays in documents

Column-Family (Cassandra):
  One table per query pattern
  Partition key = unit of distribution
  Clustering key = sort order within partition
  Denormalize aggressively, fan-out on write

Graph:
  Use when relationships ARE the data
  Traversal queries are orders of magnitude faster than SQL JOINs

Polyglot persistence:
  Use the right tool for each job
  Most production systems use 3-5 different databases
```

---

**Next Up: Module 11 — Build Our Own Database (Final Project)**

We've learned everything we need. Now we build:
- A simple key-value store with a WAL for durability
- An LSM-Tree storage engine (like LevelDB/RocksDB)
- A basic query parser
- Replication to a replica node
- Written in Python (readable, no boilerplate)