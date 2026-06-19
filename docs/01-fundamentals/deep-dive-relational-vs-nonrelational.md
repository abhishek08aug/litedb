# Deep Dive: Relational vs Non-Relational Databases

This document explains what fundamentally makes a database "relational" versus "non-relational."

## What Makes a Database "Relational"?

The term "relational" comes from **relational algebra** and **relational theory**, developed by Edgar F. Codd in 1970. Here are the core characteristics:

### 1. Data is Organized in Relations (Tables)

A **relation** is a mathematical term for what we commonly call a "table." Each relation has:
- **Rows (Tuples)**: Individual records
- **Columns (Attributes)**: Properties of the data
- **Schema**: Fixed structure defined upfront

**Example:**
```
Users Table (Relation)
┌────────┬───────────┬─────────────────────┬─────┐
│ UserID │   Name    │       Email         │ Age │
├────────┼───────────┼─────────────────────┼─────┤
│   1    │  Alice    │ alice@example.com   │ 28  │
│   2    │  Bob      │ bob@example.com     │ 35  │
│   3    │  Charlie  │ charlie@example.com │ 42  │
└────────┴───────────┴─────────────────────┴─────┘

Posts Table (Relation)
┌────────┬────────┬──────────────────────┬────────────┐
│ PostID │ UserID │      Content         │    Date    │
├────────┼────────┼──────────────────────┼────────────┤
│  101   │   1    │ "Hello World!"       │ 2024-01-15 │
│  102   │   2    │ "Learning databases" │ 2024-01-16 │
│  103   │   1    │ "SQL is powerful"    │ 2024-01-17 │
└────────┴────────┴──────────────────────┴────────────┘
```

### 2. Relationships Between Tables

The "relational" in relational databases refers to **relationships between tables** using keys:

**Primary Key**: Uniquely identifies each row
- In Users table: `UserID` is the primary key

**Foreign Key**: Links to another table's primary key
- In Posts table: `UserID` is a foreign key referencing Users table

**Visual Relationship:**
```
Users Table                    Posts Table
┌────────┐                    ┌────────┐
│ UserID │◄───────────────────│ UserID │ (Foreign Key)
│  Name  │                    │ PostID │
│  Email │                    │Content │
└────────┘                    └────────┘
```

This creates a **relationship**: "A User can have many Posts"

### 3. ACID Guarantees

Relational databases strictly enforce **ACID properties**:
- **Atomicity**: All or nothing transactions
- **Consistency**: Data follows all rules and constraints
- **Isolation**: Concurrent transactions don't interfere
- **Durability**: Committed data survives crashes

**Example Transaction:**
```sql
BEGIN TRANSACTION;
  UPDATE accounts SET balance = balance - 100 WHERE user_id = 1;
  UPDATE accounts SET balance = balance + 100 WHERE user_id = 2;
COMMIT;
```
Either both updates happen, or neither does. There is no partial state.

### 4. Schema Enforcement

Every table has a **fixed schema** defined upfront:

```sql
CREATE TABLE Users (
    UserID INT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    Email VARCHAR(255) UNIQUE NOT NULL,
    Age INT CHECK (Age >= 0)
);
```

**Rules enforced:**
- `UserID` must be unique and not null
- `Name` cannot be null
- `Email` must be unique
- `Age` must be non-negative

Any attempt to insert data that violates these rules is rejected by the database.

### 5. SQL (Structured Query Language)

Relational databases use SQL for querying:

```sql
-- Join two tables to get user posts
SELECT Users.Name, Posts.Content
FROM Users
INNER JOIN Posts ON Users.UserID = Posts.UserID
WHERE Users.Age > 30;
```

SQL is **declarative**: it specifies *what* result is required, not *how* to obtain it.

---

## What Makes a Database "Non-Relational" (NoSQL)?

NoSQL databases **don't follow the relational model**. They sacrifice some relational features for flexibility, scalability, or performance.

### Key Characteristics of NoSQL:

#### 1. Flexible or No Schema

Data can have different structures in the same collection:

**MongoDB Example (Document Store):**
```javascript
// User 1 - has age
{
  "_id": "user1",
  "name": "Alice",
  "email": "alice@example.com",
  "age": 28,
  "posts": [
    {"content": "Hello World!", "date": "2024-01-15"}
  ]
}

// User 2 - no age, has location
{
  "_id": "user2",
  "name": "Bob",
  "email": "bob@example.com",
  "location": "New York",
  "posts": [
    {"content": "Learning databases", "date": "2024-01-16"}
  ]
}
```

Notice:
- User 1 has `age`, User 2 doesn't
- User 2 has `location`, User 1 doesn't
- Posts are **embedded** (denormalized) instead of in a separate table

#### 2. Different Data Models

NoSQL databases use various data models:

**A. Document Stores (MongoDB, CouchDB)**
```javascript
// Nested, JSON-like documents
{
  "user": "Alice",
  "profile": {
    "age": 28,
    "address": {
      "city": "Boston",
      "zip": "02101"
    }
  }
}
```

**B. Key-Value Stores (Redis, DynamoDB)**
```
Key: "user:1"        Value: {"name": "Alice", "age": 28}
Key: "session:abc"   Value: {"userId": 1, "expires": 1234567890}
Key: "cart:user1"    Value: ["item1", "item2", "item3"]
```

**C. Column-Family Stores (Cassandra, HBase)**
```
Row Key: "user1"
├── Column Family: "profile"
│   ├── name: "Alice"
│   ├── age: 28
│   └── email: "alice@example.com"
└── Column Family: "activity"
    ├── last_login: "2024-01-15"
    └── post_count: 42
```

**D. Graph Databases (Neo4j)**
```
(Alice)-[:FOLLOWS]->(Bob)
(Alice)-[:POSTED]->(Post1)
(Bob)-[:LIKES]->(Post1)
```

#### 3. Eventual Consistency (Often)

Many NoSQL databases trade **strong consistency** for **availability** and **partition tolerance** (CAP theorem, covered in a later module).

**Example:**
```
Time: T0
Server 1: User balance = $100
Server 2: User balance = $100

Time: T1
User withdraws $20 from Server 1
Server 1: User balance = $80
Server 2: User balance = $100  ← Still old value!

Time: T2 (after replication)
Server 1: User balance = $80
Server 2: User balance = $80  ← Eventually consistent!
```

This is called **eventual consistency**: All replicas will eventually have the same data, but not immediately.

#### 4. Denormalization

NoSQL often **embeds related data** instead of using foreign keys:

**Relational Approach (Normalized):**
```sql
Users Table: [UserID, Name, Email]
Posts Table: [PostID, UserID, Content]
```

**NoSQL Approach (Denormalized):**
```javascript
{
  "userId": 1,
  "name": "Alice",
  "email": "alice@example.com",
  "posts": [
    {"postId": 101, "content": "Hello World!"},
    {"postId": 102, "content": "SQL is powerful"}
  ]
}
```

**Trade-off:**
- ✅ Faster reads (one query instead of join)
- ❌ Data duplication (if Alice's email changes, update multiple documents)

---

## Side-by-Side Comparison

| Aspect | Relational (SQL) | Non-Relational (NoSQL) |
|--------|------------------|------------------------|
| **Data Model** | Tables with rows and columns | Documents, Key-Value, Columns, Graphs |
| **Schema** | Fixed, defined upfront | Flexible or schema-less |
| **Relationships** | Foreign keys, JOINs | Embedded data or references |
| **ACID** | Strong ACID guarantees | Often eventual consistency |
| **Scaling** | Vertical (bigger servers) | Horizontal (more servers) |
| **Query Language** | SQL (standardized) | Varies by database |
| **Best For** | Complex queries, transactions | High throughput, flexible data |

---

## Real-World Example: E-Commerce System

### Relational Approach:

```sql
-- Normalized structure
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    Email VARCHAR(255)
);

CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT FOREIGN KEY REFERENCES Customers(CustomerID),
    OrderDate DATE,
    TotalAmount DECIMAL(10,2)
);

CREATE TABLE OrderItems (
    OrderItemID INT PRIMARY KEY,
    OrderID INT FOREIGN KEY REFERENCES Orders(OrderID),
    ProductID INT,
    Quantity INT,
    Price DECIMAL(10,2)
);

-- Query requires JOINs
SELECT c.Name, o.OrderDate, oi.ProductID, oi.Quantity
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
JOIN OrderItems oi ON o.OrderID = oi.OrderID
WHERE c.CustomerID = 1;
```

### NoSQL Approach (MongoDB):

```javascript
// Denormalized document
{
  "_id": "customer1",
  "name": "Alice",
  "email": "alice@example.com",
  "orders": [
    {
      "orderId": "order123",
      "orderDate": "2024-01-15",
      "totalAmount": 150.00,
      "items": [
        {"productId": "prod1", "quantity": 2, "price": 50.00},
        {"productId": "prod2", "quantity": 1, "price": 50.00}
      ]
    }
  ]
}

// Single query - no JOINs needed
db.customers.findOne({"_id": "customer1"})
```

---

## When to Use Which?

### Use Relational (SQL) When:
- ✅ Data has clear relationships and structure
- ✅ Strong ACID guarantees are required (banking, finance)
- ✅ Complex queries with JOINs are common
- ✅ Data integrity is critical
- ✅ Schema is stable and well-defined

**Examples:** Banking systems, ERP, CRM, inventory management

### Use Non-Relational (NoSQL) When:
- ✅ Schema changes frequently
- ✅ Need to scale horizontally across many servers
- ✅ High write throughput is critical
- ✅ Data is hierarchical or graph-like
- ✅ Can tolerate eventual consistency

**Examples:** Social media feeds, real-time analytics, IoT data, content management

---

## The Hybrid Approach

Modern applications often use **both**:

```
E-Commerce Application
├── PostgreSQL (Relational)
│   └── User accounts, orders, payments (needs ACID)
├── MongoDB (Document Store)
│   └── Product catalog, reviews (flexible schema)
├── Redis (Key-Value)
│   └── Session data, caching (fast access)
└── Elasticsearch (Search Engine)
    └── Product search (full-text search)
```

This is called **polyglot persistence**: using the right database for each job.

---

## Key Takeaway

**"Relational"** means:
1. Data organized in tables with relationships
2. Fixed schema with constraints
3. Strong ACID guarantees
4. SQL for querying

**"Non-Relational"** means:
1. Flexible data models (not just tables)
2. Schema-less or flexible schema
3. Often eventual consistency
4. Optimized for specific use cases

Neither is "better"; they solve different problems.

---

**Review questions:**
1. Why might a social media app use NoSQL for posts but SQL for payments?
2. What is the trade-off of embedding data (denormalization) in NoSQL?
3. In what scenario would a system need both SQL and NoSQL?