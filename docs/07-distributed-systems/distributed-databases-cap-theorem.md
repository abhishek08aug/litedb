# Module 7: Distributed Databases & CAP Theorem

When a single machine can no longer handle your data or traffic, you go distributed. This module covers the fundamental theory and real-world trade-offs of distributed databases.

---

## Why Go Distributed?

```
Single-node database limits:
  Storage:    Max ~100TB on one machine
  Throughput: Max ~100K TPS on one machine
  Latency:    Users far from server get high latency
  Availability: One machine = one point of failure

Solutions:
  Scale UP (vertical):   Bigger machine, more RAM/CPU/disk
                         → Expensive, has hard limits

  Scale OUT (horizontal): More machines working together
                          → Theoretically unlimited scale
                          → But introduces new problems!
```

**New problems in distributed systems:**
```
1. Network partitions: machines can't talk to each other
2. Partial failures: some nodes fail, others don't
3. Clock skew: clocks on different machines drift apart
4. Consistency: how to keep data in sync across nodes?
5. Coordination: who's in charge? how to agree on anything?
```

---

## CAP Theorem

Proposed by Eric Brewer (2000), proved by Gilbert & Lynch (2002):

> **A distributed system can guarantee at most 2 of these 3 properties simultaneously:**

```
        Consistency (C)
            /\
           /  \
          /    \
         /      \
        /________\
  Availability   Partition
      (A)        Tolerance (P)
```

### The Three Properties

**Consistency (C):**
```
Every read receives the most recent write or an error.
All nodes see the same data at the same time.

NOT the same as ACID consistency!
CAP-C = "linearizability" or "strong consistency"
```

**Availability (A):**
```
Every request receives a response (not an error).
The system keeps serving requests even if some nodes fail.
Note: response doesn't have to be the latest data.
```

**Partition Tolerance (P):**
```
The system continues operating even when network
messages between nodes are lost or delayed.

A "partition" = network split where nodes can't communicate.
```

### The Catch: P is Not Optional

```
In any real distributed system, network partitions WILL happen.
  - Network cables get cut
  - Switches fail
  - Data centers lose connectivity
  - AWS availability zones go down

Therefore: P is mandatory.
Real choice is between C and A during a partition.

CA systems: Only work if there's never a partition
            → Only possible on a single machine!
            → Not truly distributed.
```

### The Real Choice: CP vs AP

```
CP (Consistency + Partition Tolerance):
  During a partition → refuse requests (return error)
  → Never serve stale data
  → Some requests fail
  Examples: HBase, Zookeeper, etcd, MongoDB (default)

AP (Availability + Partition Tolerance):
  During a partition → serve requests with possibly stale data
  → Always respond, even if data is old
  → Eventually consistent
  Examples: Cassandra, DynamoDB, CouchDB, Riak
```

**Concrete scenario:**

```
Setup: 2 nodes (Node A, Node B), both have balance=1000

Network partition occurs: A and B can't communicate

Client writes to Node A: balance = 800

Now client reads from Node B:

CP system: Node B returns ERROR (can't guarantee consistency)
AP system: Node B returns 1000 (stale, but available)

Partition heals → both systems eventually sync to balance=800
```

---

## CAP is Too Simplistic: Enter PACELC

CAP only talks about behavior during partitions. **PACELC** (Daniel Abadi, 2012) extends it:

```
PACELC:
  If Partition:  choose between Availability and Consistency
  Else (normal): choose between Latency and Consistency

Full form: PAC-ELC

During partition:  A vs C  (same as CAP)
During normal op:  L vs C  (new insight!)
```

**Why latency vs consistency matters:**

```
Strong consistency requires coordination between nodes:
  Write to Node A → wait for Node B to confirm → return success
  → Higher latency (network round trip)
  → But all nodes agree

Eventual consistency skips coordination:
  Write to Node A → return success immediately
  → Lower latency
  → Node B might be stale for a moment
```

**PACELC classification of real systems:**

```
System          Partition   Normal Op    Classification
──────────────────────────────────────────────────────
DynamoDB        A           L            PA/EL
Cassandra       A           L            PA/EL
Riak            A           L            PA/EL
MongoDB         C           C            PC/EC
HBase           C           C            PC/EC
MySQL (single)  C           C            PC/EC
Zookeeper       C           C            PC/EC
CockroachDB     C           L            PC/EL
YugabyteDB      C           L            PC/EL
```

---

## Consistency Models (Spectrum)

Not all consistency is binary. There's a spectrum:

```
STRONG ←────────────────────────────────────────→ WEAK

Linearizability → Sequential → Causal → Eventual
(strongest)                              (weakest)
```

### 1. Linearizability (Strongest)

```
Every operation appears to take effect instantaneously
at some point between its start and end.

All clients see the same order of operations.
Reads always return the latest write.

Cost: High latency (requires coordination)
Used by: etcd, Zookeeper, Google Spanner
```

### 2. Sequential Consistency

```
All operations appear in some sequential order.
Each client's operations appear in the order they issued them.
But different clients may see different orderings.

Weaker than linearizability (no real-time guarantee).
```

### 3. Causal Consistency

```
Operations that are causally related appear in the same order
to all nodes. Concurrent operations may appear in any order.

"If A caused B, everyone sees A before B."

Example:
  Alice posts: "I'm going to the store"
  Bob replies: "Bring milk!"

Causal consistency guarantees everyone sees Alice's post
before Bob's reply. (They're causally related.)

Used by: MongoDB (causal sessions), some Cassandra configs
```

### 4. Eventual Consistency (Weakest)

```
If no new updates are made, all replicas will eventually
converge to the same value.

No guarantee on WHEN they converge.
No guarantee on what you read in the meantime.

Used by: Cassandra, DynamoDB, DNS
```

---

## Consensus: How Distributed Nodes Agree

The hardest problem in distributed systems: **how do multiple nodes agree on a single value** when messages can be lost and nodes can fail?

### The Problem

```
3 nodes must agree on "who is the leader":

Node A says: "I'm the leader!"
Node B says: "I'm the leader!"
Node C says: "I'm the leader!"

Network partition: A can't talk to B or C

Who wins? How do we prevent split-brain?
(Split-brain = two nodes both think they're leader → data corruption)
```

### Quorum: The Key Insight

```
Quorum = majority of nodes must agree

With N nodes, quorum = floor(N/2) + 1

N=3: quorum = 2  (majority of 3)
N=5: quorum = 3  (majority of 5)

Why quorum works:
  Any two quorums must overlap by at least 1 node.
  That overlapping node carries the latest information.

Write quorum: W nodes must confirm write
Read quorum:  R nodes must respond to read
Consistency:  W + R > N  (read and write quorums overlap)

Example (N=3, W=2, R=2):
  Write: 2 nodes confirm → success
  Read:  2 nodes respond → at least 1 has latest write
  W + R = 4 > 3 = N ✓
```

### Raft Consensus Algorithm

Raft (2014) is the most widely used consensus algorithm today (etcd, CockroachDB, TiKV, Consul):

```
Raft roles:
  Leader:    Handles all client requests, replicates to followers
  Follower:  Passive, replicates from leader
  Candidate: Trying to become leader (during election)

Normal operation:
  1. Client sends write to Leader
  2. Leader appends to its log
  3. Leader sends log entry to all Followers
  4. Followers append to their logs, send ACK
  5. Once quorum (majority) ACK → Leader commits
  6. Leader responds to client
  7. Leader tells Followers to commit

Leader election (when leader fails):
  1. Follower times out (no heartbeat from leader)
  2. Follower becomes Candidate, increments term
  3. Candidate requests votes from all nodes
  4. Nodes vote for first candidate they hear from (per term)
  5. Candidate with quorum votes → becomes new Leader
  6. New Leader starts sending heartbeats
```

**Raft log replication:**
```
Leader log:  [1: SET x=1] [2: SET y=2] [3: SET x=5]
                                              ↑ committed (quorum ACK'd)

Follower A:  [1: SET x=1] [2: SET y=2] [3: SET x=5]  ← in sync
Follower B:  [1: SET x=1] [2: SET y=2]               ← behind (will catch up)

If leader fails after entry 3 is committed:
  New leader elected from A or B
  A has all committed entries → can be leader
  B is missing entry 3 → will get it from new leader
```

### Paxos vs Raft

```
Paxos (1989, Lamport):
  Theoretically elegant, notoriously hard to understand
  Many variants: Basic Paxos, Multi-Paxos, Fast Paxos
  Used by: Google Chubby, Google Spanner (internally)

Raft (2014, Ongaro & Ousterhout):
  Designed to be understandable
  Same safety guarantees as Paxos
  Used by: etcd, CockroachDB, TiKV, Consul, RethinkDB
  "Raft is Paxos made understandable"
```

---

## Google Spanner: Having It All?

Google Spanner (2012) claims to be a globally distributed CP database with high availability. How?

```
Key innovation: TrueTime API

Problem: Distributed systems can't agree on "now"
  (clocks drift, network delays vary)

TrueTime: GPS + atomic clocks in every data center
  Returns: [earliest, latest] — a time interval, not a point
  Uncertainty: typically 1-7 milliseconds

Spanner's trick:
  Before committing, wait out the uncertainty interval
  → Guarantees that commit timestamp is in the past
  → External consistency: if T2 starts after T1 commits,
    T2's timestamp > T1's timestamp

Result:
  Globally consistent reads across continents
  At the cost of: 7ms commit latency (waiting for TrueTime)

Lesson: You CAN have strong consistency globally,
        but you pay in latency, not availability.
```

---

## Real-World System Choices

```
Use case: Financial transactions (bank transfers)
  Need: Strong consistency (can't lose money)
  Choose: CP system (PostgreSQL, CockroachDB, Spanner)

Use case: Social media likes/views counter
  Need: High availability, eventual consistency OK
  Choose: AP system (Cassandra, DynamoDB)

Use case: Shopping cart
  Need: Always available (can't lose cart), eventual OK
  Choose: AP system (Amazon Dynamo paper inspired this)

Use case: Distributed lock / leader election
  Need: Strong consistency (split-brain = disaster)
  Choose: CP system (etcd, Zookeeper)

Use case: DNS
  Need: Always available, stale data OK for seconds
  Choose: AP system (eventual consistency)

Use case: User sessions
  Need: Available, stale by seconds OK
  Choose: AP system (Redis with replication)
```

---

## Key Takeaways

```
CAP Theorem:
  Can't have C + A + P simultaneously
  P is mandatory in real distributed systems
  Real choice: CP (consistency) vs AP (availability)

PACELC extends CAP:
  Normal operation: Latency vs Consistency trade-off
  Strong consistency = coordination = higher latency

Consistency spectrum:
  Linearizability > Sequential > Causal > Eventual

Quorum:
  Majority must agree for safety
  W + R > N for strong consistency

Raft:
  Leader-based consensus
  Log replication to quorum before commit
  Leader election on failure

Choose based on your invariants:
  "Can I serve stale data?" → AP
  "Must data always be correct?" → CP
```

---

**Next Up: Module 8 — Sharding & Partitioning**

We'll explore:
- Horizontal partitioning: how to split data across nodes
- Range sharding vs Hash sharding vs Directory-based
- Consistent hashing (how Cassandra and DynamoDB do it)
- Hotspots and how to avoid them
- Resharding: the hardest problem in distributed databases