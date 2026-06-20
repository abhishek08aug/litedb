package com.litedb.demo;

import com.litedb.auth.AuthManager;
import com.litedb.auth.AuthManager.*;
import com.litedb.btree.BPlusTree;
import com.litedb.memtable.MemTable;
import com.litedb.metrics.MetricsRegistry;
import com.litedb.metrics.MetricsRegistry.*;
import com.litedb.raft.RaftNode;
import com.litedb.sharding.ConsistentHashRing;
import com.litedb.sql.SQLParser;
import com.litedb.sql.SQLParser.*;
import com.litedb.txn.MVCCStore;
import com.litedb.wal.WriteAheadLog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * RunDemo — End-to-end integration demo of LiteDB.
 *
 * This ties together every module we built:
 *
 *   1. WAL          — durability: write-ahead log before any mutation
 *   2. MemTable     — in-memory sorted write buffer
 *   3. B+ Tree      — index structure for fast lookups and range scans
 *   4. MVCC         — snapshot isolation for concurrent transactions
 *   5. SQL Parser   — parse SQL text into AST
 *   6. Auth         — authenticate users, enforce RBAC permissions
 *   7. Metrics      — track QPS, latency, error rate
 *   8. Consistent Hashing — route keys to shards
 *   9. Raft         — consensus across a 3-node cluster
 *
 * This is the "putting it all together" moment — you now have a working
 * skeleton of a database engine built entirely from first principles.
 */
public class RunDemo {

    // ------------------------------------------------------------------ //
    //  Helpers                                                            //
    // ------------------------------------------------------------------ //

    private static void banner(String title) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("  " + title);
        System.out.println("=".repeat(60));
    }

    private static void step(int n, String desc) {
        System.out.println("\n[Step " + n + "] " + desc);
        System.out.println("-".repeat(50));
    }

    // ------------------------------------------------------------------ //
    //  Main                                                               //
    // ------------------------------------------------------------------ //

    public static void main(String[] args) throws Exception {
        banner("LiteDB — Full Integration Demo");
        System.out.println("  Building a database from scratch in Java.");
        System.out.println("  Every module implemented from first principles.\n");

        MetricsRegistry metrics = new MetricsRegistry();
        MetricsRegistry.Timer   opTimer  = metrics.timer("litedb_op_duration_ms");
        MetricsRegistry.Counter opCount  = metrics.counter("litedb_ops_total");
        MetricsRegistry.Counter errCount = metrics.counter("litedb_errors_total");

        // ============================================================
        // MODULE 1: Authentication
        // ============================================================
        banner("MODULE 1: Authentication & Authorization");

        AuthManager auth = new AuthManager(300_000);
        auth.createUser("admin",   "admin123",  Role.ADMIN);
        auth.createUser("alice",   "alice456",  Role.READ_WRITE);
        auth.createUser("bob",     "bob789",    Role.READ_ONLY);

        step(1, "Login as alice (READ_WRITE)");
        Session aliceSession = auth.login("alice", "alice456");
        System.out.println("  Session: " + aliceSession);

        step(2, "Authorization checks for alice");
        for (Permission p : Permission.values()) {
            System.out.printf("  alice can %-8s: %b%n", p, auth.canDo(aliceSession.token, p));
        }

        step(3, "Reject wrong password");
        try {
            auth.login("alice", "wrongpass");
        } catch (AuthManager.AuthException e) {
            System.out.println("  Correctly rejected: " + e.getMessage());
            errCount.inc();
        }

        // ============================================================
        // MODULE 2: WAL + MemTable
        // ============================================================
        banner("MODULE 2: WAL + MemTable (Write Path)");

        Path walFile = Files.createTempFile("litedb-wal-demo", ".wal");
        WriteAheadLog wal = new WriteAheadLog(walFile.toString());

        step(4, "Write records through WAL → MemTable");
        MemTable memTable = new MemTable(1024 * 1024); // 1MB

        String[][] records = {
            {"user:1", "Alice,30,alice@example.com"},
            {"user:2", "Bob,25,bob@example.com"},
            {"user:3", "Charlie,35,charlie@example.com"},
            {"user:4", "Diana,28,diana@example.com"},
            {"user:5", "Eve,22,eve@example.com"},
        };

        for (String[] rec : records) {
            try (MetricsRegistry.Timer.Sample s = opTimer.start()) {
                wal.appendSet(rec[0], rec[1]);
                memTable.set(rec[0], rec[1]);
                opCount.inc();
            }
            System.out.println("  WAL+MemTable: SET " + rec[0] + " = " + rec[1]);
        }

        step(5, "Read from MemTable");
        System.out.println("  GET user:1 = " + memTable.get("user:1"));
        System.out.println("  GET user:3 = " + memTable.get("user:3"));
        System.out.println("  GET user:9 = " + memTable.get("user:9") + " (not found)");

        // ============================================================
        // MODULE 3: B+ Tree Index
        // ============================================================
        banner("MODULE 3: B+ Tree Index");

        step(6, "Build B+ Tree index on user data");
        BPlusTree index = new BPlusTree();
        for (String[] rec : records) {
            index.insert(rec[0], rec[1]);
        }
        System.out.println("  Index size: " + index.size() + " entries");

        step(7, "Point lookup via B+ Tree");
        System.out.println("  GET user:2 = " + index.get("user:2"));
        System.out.println("  GET user:4 = " + index.get("user:4"));

        step(8, "Range scan user:2 to user:4");
        for (Map.Entry<String, String> e : index.range("user:2", "user:4")) {
            System.out.println("  " + e.getKey() + " → " + e.getValue());
        }

        // ============================================================
        // MODULE 4: MVCC Transactions
        // ============================================================
        banner("MODULE 4: MVCC Transactions");

        step(9, "Concurrent transactions with snapshot isolation");
        MVCCStore mvcc = new MVCCStore();

        // Seed data
        MVCCStore.Transaction seed = mvcc.begin();
        seed.set("balance:alice", "1000");
        seed.set("balance:bob",   "500");
        seed.commit();

        // Concurrent transfer: alice → bob
        MVCCStore.Transaction txA = mvcc.begin();
        MVCCStore.Transaction txB = mvcc.begin(); // starts before txA commits

        System.out.println("  txB reads alice balance (before txA commits): "
                + txB.get("balance:alice"));

        txA.set("balance:alice", "800");  // debit 200
        txA.set("balance:bob",   "700");  // credit 200
        txA.commit();

        System.out.println("  txA committed transfer: alice 1000→800, bob 500→700");
        System.out.println("  txB still sees old snapshot: alice=" + txB.get("balance:alice")
                + ", bob=" + txB.get("balance:bob"));
        txB.rollback();

        MVCCStore.Transaction txC = mvcc.begin();
        System.out.println("  New txC sees committed values: alice=" + txC.get("balance:alice")
                + ", bob=" + txC.get("balance:bob"));
        txC.rollback();

        // ============================================================
        // MODULE 5: SQL Parser
        // ============================================================
        banner("MODULE 5: SQL Parser");

        step(10, "Parse SQL queries into AST");
        SQLParser parser = new SQLParser();
        String[] queries = {
            "SELECT * FROM users WHERE age > 25",
            "INSERT INTO users (id, name) VALUES (6, 'Frank')",
            "UPDATE users SET name = 'Alice Smith' WHERE id = 1",
            "DELETE FROM users WHERE id = 5",
            "CREATE TABLE orders (id INT, user_id INT, total FLOAT)",
        };
        for (String sql : queries) {
            Statement stmt = parser.parse(sql);
            System.out.println("  [" + stmt.type() + "] " + stmt);
        }

        // ============================================================
        // MODULE 6: Consistent Hashing
        // ============================================================
        banner("MODULE 6: Consistent Hashing (Sharding)");

        step(11, "Route keys across 3 shards");
        ConsistentHashRing ring = new ConsistentHashRing(100);
        ring.addNode("shard-1");
        ring.addNode("shard-2");
        ring.addNode("shard-3");

        String[] shardKeys = {"user:1","user:2","user:3","user:4","user:5",
                              "order:100","order:200","product:99"};
        for (String key : shardKeys) {
            List<String> replicas = ring.getReplicaNodes(key, 2);
            System.out.println("  " + key + " → primary=" + replicas.get(0)
                    + ", replica=" + replicas.get(1));
        }

        step(12, "Add shard-4 — minimal key movement");
        Map<String, String> before = new LinkedHashMap<>();
        for (String k : shardKeys) before.put(k, ring.getNode(k));
        ring.addNode("shard-4");
        int moved = 0;
        for (String k : shardKeys) {
            if (!ring.getNode(k).equals(before.get(k))) moved++;
        }
        System.out.println("  Keys moved after adding shard-4: " + moved + "/" + shardKeys.length);

        // ============================================================
        // MODULE 7: Raft Consensus
        // ============================================================
        banner("MODULE 7: Raft Consensus");

        step(13, "3-node Raft cluster — elect leader and replicate");
        RaftNode r1 = new RaftNode("raft-1");
        RaftNode r2 = new RaftNode("raft-2");
        RaftNode r3 = new RaftNode("raft-3");
        for (RaftNode a : List.of(r1, r2, r3))
            for (RaftNode b : List.of(r1, r2, r3))
                if (a != b) a.addPeer(b);

        r1.startElection();
        System.out.println();
        r1.appendCommand("SET balance:alice=800");
        r1.appendCommand("SET balance:bob=700");

        System.out.println("\n  Cluster state:");
        for (RaftNode n : List.of(r1, r2, r3)) {
            System.out.println("  " + n + " commitIndex=" + n.getCommitIndex());
        }

        // ============================================================
        // MODULE 8: Metrics Summary
        // ============================================================
        banner("MODULE 8: Metrics");

        // Simulate some more ops for realistic numbers
        Random rng = new Random(42);
        for (int i = 0; i < 500; i++) {
            opCount.inc();
            opTimer.record(rng.nextInt(100) < 95 ? rng.nextInt(10) + 1 : rng.nextInt(200) + 50);
        }

        metrics.gauge("litedb_memtable_size_bytes").set(memTable.sizeBytes());
        metrics.gauge("litedb_btree_entries").set(index.size());
        metrics.gauge("litedb_raft_commit_index").set(r1.getCommitIndex());

        metrics.report();

        // ============================================================
        // SUMMARY
        // ============================================================
        banner("LITEDB — ARCHITECTURE SUMMARY");
        System.out.println("  ┌─────────────────────────────────────────────────────┐");
        System.out.println("  │                   LiteDB Architecture               │");
        System.out.println("  ├─────────────────────────────────────────────────────┤");
        System.out.println("  │  Client → Auth (RBAC) → SQL Parser → Query Engine   │");
        System.out.println("  │                              │                       │");
        System.out.println("  │                    ┌─────────▼──────────┐           │");
        System.out.println("  │                    │   MVCC (Snapshot)   │           │");
        System.out.println("  │                    └─────────┬──────────┘           │");
        System.out.println("  │                              │                       │");
        System.out.println("  │              ┌───────────────▼──────────────┐       │");
        System.out.println("  │              │  WAL → MemTable → SSTable     │       │");
        System.out.println("  │              │  (LSM-Tree write path)        │       │");
        System.out.println("  │              └───────────────┬──────────────┘       │");
        System.out.println("  │                              │                       │");
        System.out.println("  │              ┌───────────────▼──────────────┐       │");
        System.out.println("  │              │  B+ Tree Index (read path)    │       │");
        System.out.println("  │              └───────────────────────────────┘       │");
        System.out.println("  │                                                       │");
        System.out.println("  │  Distributed:                                         │");
        System.out.println("  │    Consistent Hashing → route to shard               │");
        System.out.println("  │    Raft Consensus     → replicate within shard        │");
        System.out.println("  │                                                       │");
        System.out.println("  │  Observability:                                       │");
        System.out.println("  │    Metrics (counters, gauges, histograms, timers)     │");
        System.out.println("  └─────────────────────────────────────────────────────┘");

        System.out.println("  Modules implemented:");
        System.out.println("    ✓ WAL (Write-Ahead Log)         — durability");
        System.out.println("    ✓ MemTable (Red-Black Tree)     — fast writes");
        System.out.println("    ✓ SSTable + Bloom Filter        — disk storage");
        System.out.println("    ✓ LSM-Tree                      — compaction");
        System.out.println("    ✓ B+ Tree                       — indexes");
        System.out.println("    ✓ MVCC                          — isolation");
        System.out.println("    ✓ SQL Parser                    — query language");
        System.out.println("    ✓ TCP Server                    — network layer");
        System.out.println("    ✓ Replication Log               — HA");
        System.out.println("    ✓ Consistent Hashing            — sharding");
        System.out.println("    ✓ Raft Consensus                — distributed agreement");
        System.out.println("    ✓ Auth + Connection Pool        — security");
        System.out.println("    ✓ Metrics Registry              — observability");
        System.out.println("\n  Total: 13 core database subsystems, ~3000 lines of Java.");
        System.out.println("\n  You now understand how MySQL, PostgreSQL, Cassandra,");
        System.out.println("  MongoDB, CockroachDB, and DynamoDB work under the hood.");
        System.out.println("\n[LiteDB Demo Complete]");

        wal.close();
    }
}