package com.litedb.replication;

import com.litedb.lsm.LSMEngine;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ReplicationLog — Primary-Replica replication via an in-memory operation log.
 *
 * CONCEPT:
 *   In a replicated database:
 *     - PRIMARY node accepts all writes
 *     - REPLICA nodes receive a copy of every write (replication log)
 *     - Replicas apply the log to their own storage engine
 *
 *   Replication modes:
 *     SYNCHRONOUS  — primary waits for replica ACK before confirming write
 *                    (strong consistency, higher latency)
 *     ASYNCHRONOUS — primary confirms write immediately, replica catches up
 *                    (eventual consistency, lower latency, risk of data loss)
 *
 *   This is how MySQL binlog, PostgreSQL WAL streaming, and MongoDB oplog work.
 *
 *   Replication lag: the number of operations the replica is behind the primary.
 *   If the primary crashes, a replica with lag > 0 may be missing recent writes.
 */
public class ReplicationLog {

    public enum Mode { SYNC, ASYNC }

    // ------------------------------------------------------------------ //
    //  Operation entry                                                    //
    // ------------------------------------------------------------------ //

    public static class OpEntry {
        public final long   lsn;        // Log Sequence Number
        public final String operation;  // "SET" or "DELETE"
        public final String key;
        public final String value;      // null for DELETE

        public OpEntry(long lsn, String operation, String key, String value) {
            this.lsn       = lsn;
            this.operation = operation;
            this.key       = key;
            this.value     = value;
        }

        @Override
        public String toString() {
            return "Op(lsn=" + lsn + ", " + operation + " " + key
                 + (value != null ? "=" + value : "") + ")";
        }
    }

    // ------------------------------------------------------------------ //
    //  Primary node                                                       //
    // ------------------------------------------------------------------ //

    public static class Primary {
        private final String    nodeId;
        private final LSMEngine engine;
        private final Mode      mode;
        private final List<Replica> replicas = new CopyOnWriteArrayList<>();
        private final AtomicLong lsnCounter  = new AtomicLong(0);

        public Primary(String nodeId, LSMEngine engine, Mode mode) {
            this.nodeId = nodeId;
            this.engine = engine;
            this.mode   = mode;
        }

        public void addReplica(Replica replica) {
            replicas.add(replica);
            System.out.println("[Primary:" + nodeId + "] Added replica: " + replica.nodeId);
        }

        public void set(String key, String value) throws IOException, InterruptedException {
            engine.set(key, value);
            OpEntry op = new OpEntry(lsnCounter.incrementAndGet(), "SET", key, value);
            replicate(op);
        }

        public void delete(String key) throws IOException, InterruptedException {
            engine.delete(key);
            OpEntry op = new OpEntry(lsnCounter.incrementAndGet(), "DELETE", key, null);
            replicate(op);
        }

        public String get(String key) throws IOException {
            return engine.get(key);
        }

        private void replicate(OpEntry op) throws IOException, InterruptedException {
            if (mode == Mode.SYNC) {
                // Wait for all replicas to acknowledge
                for (Replica r : replicas) {
                    r.applySync(op);
                }
            } else {
                // Fire-and-forget
                for (Replica r : replicas) {
                    r.applyAsync(op);
                }
            }
        }

        public long currentLsn() { return lsnCounter.get(); }

        @Override
        public String toString() {
            return "Primary(" + nodeId + ", mode=" + mode + ", lsn=" + lsnCounter.get() + ")";
        }
    }

    // ------------------------------------------------------------------ //
    //  Replica node                                                       //
    // ------------------------------------------------------------------ //

    public static class Replica {
        public final String    nodeId;
        private final LSMEngine engine;
        private final long      networkDelayMs; // simulated replication lag
        private volatile long   appliedLsn = 0;
        private final BlockingQueue<OpEntry> queue = new LinkedBlockingQueue<>();
        private final ExecutorService applyThread;

        public Replica(String nodeId, LSMEngine engine, long networkDelayMs) {
            this.nodeId         = nodeId;
            this.engine         = engine;
            this.networkDelayMs = networkDelayMs;
            this.applyThread    = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "replica-apply-" + nodeId);
                t.setDaemon(true);
                return t;
            });
            // Start async apply loop
            applyThread.submit(this::applyLoop);
        }

        /** Synchronous apply — blocks until the op is applied. */
        public void applySync(OpEntry op) throws IOException, InterruptedException {
            if (networkDelayMs > 0) Thread.sleep(networkDelayMs);
            applyOp(op);
        }

        /** Asynchronous apply — queues the op for background application. */
        public void applyAsync(OpEntry op) {
            queue.offer(op);
        }

        private void applyLoop() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    OpEntry op = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (op == null) continue;
                    if (networkDelayMs > 0) Thread.sleep(networkDelayMs);
                    applyOp(op);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (IOException e) {
                    System.out.println("[Replica:" + nodeId + "] Apply error: " + e.getMessage());
                }
            }
        }

        private synchronized void applyOp(OpEntry op) throws IOException {
            if ("SET".equals(op.operation)) {
                engine.set(op.key, op.value);
            } else if ("DELETE".equals(op.operation)) {
                engine.delete(op.key);
            }
            appliedLsn = op.lsn;
        }

        public String get(String key) throws IOException {
            return engine.get(key);
        }

        public long replicationLag(long primaryLsn) {
            return primaryLsn - appliedLsn;
        }

        public void waitForCatchup(long targetLsn, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (appliedLsn < targetLsn && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
        }

        public void shutdown() { applyThread.shutdownNow(); }

        @Override
        public String toString() {
            return "Replica(" + nodeId + ", appliedLsn=" + appliedLsn + ")";
        }
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) throws Exception {
        java.nio.file.Path tmpDir = java.nio.file.Files.createTempDirectory("litedb_repl_demo_");

        System.out.println("============================================================");
        System.out.println("REPLICATION DEMO");
        System.out.println("============================================================");

        // ---- Part 1: Synchronous replication ----
        System.out.println("\n[Part 1] Synchronous replication (strong consistency)");
        LSMEngine primaryEngine  = new LSMEngine(tmpDir.resolve("primary").toString());
        LSMEngine replicaEngine1 = new LSMEngine(tmpDir.resolve("replica1").toString());

        Primary primary = new Primary("primary", primaryEngine, Mode.SYNC);
        Replica replica1 = new Replica("replica-1", replicaEngine1, 0);
        primary.addReplica(replica1);

        primary.set("name", "Alice");
        primary.set("age",  "30");
        primary.set("city", "NYC");

        System.out.println("  Primary  GET name → " + primary.get("name"));
        System.out.println("  Replica1 GET name → " + replica1.get("name") + " (sync: immediately consistent)");
        System.out.println("  Replication lag: " + replica1.replicationLag(primary.currentLsn()));

        // ---- Part 2: Asynchronous replication ----
        System.out.println("\n[Part 2] Asynchronous replication (eventual consistency)");
        LSMEngine primaryEngine2 = new LSMEngine(tmpDir.resolve("primary2").toString());
        LSMEngine replicaEngine2 = new LSMEngine(tmpDir.resolve("replica2").toString());

        Primary primary2 = new Primary("primary2", primaryEngine2, Mode.ASYNC);
        Replica replica2 = new Replica("replica-2", replicaEngine2, 50); // 50ms simulated lag
        primary2.addReplica(replica2);

        primary2.set("x", "100");
        primary2.set("y", "200");
        primary2.set("z", "300");

        System.out.println("  Immediately after writes:");
        System.out.println("    Primary  GET x → " + primary2.get("x"));
        System.out.println("    Replica2 GET x → " + replica2.get("x") + " (may be null — not yet applied)");
        System.out.println("    Replication lag: " + replica2.replicationLag(primary2.currentLsn()) + " ops");

        // Wait for replica to catch up
        replica2.waitForCatchup(primary2.currentLsn(), 2000);
        System.out.println("\n  After waiting for replica to catch up:");
        System.out.println("    Replica2 GET x → " + replica2.get("x") + " (now consistent)");
        System.out.println("    Replication lag: " + replica2.replicationLag(primary2.currentLsn()) + " ops");

        // Cleanup
        replica1.shutdown();
        replica2.shutdown();
        primaryEngine.close();
        replicaEngine1.close();
        primaryEngine2.close();
        replicaEngine2.close();
        deleteDir(tmpDir.toFile());

        System.out.println("\n[Done] Replication demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. SYNC replication: write confirmed only after replica ACK → no data loss");
        System.out.println("  2. ASYNC replication: write confirmed immediately → possible data loss on crash");
        System.out.println("  3. Replication lag = how far behind the replica is");
        System.out.println("  4. MySQL binlog, PostgreSQL WAL streaming, MongoDB oplog all work this way");
    }

    private static void deleteDir(java.io.File dir) {
        if (dir.isDirectory()) { for (java.io.File f : dir.listFiles()) deleteDir(f); }
        dir.delete();
    }
}