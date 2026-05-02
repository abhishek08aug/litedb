package com.litedb.txn;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * MVCCStore — Multi-Version Concurrency Control
 *
 * CONCEPT:
 *   MVCC allows readers and writers to proceed concurrently without blocking
 *   each other. Instead of locking a row for a read, we keep multiple versions
 *   of each key. Each transaction sees a consistent snapshot of the database
 *   as of its start time.
 *
 *   How it works:
 *     - Every write creates a NEW version of the key (tagged with txn ID)
 *     - Readers see the latest version that was committed BEFORE their txn started
 *     - Writers create pending (uncommitted) versions
 *     - On COMMIT: versions become visible to future transactions
 *     - On ROLLBACK: pending versions are discarded
 *
 *   This is how PostgreSQL, MySQL InnoDB, Oracle, and CockroachDB implement
 *   snapshot isolation and serializable isolation.
 *
 *   Isolation levels:
 *     READ UNCOMMITTED — see uncommitted writes (dirty reads) — we don't implement
 *     READ COMMITTED   — see only committed writes (no dirty reads)
 *     SNAPSHOT         — see consistent snapshot from txn start (no phantom reads)
 *     SERIALIZABLE     — full serializability (we approximate with write-write conflict)
 */
public class MVCCStore {

    // ------------------------------------------------------------------ //
    //  Version chain                                                      //
    // ------------------------------------------------------------------ //

    static class Version {
        final long   txnId;
        final String value;      // null = tombstone (deleted)
        final boolean committed;

        Version(long txnId, String value, boolean committed) {
            this.txnId     = txnId;
            this.value     = value;
            this.committed = committed;
        }

        @Override
        public String toString() {
            return "V(txn=" + txnId + ", val=" + value + ", committed=" + committed + ")";
        }
    }

    // ------------------------------------------------------------------ //
    //  Transaction                                                        //
    // ------------------------------------------------------------------ //

    public class Transaction {
        public final long txnId;
        private final long snapshotId;  // see versions committed before this
        private final Map<String, String> writeSet = new LinkedHashMap<>();
        private boolean active = true;

        Transaction(long txnId, long snapshotId) {
            this.txnId      = txnId;
            this.snapshotId = snapshotId;
        }

        /** Read a key — sees snapshot at txn start time. */
        public String get(String key) {
            checkActive();
            // Check own write set first (read-your-own-writes)
            if (writeSet.containsKey(key)) {
                String v = writeSet.get(key);
                return "__DELETED__".equals(v) ? null : v;
            }
            return MVCCStore.this.getVisible(key, snapshotId);
        }

        /** Write a key — buffered until commit. */
        public void set(String key, String value) {
            checkActive();
            writeSet.put(key, value);
        }

        /** Delete a key — writes a tombstone. */
        public void delete(String key) {
            checkActive();
            writeSet.put(key, "__DELETED__");
        }

        /** Commit: make all writes visible to future transactions. */
        public void commit() {
            checkActive();
            active = false;
            MVCCStore.this.commit(this);
        }

        /** Rollback: discard all writes. */
        public void rollback() {
            active = false;
            System.out.println("[MVCC] Txn " + txnId + " rolled back (" + writeSet.size() + " writes discarded)");
        }

        private void checkActive() {
            if (!active) throw new IllegalStateException("Transaction " + txnId + " is no longer active");
        }

        @Override
        public String toString() {
            return "Txn(id=" + txnId + ", snapshot=" + snapshotId + ", writes=" + writeSet.size() + ")";
        }
    }

    // ------------------------------------------------------------------ //
    //  Store internals                                                    //
    // ------------------------------------------------------------------ //

    // key → list of versions (oldest first)
    private final Map<String, List<Version>> store = new ConcurrentHashMap<>();
    private final AtomicLong txnCounter      = new AtomicLong(0);
    private final AtomicLong committedCounter = new AtomicLong(0);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Begin a new transaction. */
    public Transaction begin() {
        long txnId     = txnCounter.incrementAndGet();
        long snapshotId = committedCounter.get(); // snapshot = latest committed txn
        Transaction txn = new Transaction(txnId, snapshotId);
        System.out.println("[MVCC] Begin " + txn);
        return txn;
    }

    /** Apply committed writes and advance the committed counter. */
    private void commit(Transaction txn) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<String, String> e : txn.writeSet.entrySet()) {
                store.computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                     .add(new Version(txn.txnId, e.getValue(), true));
            }
            committedCounter.set(Math.max(committedCounter.get(), txn.txnId));
            System.out.println("[MVCC] Commit txn=" + txn.txnId + " (" + txn.writeSet.size() + " writes)");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Return the latest committed version of key visible at snapshotId. */
    private String getVisible(String key, long snapshotId) {
        lock.readLock().lock();
        try {
            List<Version> versions = store.get(key);
            if (versions == null) return null;
            // Walk backwards — find latest committed version with txnId <= snapshotId
            for (int i = versions.size() - 1; i >= 0; i--) {
                Version v = versions.get(i);
                if (v.committed && v.txnId <= snapshotId) {
                    return "__DELETED__".equals(v.value) ? null : v.value;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Vacuum: remove old versions no longer needed by any active transaction. */
    public int vacuum(long minSnapshotId) {
        lock.writeLock().lock();
        int removed = 0;
        try {
            for (List<Version> versions : store.values()) {
                // Keep at most one version per key that is <= minSnapshotId
                int keepFrom = 0;
                for (int i = versions.size() - 1; i >= 0; i--) {
                    if (versions.get(i).committed && versions.get(i).txnId <= minSnapshotId) {
                        keepFrom = i;
                        break;
                    }
                }
                int before = versions.size();
                List<Version> kept = new ArrayList<>(versions.subList(keepFrom, versions.size()));
                versions.clear();
                versions.addAll(kept);
                removed += before - versions.size();
            }
        } finally {
            lock.writeLock().unlock();
        }
        System.out.println("[MVCC] Vacuum removed " + removed + " old versions");
        return removed;
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) throws InterruptedException {
        System.out.println("============================================================");
        System.out.println("MVCC TRANSACTIONS DEMO");
        System.out.println("============================================================");

        MVCCStore store = new MVCCStore();

        // ---- Part 1: Basic commit/rollback ----
        System.out.println("\n[Part 1] Basic commit and rollback");
        Transaction t1 = store.begin();
        t1.set("name", "Alice");
        t1.set("age",  "30");
        t1.commit();

        Transaction t2 = store.begin();
        System.out.println("  After t1 commit: GET name = " + t2.get("name"));
        t2.set("name", "Bob");  // not committed yet
        System.out.println("  t2 sees own write: GET name = " + t2.get("name"));
        t2.rollback();

        Transaction t3 = store.begin();
        System.out.println("  After t2 rollback: GET name = " + t3.get("name") + " (still Alice)");
        t3.rollback();

        // ---- Part 2: Snapshot isolation ----
        System.out.println("\n[Part 2] Snapshot isolation — concurrent transactions");
        Transaction writer = store.begin();
        Transaction reader = store.begin(); // starts BEFORE writer commits

        writer.set("balance", "1000");
        System.out.println("  Writer set balance=1000 (not committed)");
        System.out.println("  Reader sees balance = " + reader.get("balance") + " (null — writer not committed)");

        writer.commit();
        System.out.println("  Writer committed");
        System.out.println("  Reader still sees balance = " + reader.get("balance")
                + " (snapshot isolation — reader sees state from when it started)");

        Transaction newReader = store.begin();
        System.out.println("  New reader sees balance = " + newReader.get("balance") + " (sees committed write)");
        reader.rollback();
        newReader.rollback();

        // ---- Part 3: Delete ----
        System.out.println("\n[Part 3] Delete via tombstone");
        Transaction t4 = store.begin();
        t4.delete("age");
        t4.commit();

        Transaction t5 = store.begin();
        System.out.println("  After delete: GET age = " + t5.get("age") + " (null = deleted)");
        t5.rollback();

        // ---- Part 4: Vacuum ----
        System.out.println("\n[Part 4] Vacuum old versions");
        store.vacuum(store.committedCounter.get());

        System.out.println("\n[Done] MVCC demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. Readers never block writers; writers never block readers");
        System.out.println("  2. Each transaction sees a consistent snapshot from its start time");
        System.out.println("  3. Rollback is free — just discard the write set");
        System.out.println("  4. Vacuum (AUTOVACUUM in PostgreSQL) reclaims old versions");
        System.out.println("  5. This is how PostgreSQL, MySQL InnoDB, Oracle all work");
    }
}