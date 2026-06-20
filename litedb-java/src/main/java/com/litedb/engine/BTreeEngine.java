package com.litedb.engine;

import com.litedb.btree.BTree;
import com.litedb.wal.WALEntry;
import com.litedb.wal.WriteAheadLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * BTreeEngine — a primary key->value storage engine backed by an in-memory B+Tree,
 * made durable by a write-ahead log (the engine replays the WAL on startup).
 *
 * This is the read-optimized counterpart to {@link com.litedb.lsm.LSMEngine}: updates
 * happen in place in the tree (no MemTable / SSTable / compaction). Point lookups and
 * range scans go straight to a single sorted structure. Like the LSM engine, durability
 * comes from a WAL.
 *
 * Scope / limitations (teaching implementation):
 *   - The tree is held entirely in memory; durability is via WAL replay, not on-disk
 *     B+Tree pages.
 *   - The WAL is not checkpointed, so it grows with the write history. A production B-tree
 *     engine periodically snapshots the tree (a checkpoint) and truncates the WAL.
 *
 * This engine does not provide a secondary index (see {@code supportsSecondaryIndex} on the
 * interface) — the LSM engine is the one wired for that.
 */
public final class BTreeEngine implements StorageEngine {

    public final String dataDir;
    private final BTree tree = new BTree();
    private final WriteAheadLog wal;

    public BTreeEngine(String dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(Paths.get(dataDir));
        this.wal = new WriteAheadLog(dataDir + "/wal.log");
        recover();
    }

    /** Rebuild the in-memory tree by replaying the WAL. */
    private void recover() throws IOException {
        int n = 0;
        for (WALEntry e : wal.readAll()) {
            if ("SET".equals(e.operation)) {
                tree.insert(e.key, e.value != null ? e.value : "");
            } else if ("DELETE".equals(e.operation)) {
                tree.delete(e.key);
            }
            n++;
        }
        if (n > 0) System.out.println("[BTreeEngine] Recovered " + n + " WAL entries into the B+Tree");
    }

    @Override
    public synchronized void set(String key, String value) throws IOException {
        wal.appendSet(key, value);   // durability first
        tree.insert(key, value);     // in-place update
    }

    @Override
    public synchronized void delete(String key) throws IOException {
        wal.appendDelete(key);
        tree.delete(key);
    }

    @Override
    public synchronized String get(String key) {
        return tree.get(key);
    }

    @Override
    public synchronized List<Map.Entry<String, String>> scan(String startKey, String endKey) {
        return tree.range(startKey, endKey);
    }

    @Override
    public void flush() {
        // Nothing buffered: writes are already durable in the WAL and applied to the tree.
    }

    @Override
    public synchronized Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("engine", "btree");
        m.put("entries", tree.size());
        return m;
    }

    @Override
    public String name() {
        return "btree";
    }

    @Override
    public synchronized void close() throws IOException {
        wal.close();
        System.out.println("[BTreeEngine] Closed.");
    }
}
