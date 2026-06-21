package com.litedb.lsm;

import com.litedb.memtable.MemTable;
import com.litedb.sstable.SSTableReader;
import com.litedb.sstable.SSTableWriter;
import com.litedb.wal.WALEntry;
import com.litedb.wal.WriteAheadLog;
import com.litedb.engine.StorageEngine;
import com.litedb.engine.WriteOp;
import com.litedb.index.SecondaryIndex;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * LSMEngine — Log-Structured Merge-Tree Storage Engine
 *
 * CONCEPT:
 *   The LSM-Tree is the storage engine behind LevelDB, RocksDB, Cassandra,
 *   HBase, and ScyllaDB. It optimises for write throughput by:
 *     1. Buffering writes in memory (MemTable)
 *     2. Flushing to immutable sorted files on disk (SSTables)
 *     3. Periodically merging SSTables (compaction) to reclaim space
 *
 *   Read path:  MemTable → L0 SSTables (newest first) → L1 SSTables
 *   Write path: WAL → MemTable → (flush) → SSTable
 *   Compaction: merge L0 + L1 → new L1, drop tombstones
 */
public class LSMEngine implements StorageEngine {

    private static final String TOMBSTONE = "__DELETED__";

    public final String dataDir;

    private final MemTable        memtable;
    private final WriteAheadLog   wal;
    private final List<SSTableReader> l0 = new ArrayList<>(); // newest first
    private final List<SSTableReader> l1 = new ArrayList<>();
    private final ReentrantLock   lock      = new ReentrantLock();
    private final ReentrantLock   flushLock = new ReentrantLock();
    private final SecondaryIndex  valueIndex = new SecondaryIndex(); // value -> primary keys

    private int sstSequence = 0;

    private static final int L0_COMPACTION_THRESHOLD = 4;

    public LSMEngine(String dataDir) throws IOException {
        this.dataDir  = dataDir;
        Files.createDirectories(Paths.get(dataDir));
        this.memtable = new MemTable(4L * 1024 * 1024); // 4 MB
        this.wal      = new WriteAheadLog(dataDir + "/wal.log");
        recover();
    }

    // ------------------------------------------------------------------ //
    //  Public API                                                         //
    // ------------------------------------------------------------------ //

    public void set(String key, String value) throws IOException {
        String old = get(key);                 // prior value, to maintain the secondary index
        wal.appendSet(key, value);
        memtable.set(key, value);
        valueIndex.update(key, old, value);
        maybeFlush();
    }

    public void delete(String key) throws IOException {
        String old = get(key);
        wal.appendDelete(key);
        memtable.delete(key);
        if (old != null) valueIndex.remove(key, old);
        maybeFlush();
    }

    @Override
    public boolean supportsAtomicBatch() {
        return true;
    }

    /**
     * Atomically apply a batch of writes. The whole batch is framed in the WAL between BEGIN and
     * COMMIT markers, then applied to the MemTable and the value index. On recovery a batch is
     * replayed only if its COMMIT marker is present — a crash mid-batch leaves no partial state.
     */
    @Override
    public void writeBatch(List<WriteOp> ops) throws IOException {
        flushLock.lock();   // a flush truncates the WAL — keep it out of the batch window
        try {
            wal.appendMarker("BEGIN");
            for (WriteOp op : ops) {
                if (op.delete) wal.appendDelete(op.key);
                else           wal.appendSet(op.key, op.value);
            }
            wal.appendMarker("COMMIT");
            for (WriteOp op : ops) {
                String old = get(op.key);
                if (op.delete) {
                    memtable.delete(op.key);
                    if (old != null) valueIndex.remove(op.key, old);
                } else {
                    memtable.set(op.key, op.value);
                    valueIndex.update(op.key, old, op.value);
                }
            }
        } finally {
            flushLock.unlock();
        }
        maybeFlush();
    }

    /**
     * TEST ONLY — simulate a crash mid-batch: log BEGIN + the ops but never COMMIT, and do not
     * apply them. On the next recovery this batch must be discarded entirely.
     */
    public void writeBatchSimulateCrash(List<WriteOp> ops) throws IOException {
        flushLock.lock();
        try {
            wal.appendMarker("BEGIN");
            for (WriteOp op : ops) {
                if (op.delete) wal.appendDelete(op.key);
                else           wal.appendSet(op.key, op.value);
            }
            // intentionally no COMMIT and no apply
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Read path: MemTable → L0 (newest first) → L1
     */
    public String get(String key) throws IOException {
        // 1. Check MemTable
        String val = memtable.get(key);
        if (val != null) {
            return TOMBSTONE.equals(val) ? null : val;
        }

        lock.lock();
        try {
            // 2. Check L0 SSTables (newest first)
            for (int i = l0.size() - 1; i >= 0; i--) {
                val = l0.get(i).get(key);
                if (val != null) return TOMBSTONE.equals(val) ? null : val;
            }
            // 3. Check L1 SSTables
            for (int i = l1.size() - 1; i >= 0; i--) {
                val = l1.get(i).get(key);
                if (val != null) return TOMBSTONE.equals(val) ? null : val;
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    /**
     * Range scan across MemTable + all SSTables, merging results.
     */
    public List<Map.Entry<String, String>> scan(String startKey, String endKey) throws IOException {
        // Collect all versions; newest source wins
        TreeMap<String, String> merged = new TreeMap<>();

        lock.lock();
        try {
            // L1 first (oldest)
            for (SSTableReader sst : l1) {
                for (Map.Entry<String, String> e : sst.scan(startKey, endKey)) {
                    merged.put(e.getKey(), e.getValue());
                }
            }
            // L0 (newer)
            for (SSTableReader sst : l0) {
                for (Map.Entry<String, String> e : sst.scan(startKey, endKey)) {
                    merged.put(e.getKey(), e.getValue());
                }
            }
        } finally {
            lock.unlock();
        }

        // MemTable (newest)
        for (Map.Entry<String, String> e : memtable.scan(startKey, endKey)) {
            merged.put(e.getKey(), e.getValue());
        }

        // Filter tombstones
        List<Map.Entry<String, String>> result = new ArrayList<>();
        for (Map.Entry<String, String> e : merged.entrySet()) {
            if (!TOMBSTONE.equals(e.getValue())) result.add(e);
        }
        return result;
    }

    // ------------------------------------------------------------------ //
    //  Flush                                                              //
    // ------------------------------------------------------------------ //

    private void maybeFlush() throws IOException {
        if (memtable.shouldFlush()) {
            flushMemtable();
        }
    }

    private void flushMemtable() throws IOException {
        flushLock.lock();
        try {
            if (memtable.entryCount() == 0) return;

            List<Map.Entry<String, String>> items = memtable.itemsSorted();
            String sstPath = newSstPath(0);
            SSTableWriter writer = new SSTableWriter(sstPath);
            SSTableReader reader = writer.write(items);

            lock.lock();
            try {
                l0.add(reader);
            } finally {
                lock.unlock();
            }

            memtable.clear();
            wal.truncate();

            System.out.println("[LSM] Flushed MemTable → " + sstPath);

            if (l0.size() >= L0_COMPACTION_THRESHOLD) {
                compactL0toL1();
            }
        } finally {
            flushLock.unlock();
        }
    }

    // ------------------------------------------------------------------ //
    //  Compaction                                                         //
    // ------------------------------------------------------------------ //

    private void compactL0toL1() throws IOException {
        flushLock.lock();
        try {
            List<SSTableReader> l0Snap, l1Snap;
            lock.lock();
            try {
                l0Snap = new ArrayList<>(l0);
                l1Snap = new ArrayList<>(l1);
            } finally {
                lock.unlock();
            }

            if (l0Snap.isEmpty()) return;
            System.out.println("[LSM] Compacting " + l0Snap.size() + " L0 + " + l1Snap.size() + " L1 SSTables...");

            // Merge: oldest first, newest overwrites
            Map<String, String> allEntries = new LinkedHashMap<>();
            for (SSTableReader sst : l1Snap) {
                for (Map.Entry<String, String> e : sst.iterAll()) allEntries.put(e.getKey(), e.getValue());
            }
            for (SSTableReader sst : l0Snap) {
                for (Map.Entry<String, String> e : sst.iterAll()) allEntries.put(e.getKey(), e.getValue());
            }

            // Sort and drop tombstones
            List<Map.Entry<String, String>> merged = allEntries.entrySet().stream()
                    .filter(e -> !TOMBSTONE.equals(e.getValue()))
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());

            List<String> oldPaths = new ArrayList<>();
            for (SSTableReader s : l0Snap) oldPaths.add(s.path);
            for (SSTableReader s : l1Snap) oldPaths.add(s.path);

            if (!merged.isEmpty()) {
                String newPath = newSstPath(1);
                SSTableWriter writer = new SSTableWriter(newPath);
                SSTableReader newL1  = writer.write(merged);

                lock.lock();
                try {
                    l0.clear();
                    l1.clear();
                    l1.add(newL1);
                } finally {
                    lock.unlock();
                }
                System.out.println("[LSM] Compaction done → " + newPath + " (" + merged.size() + " entries)");
            } else {
                lock.lock();
                try { l0.clear(); l1.clear(); } finally { lock.unlock(); }
                System.out.println("[LSM] Compaction done — all entries were tombstones");
            }

            for (String p : oldPaths) {
                try { Files.deleteIfExists(Paths.get(p)); } catch (IOException ignored) {}
            }
        } finally {
            flushLock.unlock();
        }
    }

    // ------------------------------------------------------------------ //
    //  Recovery                                                           //
    // ------------------------------------------------------------------ //

    private void recover() throws IOException {
        // Load existing SSTables
        for (int level = 0; level <= 1; level++) {
            final int lvl = level;
            List<Path> paths = new ArrayList<>();
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(
                    Paths.get(dataDir), "sst_l" + level + "_*.sst")) {
                for (Path p : ds) paths.add(p);
            }
            paths.sort(Comparator.comparing(Path::toString));
            for (Path p : paths) {
                try {
                    SSTableReader r = new SSTableReader(p.toString());
                    String base = p.getFileName().toString();
                    int seq = Integer.parseInt(base.split("_")[2].replace(".sst", ""));
                    if (seq >= sstSequence) sstSequence = seq + 1;
                    if (lvl == 0) l0.add(r); else l1.add(r);
                } catch (Exception e) {
                    System.out.println("[LSM] Warning: could not load SSTable " + p + ": " + e.getMessage());
                }
            }
        }

        // Replay WAL
        int replayed = 0, discarded = 0;
        boolean inBatch = false;
        List<WALEntry> batch = new ArrayList<>();
        for (WALEntry entry : wal.readAll()) {
            switch (entry.operation) {
                case "BEGIN":
                    inBatch = true; batch.clear();
                    break;
                case "COMMIT":
                    for (WALEntry b : batch) applyWalEntry(b);
                    replayed += batch.size(); batch.clear(); inBatch = false;
                    break;
                default:
                    if (inBatch) batch.add(entry);          // buffer until COMMIT
                    else { applyWalEntry(entry); replayed++; }
            }
        }
        if (inBatch) discarded = batch.size();              // crash before COMMIT -> discard
        if (replayed > 0 || discarded > 0) {
            System.out.println("[LSM] Recovered " + replayed + " entries"
                    + (discarded > 0 ? " (discarded " + discarded + " from an uncommitted batch)" : "")
                    + " from WAL");
        }

        rebuildIndex();
    }

    private void applyWalEntry(WALEntry e) {
        if ("SET".equals(e.operation)) {
            memtable.set(e.key, e.value != null ? e.value : "");
        } else if ("DELETE".equals(e.operation)) {
            memtable.delete(e.key);
        }
    }

    /** Rebuild the in-memory secondary index from all live data (called after recovery). */
    private void rebuildIndex() throws IOException {
        for (Map.Entry<String, String> e : scan("", "" + Character.MAX_VALUE)) {
            valueIndex.add(e.getKey(), e.getValue());
        }
    }

    @Override
    public String name() {
        return "lsm";
    }

    @Override
    public boolean supportsSecondaryIndex() {
        return true;
    }

    /** Reverse lookup via the secondary index: primary keys whose value is in [lo, hi]. */
    @Override
    public List<String> findByValueRange(String lowValue, String highValue) {
        return valueIndex.keysInValueRange(lowValue, highValue);
    }

    // ------------------------------------------------------------------ //
    //  Lifecycle                                                          //
    // ------------------------------------------------------------------ //

    /** Force flush MemTable to disk (e.g., on graceful shutdown). */
    public void flush() throws IOException {
        if (memtable.entryCount() > 0) {
            memtable.setSizeLimitBytes(0);
            flushMemtable();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        wal.close();
        System.out.println("[LSM] Engine closed.");
    }

    @Override
    public Map<String, Object> stats() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("engine",             "lsm");
        s.put("memtable_entries",   memtable.entryCount());
        s.put("memtable_size_bytes", memtable.sizeBytes());
        s.put("l0_sstables",        l0.size());
        s.put("l1_sstables",        l1.size());
        s.put("l0_entries",         l0.stream().mapToInt(r -> r.entryCount).sum());
        s.put("l1_entries",         l1.stream().mapToInt(r -> r.entryCount).sum());
        s.put("index_entries",      valueIndex.size());
        return s;
    }

    private String newSstPath(int level) {
        return dataDir + "/sst_l" + level + "_" + String.format("%06d", sstSequence++) + ".sst";
    }

    @Override
    public String toString() {
        return "LSMEngine(dir=" + dataDir + ", mem=" + memtable.entryCount()
             + " entries, L0=" + l0.size() + ", L1=" + l1.size() + ")";
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) throws Exception {
        Path tmpDir = Files.createTempDirectory("litedb_lsm_demo_");

        System.out.println("============================================================");
        System.out.println("LSM-TREE ENGINE DEMO");
        System.out.println("============================================================");

        LSMEngine engine = new LSMEngine(tmpDir.toString());

        // Step 1: Write data
        System.out.println("\n[Step 1] Writing 20 key-value pairs...");
        String[] fruits = {"apple","banana","cherry","date","elderberry",
                           "fig","grape","honeydew","kiwi","lemon",
                           "mango","nectarine","orange","papaya","quince",
                           "raspberry","strawberry","tangerine","ugli","vanilla"};
        for (int i = 0; i < fruits.length; i++) engine.set(fruits[i], "value_" + i);
        System.out.println("  Stats: " + engine.stats());

        // Step 2: Read
        System.out.println("\n[Step 2] Reading keys...");
        for (String key : new String[]{"apple","mango","zebra"}) {
            System.out.println("  GET '" + key + "' → " + engine.get(key));
        }

        // Step 3: Update
        System.out.println("\n[Step 3] Updating 'apple'...");
        engine.set("apple", "updated_apple_value");
        System.out.println("  GET apple → " + engine.get("apple"));

        // Step 4: Delete
        System.out.println("\n[Step 4] Deleting 'banana'...");
        engine.delete("banana");
        System.out.println("  GET banana → " + engine.get("banana") + " (null = deleted)");

        // Step 5: Range scan
        System.out.println("\n[Step 5] Range scan 'c' to 'f'...");
        for (Map.Entry<String, String> e : engine.scan("c", "f")) {
            System.out.println("  '" + e.getKey() + "': '" + e.getValue() + "'");
        }

        // Step 6: Force flush
        System.out.println("\n[Step 6] Forcing flush to SSTable...");
        engine.flush();
        System.out.println("  Stats after flush: " + engine.stats());

        // Step 7: Read after flush
        System.out.println("\n[Step 7] Reading after flush (data is now on disk)...");
        System.out.println("  GET apple  → " + engine.get("apple"));
        System.out.println("  GET banana → " + engine.get("banana") + " (still deleted)");
        System.out.println("  GET mango  → " + engine.get("mango"));

        engine.close();

        // Cleanup
        deleteDir(tmpDir.toFile());
        System.out.println("\n[Done] LSM-Tree engine demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. Writes go to WAL + MemTable (fast, durable)");
        System.out.println("  2. MemTable flushes to SSTable when full");
        System.out.println("  3. SSTables are immutable — never modified");
        System.out.println("  4. Compaction merges SSTables, removes tombstones");
        System.out.println("  5. On crash: replay WAL to recover unflushed writes");
    }

    private static void deleteDir(File dir) {
        if (dir.isDirectory()) { for (File f : dir.listFiles()) deleteDir(f); }
        dir.delete();
    }
}