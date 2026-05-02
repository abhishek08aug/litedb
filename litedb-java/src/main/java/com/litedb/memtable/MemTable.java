package com.litedb.memtable;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * MemTable — In-Memory Write Buffer
 *
 * CONCEPT:
 *   The MemTable is an in-memory sorted data structure that buffers
 *   recent writes before they are flushed to disk as an SSTable.
 *
 *   Read path:
 *     1. Check MemTable first (most recent writes are here)
 *     2. If not found, check SSTables on disk (older data)
 *
 *   Write path:
 *     1. Append to WAL (durability)
 *     2. Insert into MemTable (fast in-memory write)
 *     3. When MemTable exceeds size threshold → flush to SSTable
 *
 *   This is exactly how LevelDB, RocksDB, and Cassandra work.
 *
 *   Data structure: TreeMap (Red-Black Tree) — O(log n) insert,
 *   maintains sorted order for efficient range scans and flush.
 */
public class MemTable {

    public static final String TOMBSTONE = "__DELETED__";

    // TreeMap keeps keys sorted — critical for SSTable flush and range scans
    private final TreeMap<String, String> data = new TreeMap<>();
    private final ReentrantReadWriteLock  rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock  readLock  = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private long sizeLimitBytes;
    private long sizeBytes = 0;
    private int  writeCount = 0;

    /** Default 4 MB size limit (same as Python implementation). */
    public MemTable() {
        this(4L * 1024 * 1024);
    }

    public MemTable(long sizeLimitBytes) {
        this.sizeLimitBytes = sizeLimitBytes;
    }

    // ------------------------------------------------------------------ //
    //  Write operations                                                   //
    // ------------------------------------------------------------------ //

    /** Insert or update a key-value pair. */
    public void set(String key, String value) {
        writeLock.lock();
        try {
            if (data.containsKey(key)) {
                sizeBytes -= key.length() + data.get(key).length();
            }
            data.put(key, value);
            sizeBytes += key.length() + value.length();
            writeCount++;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Mark a key as deleted by writing a tombstone.
     * We do NOT remove the key — the tombstone must propagate to SSTables
     * during compaction to shadow older values.
     */
    public void delete(String key) {
        writeLock.lock();
        try {
            if (data.containsKey(key)) {
                sizeBytes -= key.length() + data.get(key).length();
            }
            data.put(key, TOMBSTONE);
            sizeBytes += key.length() + TOMBSTONE.length();
            writeCount++;
        } finally {
            writeLock.unlock();
        }
    }

    // ------------------------------------------------------------------ //
    //  Read operations                                                    //
    // ------------------------------------------------------------------ //

    /**
     * Look up a key.
     * Returns:
     *   - The value string if key exists and is not deleted
     *   - TOMBSTONE if the key was deleted (caller must handle this)
     *   - null if key was never written to this MemTable
     */
    public String get(String key) {
        readLock.lock();
        try {
            return data.get(key);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Range scan: return all (key, value) pairs where startKey <= key <= endKey.
     * Returns in sorted key order (TreeMap guarantees this).
     * Includes tombstones — caller decides whether to skip them.
     */
    public List<Map.Entry<String, String>> scan(String startKey, String endKey) {
        readLock.lock();
        try {
            NavigableMap<String, String> sub = data.subMap(startKey, true, endKey, true);
            return new ArrayList<>(sub.entrySet());
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Return all (key, value) pairs sorted by key.
     * Used when flushing to SSTable.
     */
    public List<Map.Entry<String, String>> itemsSorted() {
        readLock.lock();
        try {
            return new ArrayList<>(data.entrySet());
        } finally {
            readLock.unlock();
        }
    }

    // ------------------------------------------------------------------ //
    //  Size / flush threshold                                             //
    // ------------------------------------------------------------------ //

    /** Returns true when the MemTable has grown past the size limit. */
    public boolean shouldFlush() {
        return sizeBytes >= sizeLimitBytes;
    }

    public long sizeBytes() { return sizeBytes; }

    public int entryCount() {
        readLock.lock();
        try { return data.size(); } finally { readLock.unlock(); }
    }

    /**
     * Clear the MemTable after a successful flush to SSTable.
     * Called ONLY after the SSTable is safely written to disk.
     */
    public void clear() {
        writeLock.lock();
        try {
            data.clear();
            sizeBytes  = 0;
            writeCount = 0;
        } finally {
            writeLock.unlock();
        }
    }

    /** For testing: temporarily lower the limit to force a flush. */
    public void setSizeLimitBytes(long limit) {
        this.sizeLimitBytes = limit;
    }

    @Override
    public String toString() {
        return String.format("MemTable(entries=%d, size=%.1fKB, limit=%dMB)",
                entryCount(), sizeBytes / 1024.0, sizeLimitBytes / 1024 / 1024);
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) {
        System.out.println("============================================================");
        System.out.println("MEMTABLE DEMO");
        System.out.println("============================================================");

        MemTable mt = new MemTable(1024); // tiny 1KB limit for demo

        // Writes
        System.out.println("\n[Step 1] Writing key-value pairs...");
        mt.set("apple",      "red fruit");
        mt.set("banana",     "yellow fruit");
        mt.set("cherry",     "red berry");
        mt.set("date",       "brown fruit");
        mt.set("elderberry", "dark berry");
        System.out.println("  " + mt);

        // Reads
        System.out.println("\n[Step 2] Reading keys...");
        System.out.println("  GET apple    → " + mt.get("apple"));
        System.out.println("  GET banana   → " + mt.get("banana"));
        System.out.println("  GET mango    → " + mt.get("mango") + " (not found)");

        // Delete (tombstone)
        System.out.println("\n[Step 3] Deleting 'banana'...");
        mt.delete("banana");
        System.out.println("  GET banana   → '" + mt.get("banana") + "'  ← tombstone, key is deleted");

        // Range scan
        System.out.println("\n[Step 4] Range scan: 'b' to 'd'...");
        for (Map.Entry<String, String> e : mt.scan("b", "d")) {
            String marker = TOMBSTONE.equals(e.getValue()) ? " ← TOMBSTONE (deleted)" : "";
            System.out.println("  '" + e.getKey() + "': '" + e.getValue() + "'" + marker);
        }

        // Flush simulation
        System.out.println("\n[Step 5] Items sorted (as they'd be written to SSTable):");
        for (Map.Entry<String, String> e : mt.itemsSorted()) {
            System.out.println("  '" + e.getKey() + "': '" + e.getValue() + "'");
        }

        // Size threshold
        System.out.println("\n[Step 6] Should flush? " + mt.shouldFlush()
                + " (size=" + mt.sizeBytes() + " bytes, limit=1024)");

        System.out.println("\n[Done] MemTable demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. TreeMap keeps keys sorted — O(log n) insert");
        System.out.println("  2. Deletes write a TOMBSTONE, not an actual removal");
        System.out.println("  3. Sorted iteration is free (TreeMap)");
        System.out.println("  4. When size exceeds limit → flush to SSTable on disk");
    }
}