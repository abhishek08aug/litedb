package com.litedb.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * StorageEngine — the storage abstraction the server and query layer depend on.
 *
 * LiteDB ships two implementations, chosen at server startup (--engine):
 *   - {@code com.litedb.lsm.LSMEngine}     — write-optimized LSM-Tree (WAL + MemTable + SSTables)
 *   - {@code com.litedb.engine.BTreeEngine} — read-optimized in-memory B+Tree (durable via WAL)
 *
 * This mirrors how real databases let the workload pick a storage engine (e.g. MySQL's
 * pluggable engines: InnoDB B-tree vs MyRocks LSM).
 *
 * Secondary-index support is optional and advertised via {@link #supportsSecondaryIndex()}.
 */
public interface StorageEngine extends Closeable {

    /** Insert or update a key. */
    void set(String key, String value) throws IOException;

    /** Delete a key. */
    void delete(String key) throws IOException;

    /** Point lookup; returns null if absent or deleted. */
    String get(String key) throws IOException;

    /** Ordered range scan over [startKey, endKey] (inclusive, lexicographic). */
    List<Map.Entry<String, String>> scan(String startKey, String endKey) throws IOException;

    /** Force any buffered data to durable storage. */
    void flush() throws IOException;

    /** Engine statistics for the STATS command. */
    Map<String, Object> stats();

    /** Short engine name ("lsm" / "btree"). */
    String name();

    // ---- optional secondary-index capability ---------------------------------

    /** Whether this engine maintains a secondary (value) index. */
    default boolean supportsSecondaryIndex() {
        return false;
    }

    /**
     * Reverse lookup: primary keys whose stored value falls in [lowValue, highValue]
     * (inclusive, lexicographic), served from the secondary index rather than a full scan.
     */
    default List<String> findByValueRange(String lowValue, String highValue) throws IOException {
        throw new UnsupportedOperationException(
                "Engine '" + name() + "' has no secondary index");
    }

    // ---- optional atomic multi-key write -----------------------------------

    /** Whether {@link #writeBatch} is applied atomically (all-or-nothing across crashes). */
    default boolean supportsAtomicBatch() {
        return false;
    }

    /**
     * Apply a set of writes. Atomic-capable engines (see {@link #supportsAtomicBatch}) commit the
     * whole batch via a single WAL record (all-or-nothing on recovery); the default simply applies
     * the ops sequentially, which is NOT crash-atomic.
     */
    default void writeBatch(List<WriteOp> ops) throws IOException {
        for (WriteOp op : ops) {
            if (op.delete) delete(op.key);
            else set(op.key, op.value);
        }
    }
}
