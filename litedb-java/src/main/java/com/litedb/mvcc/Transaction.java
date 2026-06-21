package com.litedb.mvcc;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transaction — a single MVCC transaction.
 *
 * Reads see a consistent snapshot taken at begin ({@code readTs}); writes are buffered and become
 * visible only at {@link #commit()}, all under one commit timestamp. Within the transaction, reads
 * see the transaction's own pending writes ("read your writes").
 */
public final class Transaction {

    private final MVCCEngine mvcc;
    private final long readTs;                                  // snapshot timestamp
    private final Map<String, String> writes = new LinkedHashMap<>();  // key -> value | TOMBSTONE
    private boolean finished = false;
    private long commitTs = -1;

    Transaction(MVCCEngine mvcc, long readTs) {
        this.mvcc = mvcc;
        this.readTs = readTs;
    }

    public long readTs() { return readTs; }
    public long commitTs() { return commitTs; }

    /** Point read at the transaction's snapshot (own pending writes win). Null if absent/deleted. */
    public String get(String key) throws IOException {
        ensureActive();
        if (writes.containsKey(key)) {
            String v = writes.get(key);
            return MVCCEngine.TOMBSTONE.equals(v) ? null : v;
        }
        return mvcc.read(key, readTs);
    }

    public void put(String key, String value) {
        ensureActive();
        writes.put(key, value);
    }

    public void delete(String key) {
        ensureActive();
        writes.put(key, MVCCEngine.TOMBSTONE);
    }

    /** Commit atomically. Throws {@link ConflictException} (and stays finished) on write-write conflict. */
    public long commit() throws IOException {
        ensureActive();
        finished = true;
        commitTs = mvcc.commit(readTs, writes);
        return commitTs;
    }

    public void rollback() {
        finished = true;
        writes.clear();
    }

    private void ensureActive() {
        if (finished) throw new IllegalStateException("transaction already finished");
    }
}
