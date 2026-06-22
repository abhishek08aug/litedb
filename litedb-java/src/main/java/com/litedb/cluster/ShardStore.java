package com.litedb.cluster;

import com.litedb.engine.WriteOp;
import com.litedb.lsm.LSMEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ShardStore — the per-shard MVCC state machine driven by Raft.
 *
 * Each shard replica owns one: an LSMEngine plus the MVCC versioning rules. The crucial property
 * for replicated MVCC is DETERMINISTIC apply — the leader assigns the commit timestamp and the
 * exact versioned writes, replicates that command through Raft, and every replica applies the
 * identical versioned puts, so all replicas converge byte-for-byte. Mirrors the single-node MVCC
 * version-key encoding (userKey + \0 + %016x(MAX-ts)) so newer versions sort first.
 */
public final class ShardStore {

    static final char SEP = (char) 0;
    static final String SEP_S = String.valueOf(SEP);
    static final String TS_MAX_HEX = "ffffffffffffffff";
    static final String HI = "￿";   // sorts after any normal user-key char
    static final String TOMBSTONE = " __DELETED__";

    private final LSMEngine engine;
    private long lastTs;

    public ShardStore(String dataDir) throws IOException {
        this.engine = new LSMEngine(dataDir);
        this.lastTs = recoverMaxTs();
    }

    static String versionKey(String userKey, long ts) {
        return userKey + SEP_S + String.format("%016x", Long.MAX_VALUE - ts);
    }

    static long tsFromVersionKey(String vkey) {
        return Long.MAX_VALUE - Long.parseUnsignedLong(vkey.substring(vkey.length() - 16), 16);
    }

    static String userKeyFromVersionKey(String vkey) {
        return vkey.substring(0, vkey.length() - 17);
    }

    private long recoverMaxTs() throws IOException {
        long mx = 0;
        for (Map.Entry<String, String> e : engine.scan("", HI)) {
            long ts = tsFromVersionKey(e.getKey());
            if (ts > mx) mx = ts;
        }
        return mx;
    }

    // ---- apply (runs on every replica, in Raft log order) -----------------

    @SuppressWarnings("unchecked")
    public synchronized void apply(Map<String, Object> command) {
        long ts = ((Number) command.get("ts")).longValue();
        List<Object> writes = (List<Object>) command.get("writes");
        List<WriteOp> ops = new ArrayList<>();
        for (Object wo : writes) {
            List<Object> kv = (List<Object>) wo;
            String uk = (String) kv.get(0);
            Object value = kv.get(1);
            ops.add(WriteOp.put(versionKey(uk, ts), value == null ? TOMBSTONE : (String) value));
        }
        try {
            if (!ops.isEmpty()) engine.writeBatch(ops);
        } catch (IOException e) {
            throw new RuntimeException("apply failed", e);
        }
        if (ts > lastTs) lastTs = ts;
    }

    public synchronized long snapshotTs() {
        return lastTs;
    }

    // ---- snapshot reads ---------------------------------------------------

    public String read(String key, long readTs) {
        try {
            String lo = versionKey(key, readTs);
            String hi = key + SEP_S + TS_MAX_HEX;
            for (Map.Entry<String, String> e : engine.scan(lo, hi)) {
                return TOMBSTONE.equals(e.getValue()) ? null : e.getValue();
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Map.Entry<String, String>> scan(String loUser, String hiUser, long readTs) {
        try {
            List<Map.Entry<String, String>> out = new ArrayList<>();
            String cur = null;
            boolean resolved = false;
            for (Map.Entry<String, String> e : engine.scan(loUser, hiUser + HI)) {
                String uk = userKeyFromVersionKey(e.getKey());
                long ts = tsFromVersionKey(e.getKey());
                if (!uk.equals(cur)) {
                    cur = uk;
                    resolved = false;
                }
                if (resolved || ts > readTs) continue;
                resolved = true;
                if (!TOMBSTONE.equals(e.getValue())) {
                    out.add(Map.entry(uk, e.getValue()));
                }
            }
            return out;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long newestCommittedTs(String key) {
        try {
            for (Map.Entry<String, String> e : engine.scan(key + SEP_S, key + SEP_S + TS_MAX_HEX)) {
                return tsFromVersionKey(e.getKey());
            }
            return 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try { engine.close(); } catch (IOException ignored) {}
    }
}
