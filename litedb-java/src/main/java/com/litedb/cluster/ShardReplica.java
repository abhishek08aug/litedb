package com.litedb.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ShardReplica — one node's replica of one shard: a {@link RaftGroup} bound to a {@link ShardStore},
 * plus the leader-side transactional commit path. 2PC PREPARE/COMMIT/ABORT are replicated through
 * Raft as intent entries, so a prepared intent survives a leadership change (any new leader has it)
 * — the participant-recovery property. The in-process lock just serializes the check+propose section.
 */
public final class ShardReplica {

    public final String nodeId;
    public final String shardId;
    private final Hlc hlc;
    public final ShardStore store;
    public final RaftGroup raft;
    private final ReentrantLock lock = new ReentrantLock();

    public ShardReplica(String nodeId, String shardId, List<String> peers, RaftGroup.Transport transport,
                        String dataDir, Hlc hlc, boolean preferred, RaftGroup.Events events) throws IOException {
        this.nodeId = nodeId;
        this.shardId = shardId;
        this.hlc = hlc;
        this.store = new ShardStore(dataDir + "/shard-" + shardId + "-data");
        this.raft = new RaftGroup(nodeId, shardId, peers, transport,
                (index, command) -> store.apply(command),
                dataDir + "/shard-" + shardId + "-raft", preferred, events);
    }

    public void start() { raft.start(); }
    public void stop() { raft.stop(); store.close(); }
    public boolean isLeader() { return raft.isLeader(); }
    public boolean isReady() { return raft.isReady(); }
    public String leaderId() { return raft.leaderId(); }
    public long snapshotTs() { return store.snapshotTs(); }

    // ---- single-shard transactional write ---------------------------------

    public Map<String, Object> commitWrite(Map<String, Object> writes, Long readTs, long timeoutMs) {
        lock.lock();
        try {
            if (!raft.isLeader()) return err("not_leader", "leader", raft.leaderId());
            if (!raft.isReady()) return err("not_ready");
            long rts = readTs != null ? readTs : store.snapshotTs();
            Map<String, Object> conflict = checkConflicts(writes, rts, "");
            if (conflict != null) return conflict;
            long commitTs = readTs != null ? hlc.update(rts) : hlc.now();
            Long index = raft.propose(buildCommand(commitTs, writes));
            if (index == null) return err("not_leader");
            boolean ok = raft.waitCommit(index, timeoutMs);
            if (!ok) return err("timeout");
            Map<String, Object> r = ok();
            r.put("commit_ts", commitTs);
            return r;
        } finally {
            lock.unlock();
        }
    }

    /** A write conflicts if a newer committed version exists (OCC) or a key is locked by another
     * prepared 2PC intent. Returns an error map, or null if clear. */
    private Map<String, Object> checkConflicts(Map<String, Object> writes, long readTs, String txnId) {
        for (String key : writes.keySet()) {
            if (store.newestCommittedTs(key) > readTs) return err("conflict", "key", key);
            String locker = store.intentLocking(key, txnId);
            if (locker != null) return err("locked", "key", key, "by", locker);
        }
        return null;
    }

    // ---- 2PC participant side (intents replicated through Raft) ------------

    public Map<String, Object> prepare(String txnId, Map<String, Object> writes, long readTs,
                                       long commitTs, String coordinator) {
        lock.lock();
        try {
            if (!raft.isLeader()) return err("not_leader", "leader", raft.leaderId());
            if (!raft.isReady()) return err("not_ready");
            Map<String, Object> conflict = checkConflicts(writes, readTs, txnId);
            if (conflict != null) return conflict;
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("op", "prepare");
            cmd.put("txn_id", txnId);
            cmd.put("commit_ts", commitTs);
            cmd.put("writes", writesList(writes));
            Long index = raft.propose(cmd);
            if (index == null) return err("not_leader");
            return raft.waitCommit(index, 3000) ? ok() : err("timeout");
        } finally {
            lock.unlock();
        }
    }

    public Map<String, Object> commitPrepared(String txnId, long timeoutMs) {
        Long index = raft.propose(txnOp("commit", txnId));
        if (index == null) return err("not_leader");
        return raft.waitCommit(index, timeoutMs) ? ok() : err("timeout");
    }

    public Map<String, Object> abortPrepared(String txnId) {
        Long index = raft.propose(txnOp("abort", txnId));
        if (index == null) return err("not_leader");
        return raft.waitCommit(index, 3000) ? ok() : err("timeout");
    }

    private static Map<String, Object> txnOp(String op, String txnId) {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("op", op);
        cmd.put("txn_id", txnId);
        return cmd;
    }

    // ---- reads ------------------------------------------------------------

    public String read(String key, Long readTs) {
        return store.read(key, readTs != null ? readTs : store.snapshotTs());
    }

    public List<Map.Entry<String, String>> scan(String lo, String hi, Long readTs) {
        return store.scan(lo, hi, readTs != null ? readTs : store.snapshotTs());
    }

    public Map<String, Object> status() { return raft.status(); }

    // ---- helpers ----------------------------------------------------------

    private static List<Object> writesList(Map<String, Object> writes) {
        List<Object> ws = new ArrayList<>();
        for (Map.Entry<String, Object> e : writes.entrySet()) {
            List<Object> kv = new ArrayList<>();
            kv.add(e.getKey());
            kv.add(e.getValue());  // null encodes a delete
            ws.add(kv);
        }
        return ws;
    }

    private static Map<String, Object> buildCommand(long commitTs, Map<String, Object> writes) {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("ts", commitTs);
        cmd.put("writes", writesList(writes));
        return cmd;
    }

    private static Map<String, Object> ok() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", true);
        return m;
    }

    private static Map<String, Object> err(String error, Object... extra) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", false);
        m.put("error", error);
        for (int i = 0; i < extra.length; i += 2) m.put((String) extra[i], extra[i + 1]);
        return m;
    }
}
