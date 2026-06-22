package com.litedb.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * ShardReplica — one node's replica of one shard: a {@link RaftGroup} bound to a {@link ShardStore},
 * plus the leader-side transactional commit path (conflict-check → assign HLC commit timestamp →
 * propose through Raft → wait for majority). For cross-shard transactions the 2PC coordinator drives
 * prepare/commit across several of these.
 *
 * The 2PC lock is a {@link Semaphore} (not a ReentrantLock) because prepare and commit arrive on
 * different RPC threads, and a semaphore permit may be released by a thread other than the acquirer.
 */
public final class ShardReplica {

    public final String nodeId;
    public final String shardId;
    private final Hlc hlc;
    public final ShardStore store;
    public final RaftGroup raft;
    private final Semaphore commitLock = new Semaphore(1, true);
    private final Map<String, Object[]> prepared = new ConcurrentHashMap<>();  // txnId -> [commitTs, writes]

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
    public String leaderId() { return raft.leaderId(); }
    public long snapshotTs() { return store.snapshotTs(); }

    // ---- single-shard transactional write ---------------------------------

    public Map<String, Object> commitWrite(Map<String, Object> writes, Long readTs, long timeoutMs) {
        commitLock.acquireUninterruptibly();
        try {
            if (!raft.isLeader()) return err("not_leader", "leader", raft.leaderId());
            long rts = readTs != null ? readTs : store.snapshotTs();
            String conflict = checkConflicts(writes, rts);
            if (conflict != null) return err("conflict", "key", conflict);
            long commitTs = readTs != null ? hlc.update(rts) : hlc.now();
            Long index = raft.propose(buildCommand(commitTs, writes));
            if (index == null) return err("not_leader");
            boolean ok = raft.waitCommit(index, timeoutMs);
            if (!ok) return err("timeout");
            Map<String, Object> r = ok();
            r.put("commit_ts", commitTs);
            return r;
        } finally {
            commitLock.release();
        }
    }

    private String checkConflicts(Map<String, Object> writes, long readTs) {
        for (String key : writes.keySet()) {
            if (store.newestCommittedTs(key) > readTs) return key;
        }
        return null;
    }

    // ---- 2PC participant side ---------------------------------------------

    public Map<String, Object> prepare(String txnId, Map<String, Object> writes, long readTs, long commitTs) {
        commitLock.acquireUninterruptibly();
        boolean release = true;
        try {
            if (!raft.isLeader()) return err("not_leader", "leader", raft.leaderId());
            String conflict = checkConflicts(writes, readTs);
            if (conflict != null) return err("conflict", "key", conflict);
            prepared.put(txnId, new Object[]{commitTs, writes});
            release = false;  // hold the permit until commit/abort
            return ok();
        } finally {
            if (release) commitLock.release();
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> commitPrepared(String txnId, long timeoutMs) {
        Object[] staged = prepared.remove(txnId);
        if (staged == null) return err("unknown_txn");
        try {
            long commitTs = (Long) staged[0];
            Map<String, Object> writes = (Map<String, Object>) staged[1];
            Long index = raft.propose(buildCommand(commitTs, writes));
            if (index == null) return err("not_leader");
            boolean ok = raft.waitCommit(index, timeoutMs);
            return ok ? ok() : err("timeout");
        } finally {
            commitLock.release();
        }
    }

    public Map<String, Object> abortPrepared(String txnId) {
        if (prepared.remove(txnId) != null) {
            commitLock.release();
        }
        return ok();
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

    private static Map<String, Object> buildCommand(long commitTs, Map<String, Object> writes) {
        List<Object> ws = new ArrayList<>();
        for (Map.Entry<String, Object> e : writes.entrySet()) {
            List<Object> kv = new ArrayList<>();
            kv.add(e.getKey());
            kv.add(e.getValue());  // null encodes a delete
            ws.add(kv);
        }
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("ts", commitTs);
        cmd.put("writes", ws);
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
