package com.litedb.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * NodeServer — a single database instance (one process). Hosts one replica of every shard placed on
 * this node (multi-raft: it runs many Raft groups at once, leader of some, follower of others).
 *
 * Responsibilities: dispatch inbound Raft RPCs to the right shard; route client ops to the shard's
 * leader (forwarding across nodes that don't host the shard, so it works for any replication
 * factor); coordinate 2PC for cross-shard transactions; expose status + events for the dashboard.
 *
 * Run:  java com.litedb.cluster.NodeServer <node-id>
 */
public final class NodeServer {

    static final long SWEEP_INTERVAL_MS = 2000;   // how often the coordinator re-drives in-doubt txns
    static final double PREPARE_TIMEOUT_S = 10.0;  // 'preparing' this long → coordinator died → abort

    private final String nodeId;
    private final int port;
    private final Partitioner partitioner;
    private final Hlc hlc = new Hlc();
    final EventLog events = new EventLog();
    private final Rpc.Client client = new Rpc.Client(500);
    final Map<String, ShardReplica> shards = new LinkedHashMap<>();
    private final Map<String, Rpc.Handler> handlers = new LinkedHashMap<>();
    private final Rpc.Server server;
    private final TxnLog txnlog;

    public NodeServer(String nodeId) throws Exception {
        this.nodeId = nodeId;
        this.port = ClusterConfig.port(nodeId);
        this.partitioner = ClusterConfig.makePartitioner();
        String dataDir = ClusterConfig.nodeDataDir(nodeId);
        this.txnlog = new TxnLog(dataDir);

        for (String s : partitioner.shardsOn(nodeId)) {
            List<String> peers = new ArrayList<>();
            for (String n : partitioner.replicas(s)) if (!n.equals(nodeId)) peers.add(n);
            shards.put(s, new ShardReplica(nodeId, s, peers, makeSend(s), dataDir, hlc,
                    partitioner.preferredLeader(s).equals(nodeId), events));
        }

        handlers.put("vote", p -> shards.get(p.get("shard")).raft.handleVote(p));
        handlers.put("append", p -> shards.get(p.get("shard")).raft.handleAppend(p));
        handlers.put("put", this::onPut);
        handlers.put("get", this::onGet);
        handlers.put("txn", this::onTxn);
        handlers.put("begin", p -> resp("ok", true, "read_ts", hlc.now()));
        handlers.put("status", this::onStatus);
        handlers.put("events", p -> events.since(p.get("after") == null ? 0 : num(p, "after")));
        handlers.put("shard_leader", this::onShardLeader);
        handlers.put("shard_write", p -> withShard(p, r -> r.commitWrite(writes(p), readTs(p), 3000)));
        handlers.put("shard_get", this::onShardGet);
        handlers.put("shard_prepare", p -> withShard(p, r ->
                r.prepare((String) p.get("txn_id"), writes(p), num(p, "read_ts"), num(p, "commit_ts"),
                        (String) p.get("coordinator"))));
        handlers.put("shard_commit", p -> withShard(p, r -> r.commitPrepared((String) p.get("txn_id"), 3000)));
        handlers.put("shard_abort", p -> withShard(p, r -> r.abortPrepared((String) p.get("txn_id"))));
        this.server = new Rpc.Server(port, handlers);
    }

    private RaftGroup.Transport makeSend(String shardId) {
        return (peerNode, kind, payload) -> {
            Map<String, Object> msg = new LinkedHashMap<>(payload);
            msg.put("shard", shardId);
            return client.call(ClusterConfig.HOST, ClusterConfig.port(peerNode), kind, msg);
        };
    }

    public void start() throws Exception {
        server.start();
        for (ShardReplica r : shards.values()) r.start();
        Thread t = new Thread(this::sweepLoop, "txn-sweep");
        t.setDaemon(true);
        t.start();
    }

    /** Drives in-doubt 2PC to completion. Prepared intents are replicated through each shard's Raft
     * log, so a restarted replica or a new leader rebuilds them automatically — this loop only drives
     * the COORDINATOR side, re-sending commit/abort until every participant has acked. */
    private void sweepLoop() {
        sleep(1500);
        while (true) {
            try {
                sweepOnce();
            } catch (Exception e) {
                events.emit("txn", "sweep error: " + e.getMessage());
            }
            sleep(SWEEP_INTERVAL_MS);
        }
    }

    @SuppressWarnings("unchecked")
    private void sweepOnce() {
        for (Map<String, Object> rec : txnlog.pending()) {
            String status = (String) rec.get("status");
            if ("preparing".equals(status)) {
                double ts = rec.get("ts") == null ? 0 : ((Number) rec.get("ts")).doubleValue();
                if (System.currentTimeMillis() / 1000.0 - ts > PREPARE_TIMEOUT_S) {
                    events.emit("txn", "RECOVERY: txn " + rec.get("txn_id")
                            + " stuck 'preparing' past timeout → deciding ABORT");
                    driveTxn(txnWrite((String) rec.get("txn_id"), "aborted",
                            (List<Object>) rec.get("participants"), ((Number) rec.get("commit_ts")).longValue()));
                }
            } else {
                events.emit("txn", "RECOVERY: re-driving " + status + " txn " + rec.get("txn_id")
                        + " (a participant had not acked)");
                driveTxn(rec);
            }
        }
    }

    public void stop() {
        for (ShardReplica r : shards.values()) r.stop();
        server.stop();
    }

    // ---- local/remote dispatch -------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Object> call(String node, String method, Map<String, Object> payload) {
        if (node.equals(nodeId)) {
            try {
                return (Map<String, Object>) handlers.get(method).handle(payload);
            } catch (Exception e) {
                return resp("ok", false, "error", e.getMessage());
            }
        }
        Map<String, Object> r = client.call(ClusterConfig.HOST, ClusterConfig.port(node), method, payload);
        if (!Boolean.TRUE.equals(r.get("ok"))) return resp("ok", false, "error", r.get("error"));
        return (Map<String, Object>) r.get("result");
    }

    /** Like call(), but retries while a just-elected leader is not yet ready (committing its no-op). */
    private Map<String, Object> callReady(String node, String method, Map<String, Object> payload) {
        Map<String, Object> res = call(node, method, payload);
        for (int i = 0; i < 25 && "not_ready".equals(res.get("error")); i++) {
            sleep(100);
            res = call(node, method, payload);
        }
        return res;
    }

    private String leaderOf(String shard, int retries) {
        for (int i = 0; i < retries; i++) {
            ShardReplica rep = shards.get(shard);
            if (rep != null) {
                String lid = rep.leaderId();
                if (lid != null) return lid;
            } else {
                for (String repNode : partitioner.replicas(shard)) {
                    if (repNode.equals(nodeId)) continue;
                    Map<String, Object> res = call(repNode, "shard_leader", resp("shard", shard));
                    if (Boolean.TRUE.equals(res.get("ok")) && res.get("leader") != null) {
                        return (String) res.get("leader");
                    }
                }
            }
            sleep(100);
        }
        return null;
    }

    private String relationTo(String shard) {
        ShardReplica rep = shards.get(shard);
        if (rep == null) return "I don't host this shard";
        return rep.isLeader() ? "I'm its leader" : "I'm a follower of this shard";
    }

    // ---- client entry points ---------------------------------------------

    private Map<String, Object> onPut(Map<String, Object> p) {
        Map<String, Object> w = new LinkedHashMap<>();
        w.put((String) p.get("key"), p.get("value"));
        return onTxn(resp("writes", w, "read_ts", p.get("read_ts")));
    }

    private Map<String, Object> onShardLeader(Map<String, Object> p) {
        ShardReplica rep = shards.get(p.get("shard"));
        if (rep == null) return resp("ok", false, "error", "not_hosted");
        return resp("ok", true, "leader", rep.leaderId());
    }

    private Map<String, Object> onGet(Map<String, Object> p) {
        String key = (String) p.get("key");
        String shard = partitioner.shardFor(key);
        String relation = relationTo(shard);
        String leader = leaderOf(shard, 15);
        if (leader == null) return resp("ok", false, "error", "no_leader", "shard", shard);
        if (leader.equals(nodeId)) {
            ShardReplica rep = shards.get(shard);
            events.emit("routing", "GET " + key + " → consistent hashing maps it to " + shard
                    + "; " + relation + " → I serve the read locally (MVCC snapshot read)");
            return resp("ok", true, "value", rep.read(key, readTs(p)), "shard", shard,
                    "snapshot_ts", rep.snapshotTs());
        }
        events.emit("routing", "GET " + key + " → maps to " + shard + " (consistent hashing); "
                + relation + ", so I resolved its leader = " + leader
                + " and forward the read there (linearizable)");
        return callReady(leader, "shard_get", resp("shard", shard, "key", key, "read_ts", p.get("read_ts")));
    }

    private Map<String, Object> onShardGet(Map<String, Object> p) {
        ShardReplica rep = shards.get(p.get("shard"));
        if (rep == null) return resp("ok", false, "error", "not_hosted", "shard", p.get("shard"));
        return resp("ok", true, "value", rep.read((String) p.get("key"), readTs(p)),
                "shard", p.get("shard"), "snapshot_ts", rep.snapshotTs());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> onTxn(Map<String, Object> p) {
        Map<String, Object> writes = (Map<String, Object>) p.get("writes");
        Long readTs = readTs(p);
        Map<String, Map<String, Object>> groups = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : writes.entrySet()) {
            groups.computeIfAbsent(partitioner.shardFor(e.getKey()), x -> new LinkedHashMap<>())
                  .put(e.getKey(), e.getValue());
        }
        if (groups.size() == 1) {
            String shard = groups.keySet().iterator().next();
            Map<String, Object> w = groups.get(shard);
            String leader = leaderOf(shard, 15);
            if (leader == null) return resp("ok", false, "error", "no_leader", "shard", shard);
            String relation = relationTo(shard);
            events.emit("routing", "WRITE [" + String.join(", ", w.keySet()) + "] → all in " + shard
                    + " (consistent hashing); " + relation
                    + (leader.equals(nodeId) ? " → single-shard commit via Raft"
                       : ", so I resolved its leader = " + leader + " → forwarding the write there"));
            Map<String, Object> res = callReady(leader, "shard_write",
                    resp("shard", shard, "writes", w, "read_ts", readTs));
            res.putIfAbsent("shards", new ArrayList<>(List.of(shard)));
            return res;
        }
        events.emit("txn", "WRITE spans " + groups.size() + " shards " + groups.keySet()
                + " → these keys do NOT live together, so one Raft commit can't be atomic; "
                + "coordinating a 2-phase commit across the shard leaders");
        return coordinate2pc(groups, readTs);
    }

    private Map<String, Object> coordinate2pc(Map<String, Map<String, Object>> groups, Long readTs) {
        String txnId = "txn-" + nodeId + "-" + hlc.now();
        long commitTs = hlc.now();
        long rts = readTs != null ? readTs : commitTs;

        // Resolve all participant leaders first, then durably record the txn as undecided.
        List<Object> participants = new ArrayList<>();
        for (String shard : groups.keySet()) {
            String leader = leaderOf(shard, 15);
            if (leader == null) return resp("ok", false, "error", "no_leader", "shard", shard);
            participants.add(new ArrayList<>(List.of(leader, shard)));
        }
        txnWrite(txnId, "preparing", participants, commitTs);

        for (Object po : participants) {
            @SuppressWarnings("unchecked")
            List<Object> ls = (List<Object>) po;
            String leader = (String) ls.get(0), shard = (String) ls.get(1);
            events.emit("txn", "2PC " + txnId + ": PREPARE " + shard + " on leader " + leader
                    + " (validate no conflict + stage writes durably, holding a lock)");
            Map<String, Object> res = callReady(leader, "shard_prepare", resp(
                    "shard", shard, "txn_id", txnId, "writes", groups.get(shard),
                    "read_ts", rts, "commit_ts", commitTs, "coordinator", nodeId));
            if (!Boolean.TRUE.equals(res.get("ok"))) {
                events.emit("txn", "2PC " + txnId + ": " + shard + " voted NO (" + res.get("error")
                        + ") → deciding ABORT for the whole transaction (atomicity)");
                driveTxn(txnWrite(txnId, "aborted", participants, commitTs));
                return resp("ok", false, "error", "prepare_failed", "shard", shard);
            }
        }
        // COMMIT POINT: all voted YES → durably record the decision (fsync) before committing, so a
        // crash here — or a participant down right now — is recoverable by the sweep.
        events.emit("txn", "2PC " + txnId + ": all " + participants.size() + " shards voted YES → "
                + "durably recorded the COMMIT decision (fsync) → phase 2: COMMIT on every participant");
        Map<String, Object> rec = txnWrite(txnId, "committing", participants, commitTs);
        Map<String, Object> result = resp("ok", true, "commit_ts", commitTs, "txn_id", txnId,
                "shards", new ArrayList<Object>(groups.keySet()));
        if (!driveTxn(rec)) result.put("pending_recovery", true);
        return result;
    }

    private Map<String, Object> txnWrite(String txnId, String status, List<Object> participants, long commitTs) {
        Map<String, Object> rec = resp("txn_id", txnId, "status", status, "participants", participants,
                "commit_ts", commitTs, "ts", System.currentTimeMillis() / 1000.0);
        txnlog.write(txnId, rec);
        return rec;
    }

    /** Send commit/abort to each participant's CURRENT leader (re-resolved, since leadership may
     * have moved — the intent is replicated so the new leader has it); remove once all ack. */
    @SuppressWarnings("unchecked")
    private boolean driveTxn(Map<String, Object> rec) {
        String method = "committing".equals(rec.get("status")) ? "shard_commit" : "shard_abort";
        String txnId = (String) rec.get("txn_id");
        boolean allOk = true;
        for (Object po : (List<Object>) rec.get("participants")) {
            List<Object> ls = (List<Object>) po;
            String shard = (String) ls.get(1);
            String leader = leaderOf(shard, 1);
            if (leader == null) leader = (String) ls.get(0);
            if (!Boolean.TRUE.equals(call(leader, method, resp("shard", shard, "txn_id", txnId)).get("ok"))) {
                allOk = false;
            }
        }
        if (allOk) txnlog.remove(txnId);
        return allOk;
    }

    private Map<String, Object> onStatus(Map<String, Object> p) {
        List<Object> shardStatus = new ArrayList<>();
        List<String> sorted = new ArrayList<>(shards.keySet());
        sorted.sort(String::compareTo);
        for (String s : sorted) {
            Map<String, Object> st = shards.get(s).status();
            st.put("preferred", partitioner.preferredLeader(s).equals(nodeId));
            shardStatus.add(st);
        }
        return resp("ok", true, "node", nodeId, "alive", true, "shards", shardStatus);
    }

    // ---- helpers ----------------------------------------------------------

    private interface ShardFn { Map<String, Object> apply(ShardReplica r); }

    private Map<String, Object> withShard(Map<String, Object> p, ShardFn fn) {
        ShardReplica rep = shards.get(p.get("shard"));
        if (rep == null) return resp("ok", false, "error", "not_hosted", "shard", p.get("shard"));
        return fn.apply(rep);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> writes(Map<String, Object> p) {
        return (Map<String, Object>) p.get("writes");
    }

    private static Long readTs(Map<String, Object> p) {
        Object v = p.get("read_ts");
        return v == null ? null : ((Number) v).longValue();
    }

    private static long num(Map<String, Object> p, String k) {
        return ((Number) p.get(k)).longValue();
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    static Map<String, Object> resp(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || !ClusterConfig.NODES.containsKey(args[0])) {
            System.out.println("usage: java com.litedb.cluster.NodeServer <node-1|node-2|node-3>");
            System.exit(1);
        }
        NodeServer node = new NodeServer(args[0]);
        node.start();
        System.out.println("[" + args[0] + "] up on " + ClusterConfig.HOST + ":"
                + ClusterConfig.port(args[0]) + ", hosting shards: " + node.shards.keySet());
        Thread.currentThread().join();
    }
}
