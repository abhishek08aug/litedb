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
                r.prepare((String) p.get("txn_id"), writes(p), num(p, "read_ts"), num(p, "commit_ts"))));
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
        Thread t = new Thread(this::recoverTxns, "txn-recovery");
        t.setDaemon(true);
        t.start();
    }

    /** On restart, finish any 2PC this node was coordinating when it died. */
    @SuppressWarnings("unchecked")
    private void recoverTxns() {
        sleep(2000);  // let the RPC server + peers come up and elect leaders
        for (Map<String, Object> rec : txnlog.pending()) {
            String txnId = (String) rec.get("txn_id");
            boolean commit = "committing".equals(rec.get("status"));
            events.emit("txn", "RECOVERY: in-doubt txn " + txnId + " was '" + rec.get("status")
                    + "' when I crashed → resolving as " + (commit ? "COMMIT" : "ABORT") + " on restart");
            for (Object po : (List<Object>) rec.get("participants")) {
                List<Object> ls = (List<Object>) po;
                call((String) ls.get(0), commit ? "shard_commit" : "shard_abort",
                        resp("shard", ls.get(1), "txn_id", txnId));
            }
            txnlog.remove(txnId);
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
        return call(leader, "shard_get", resp("shard", shard, "key", key, "read_ts", p.get("read_ts")));
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
            Map<String, Object> res = call(leader, "shard_write",
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
        txnlog.write(txnId, resp("txn_id", txnId, "status", "preparing",
                "participants", participants, "commit_ts", commitTs));

        List<String[]> prepared = new ArrayList<>();
        for (Object po : participants) {
            @SuppressWarnings("unchecked")
            List<Object> ls = (List<Object>) po;
            String leader = (String) ls.get(0), shard = (String) ls.get(1);
            events.emit("txn", "2PC " + txnId + ": PREPARE " + shard + " on leader " + leader
                    + " (validate no conflict + stage writes, holding a lock)");
            Map<String, Object> res = call(leader, "shard_prepare", resp(
                    "shard", shard, "txn_id", txnId, "writes", groups.get(shard),
                    "read_ts", rts, "commit_ts", commitTs));
            if (!Boolean.TRUE.equals(res.get("ok"))) {
                events.emit("txn", "2PC " + txnId + ": " + shard + " voted NO (" + res.get("error")
                        + ") → ABORTING the whole transaction (atomicity)");
                abortAll(prepared, txnId);
                txnlog.remove(txnId);
                return resp("ok", false, "error", "prepare_failed", "shard", shard);
            }
            prepared.add(new String[]{leader, shard});
        }
        // COMMIT POINT: all voted YES → durably record the decision (fsync) before committing, so a
        // crash here is recoverable (the restart sweep re-sends the commits).
        txnlog.write(txnId, resp("txn_id", txnId, "status", "committing",
                "participants", participants, "commit_ts", commitTs));
        events.emit("txn", "2PC " + txnId + ": all " + prepared.size() + " shards voted YES → durably"
                + " recorded the COMMIT decision (fsync) → phase 2: COMMIT on every participant");
        for (String[] ls : prepared) call(ls[0], "shard_commit", resp("shard", ls[1], "txn_id", txnId));
        txnlog.remove(txnId);
        return resp("ok", true, "commit_ts", commitTs, "txn_id", txnId,
                "shards", new ArrayList<Object>(groups.keySet()));
    }

    private void abortAll(List<String[]> prepared, String txnId) {
        for (String[] ls : prepared) call(ls[0], "shard_abort", resp("shard", ls[1], "txn_id", txnId));
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
