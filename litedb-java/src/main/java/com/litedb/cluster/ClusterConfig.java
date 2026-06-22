package com.litedb.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ClusterConfig — shared static topology. Every node process and the client read this, so they all
 * compute routing identically (no central router). Three nodes, six shards; replication factor is
 * configurable via JARVIS_CLUSTER_RF so you can run e.g. RF 2 (each shard on only 2 of 3 nodes).
 */
public final class ClusterConfig {

    // Topology is env-configurable: JARVIS_CLUSTER_NODES (initial nodes), JARVIS_CLUSTER_SHARDS,
    // JARVIS_CLUSTER_RF. Defaults: 3 nodes, 6 shards, RF 3.
    private static int envInt(String k, int def) {
        String v = System.getenv(k);
        return v != null ? Integer.parseInt(v) : def;
    }
    private static final int INITIAL_NODE_COUNT = envInt("JARVIS_CLUSTER_NODES", 3);
    private static final int SHARD_COUNT = envInt("JARVIS_CLUSTER_SHARDS", 6);
    private static final int POOL_SIZE = Math.max(INITIAL_NODE_COUNT + 3, 6);

    // Address book: POOL of possible nodes (so any can reach any). Cluster starts with INITIAL_NODES;
    // more can be added at runtime (up to the pool) or active ones removed.
    public static final Map<String, int[]> NODES = new LinkedHashMap<>();
    static {
        for (int i = 1; i <= POOL_SIZE; i++) NODES.put("node-" + i, new int[]{7100 + i});
    }
    public static final List<String> INITIAL_NODES = new ArrayList<>();
    static {
        for (int i = 1; i <= INITIAL_NODE_COUNT; i++) INITIAL_NODES.add("node-" + i);
    }
    public static final String HOST = "127.0.0.1";

    public static final List<String> SHARDS = new ArrayList<>();
    static {
        for (int i = 0; i < SHARD_COUNT; i++) SHARDS.add("shard-" + i);
    }

    public static final int DASHBOARD_PORT = 7180;

    public static int replicationFactor() {
        String rf = System.getenv("JARVIS_CLUSTER_RF");
        return rf != null ? Integer.parseInt(rf) : 3;
    }

    public static String dataRoot() {
        String d = System.getenv("JARVIS_CLUSTER_DATA");
        return d != null ? d : System.getProperty("java.io.tmpdir") + "/litedb_cluster_java";
    }

    public static int port(String nodeId) { return NODES.get(nodeId)[0]; }

    public static List<String> nodeIds() { return new ArrayList<>(NODES.keySet()); }

    public static Partitioner makePartitioner() {
        return new Partitioner(SHARDS, INITIAL_NODES, replicationFactor(), 64);
    }

    /** Even round-robin assignment of shards to `rf` of the active nodes (round-robin order, NOT
     * sorted, so the preferred leader — replicas[0] — rotates and write load spreads). The balancer's
     * target; recomputing it when the node set changes yields the shards that must move. */
    public static Map<String, List<String>> computePlacement(List<String> nodes) {
        int rf = Math.min(replicationFactor(), nodes.size());
        Map<String, List<String>> p = new LinkedHashMap<>();
        for (int i = 0; i < SHARDS.size(); i++) {
            List<String> reps = new ArrayList<>();
            for (int j = 0; j < rf; j++) reps.add(nodes.get((i + j) % nodes.size()));
            p.put(SHARDS.get(i), reps);
        }
        return p;
    }

    public static String nodeDataDir(String nodeId) {
        return dataRoot() + "/" + nodeId;
    }

    private ClusterConfig() {}
}
