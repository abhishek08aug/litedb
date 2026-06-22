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

    public static final Map<String, int[]> NODES = new LinkedHashMap<>();
    static {
        NODES.put("node-1", new int[]{7101});
        NODES.put("node-2", new int[]{7102});
        NODES.put("node-3", new int[]{7103});
    }
    public static final String HOST = "127.0.0.1";

    public static final List<String> SHARDS = new ArrayList<>();
    static {
        for (int i = 0; i < 6; i++) SHARDS.add("shard-" + i);
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
        return new Partitioner(SHARDS, nodeIds(), replicationFactor(), 64);
    }

    public static String nodeDataDir(String nodeId) {
        return dataRoot() + "/" + nodeId;
    }

    private ClusterConfig() {}
}
