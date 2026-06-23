package com.litedb.cluster;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Proves dynamic membership: add a node (shards + data rebalance onto it) and remove a node (its
 * shards re-replicate to restore RF), online, data intact. Java mirror of rebalance_smoke.py. */
public class ClusterRebalanceSmoke {

    static Map<String, Process> procs = new LinkedHashMap<>();

    static Process spawn(String nid) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(System.getProperty("java.home") + "/bin/java",
                "-cp", System.getProperty("java.class.path"), "com.litedb.cluster.NodeServer", nid);
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process p = pb.start();
        procs.put(nid, p);
        return p;
    }

    @SuppressWarnings("unchecked")
    static Map<String, java.util.Set<String>> hostsOf(ClusterClient client) {
        Map<String, java.util.Set<String>> out = new TreeMap<>();
        for (Map<String, Object> st : client.status()) {
            if (!Boolean.TRUE.equals(st.get("alive"))) continue;
            java.util.Set<String> shards = new java.util.HashSet<>();
            for (Object so : (List<Object>) st.get("shards")) {
                shards.add((String) ((Map<String, Object>) so).get("group"));
            }
            out.put((String) st.get("node"), shards);
        }
        return out;
    }

    static boolean allRf3(Map<String, java.util.Set<String>> hosts, int nShards) {
        Map<String, Integer> c = new java.util.HashMap<>();
        for (java.util.Set<String> s : hosts.values()) for (String sh : s) c.merge(sh, 1, Integer::sum);
        if (c.size() != nShards) return false;
        for (int v : c.values()) if (v != 3) return false;
        return true;
    }

    @SuppressWarnings("unchecked")
    static int readyLeaders(ClusterClient client) {
        java.util.Set<String> withLeader = new java.util.HashSet<>();
        for (Map<String, Object> st : client.status()) {
            if (st.get("shards") == null) continue;
            for (Object so : (List<Object>) st.get("shards")) {
                Map<String, Object> sh = (Map<String, Object>) so;
                if ("leader".equals(sh.get("role")) && Boolean.TRUE.equals(sh.get("ready"))) {
                    withLeader.add((String) sh.get("group"));
                }
            }
        }
        return withLeader.size();
    }

    static void waitUntil(java.util.function.BooleanSupplier c, long ms, String what) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) { if (c.getAsBoolean()) return; Thread.sleep(250); }
        throw new AssertionError("timeout: " + what);
    }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(ClusterConfig.dataRoot());
        if (Files.exists(root)) Files.walk(root).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        try {
            for (String n : ClusterConfig.INITIAL_NODES) spawn(n);
            ClusterClient client = new ClusterClient();
            Controller ctrl = new Controller(ClusterConfig.INITIAL_NODES, System.out::println);
            waitUntil(() -> readyLeaders(client) == 6, 20000, "6 leaders on initial 3 nodes");
            ctrl.broadcastPlacement();
            System.out.println("started with " + ClusterConfig.INITIAL_NODES + "; each shard on all 3");

            System.out.println("writing 12 keys...");
            for (int i = 0; i < 12; i++) if (!Boolean.TRUE.equals(client.put("key" + i, "val" + i).get("ok"))) throw new AssertionError("put key" + i);
            for (int i = 0; i < 12; i++) if (!("val" + i).equals(client.get("key" + i))) throw new AssertionError("read key" + i);
            System.out.println("  all 12 readable");

            System.out.println("\nADD node-4 (the PD Raft group rebalances onto it asynchronously)...");
            spawn("node-4");
            Thread.sleep(1500);
            ctrl.addNode("node-4");
            // the PD reconciles async — poll observable placement until node-4 hosts shards and RF 3
            waitUntil(() -> {
                Map<String, java.util.Set<String>> h = hostsOf(client);
                return h.containsKey("node-4") && !h.get("node-4").isEmpty()
                        && allRf3(h, 6) && readyLeaders(client) == 6;
            }, 45000, "node-4 hosts shards and every shard is RF 3 after add");
            System.out.println("  node-4 now hosts " + hostsOf(client).get("node-4"));
            for (int i = 0; i < 12; i++) if (!("val" + i).equals(client.get("key" + i))) throw new AssertionError("data lost after add");
            if (!Boolean.TRUE.equals(client.put("after-add", "ok").get("ok"))) throw new AssertionError("write after add");
            System.out.println("  data moved onto node-4; reads/writes fine after add");

            System.out.println("\nREMOVE node-4 (the PD re-replicates its shards back to restore RF)...");
            ctrl.removeNode("node-4", false);
            waitUntil(() -> {
                Map<String, java.util.Set<String>> h = hostsOf(client);
                // node-4 stays alive after draining, so it appears with an EMPTY shard set
                return h.getOrDefault("node-4", java.util.Set.of()).isEmpty()
                        && allRf3(h, 6) && readyLeaders(client) == 6;
            }, 45000, "node-4 drained and RF restored on 3 nodes");
            for (int i = 0; i < 12; i++) if (!("val" + i).equals(client.get("key" + i))) throw new AssertionError("data lost after remove");
            System.out.println("  node-4 drained; RF restored; data intact");

            System.out.println("\nREBALANCE OK (Java): added a node (shards+data moved onto it) and removed it "
                    + "(re-replicated), online, data preserved.");
        } finally {
            for (Process p : procs.values()) p.destroyForcibly();
            for (Process p : procs.values()) p.waitFor();
        }
        System.exit(0);
    }
}
