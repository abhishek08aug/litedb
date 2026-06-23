package com.litedb.cluster;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * AutoHealSmoke — proves the gossip failure detector AUTO-restores replication factor (mirror of
 * autoheal_smoke.py). Start 4 nodes (RF 3), turn on the controller's failure detector, then KILL a
 * node with NO manual controller call. Gossip marks it dead; the reconcile loop sees the majority
 * verdict and re-replicates its shards onto survivors → RF back to 3, data intact.
 *
 * Run with 4 nodes:  LITEDB_CLUSTER_NODES=4 java com.litedb.cluster.AutoHealSmoke
 */
public class AutoHealSmoke {

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

    static Map<String, Integer> rfCount(Map<String, java.util.Set<String>> hosts) {
        Map<String, Integer> c = new HashMap<>();
        for (java.util.Set<String> shards : hosts.values()) {
            for (String s : shards) c.merge(s, 1, Integer::sum);
        }
        return c;
    }

    static void waitUntil(java.util.function.BooleanSupplier c, long ms, String what) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) { if (c.getAsBoolean()) return; Thread.sleep(250); }
        throw new AssertionError("timeout: " + what);
    }

    public static void main(String[] args) throws Exception {
        int nShards = ClusterConfig.SHARDS.size();
        Path root = Paths.get(ClusterConfig.dataRoot());
        if (Files.exists(root)) Files.walk(root).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        Controller ctrl = new Controller(ClusterConfig.INITIAL_NODES, System.out::println);
        try {
            for (String n : ClusterConfig.INITIAL_NODES) spawn(n);
            ClusterClient client = new ClusterClient();
            waitUntil(() -> readyLeaders(client) == nShards, 25000,
                    nShards + " leaders on initial " + ClusterConfig.INITIAL_NODES.size() + " nodes");
            ctrl.broadcastPlacement();
            System.out.println("started with " + ClusterConfig.INITIAL_NODES + " (RF 3)");

            for (int i = 0; i < 12; i++) {
                if (!Boolean.TRUE.equals(client.put("key" + i, "val" + i).get("ok"))) throw new AssertionError("put key" + i);
            }
            for (int i = 0; i < 12; i++) if (!("val" + i).equals(client.get("key" + i))) throw new AssertionError("read key" + i);
            System.out.println("  wrote + read 12 keys");

            // The PD leader runs the failure detector autonomously — nothing to start. Kill a non-PD
            // data node (a clean data-node death; killing a PD member is covered by PdFailoverSmoke).
            String victim = ClusterConfig.INITIAL_NODES.get(ClusterConfig.INITIAL_NODES.size() - 1);
            System.out.println("\nkilling " + victim + " — NO manual controller call; the PD failure detector must notice");
            procs.get(victim).destroyForcibly();
            procs.get(victim).waitFor();

            waitUntil(() -> !ctrl.active().contains(victim), 40000, "controller starts reaping the dead node");
            System.out.println("  controller detected " + victim + " dead and is re-replicating (active now " + ctrl.active() + ")");

            // poll OBSERVABLE replication state — removeNode frees `active` first and re-replicates after.
            waitUntil(() -> {
                Map<String, java.util.Set<String>> h = hostsOf(client);
                if (h.containsKey(victim)) return false;
                Map<String, Integer> c = rfCount(h);
                if (c.size() != nShards) return false;
                for (int v : c.values()) if (v != 3) return false;
                return true;
            }, 40000, "RF re-replicated back to 3 on every shard");

            Map<String, java.util.Set<String>> hosts = hostsOf(client);
            if (hosts.containsKey(victim)) throw new AssertionError(victim + " should host nothing");
            for (int v : rfCount(hosts).values()) if (v != 3) throw new AssertionError("RF should be 3 on survivors: " + rfCount(hosts));
            for (int i = 0; i < 12; i++) if (!("val" + i).equals(client.get("key" + i))) throw new AssertionError("data lost after auto-heal");
            System.out.println("  RF restored to 3 across survivors " + hosts.keySet() + "; all 12 keys intact");

            System.out.println("\nAUTO-HEAL OK (Java): a node died with zero manual action; gossip flagged it DEAD "
                    + "and the controller's failure detector re-replicated to restore RF, data preserved.");
        } finally {
            ctrl.stop();
            for (Process p : procs.values()) p.destroyForcibly();
            for (Process p : procs.values()) p.waitFor();
        }
        System.exit(0);
    }
}
