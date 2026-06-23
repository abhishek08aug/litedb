package com.litedb.cluster;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * PdFailoverSmoke — proves the control plane survives its OWN leader's death (mirror of
 * pd_failover_smoke.py). Kill the PD leader (also a data node) and assert (1) a new PD leader is
 * elected among the surviving PD replicas, and (2) it finishes healing the dead node's data to
 * restore RF — with no human action. The control plane is no longer a SPOF.
 *
 * Run with 4 nodes:  LITEDB_CLUSTER_NODES=4 java com.litedb.cluster.PdFailoverSmoke
 */
public class PdFailoverSmoke {

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
    static String pdLeader(Rpc.Client rpc, Set<String> exclude) {
        for (String n : ClusterConfig.PD_NODES) {
            if (exclude.contains(n)) continue;
            Map<String, Object> r = rpc.call(ClusterConfig.HOST, ClusterConfig.port(n), "pd_status", NodeServer.resp());
            if (Boolean.TRUE.equals(r.get("ok")) && r.get("result") instanceof Map) {
                Map<String, Object> res = (Map<String, Object>) r.get("result");
                if (Boolean.TRUE.equals(res.get("ok")) && res.get("leader") != null) return (String) res.get("leader");
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Set<String>> hostsOf(ClusterClient client) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (Map<String, Object> st : client.status()) {
            if (!Boolean.TRUE.equals(st.get("alive"))) continue;
            Set<String> shards = new java.util.HashSet<>();
            for (Object so : (List<Object>) st.get("shards")) shards.add((String) ((Map<String, Object>) so).get("group"));
            out.put((String) st.get("node"), shards);
        }
        return out;
    }

    static boolean allRf3(Map<String, Set<String>> hosts, int nShards) {
        Map<String, Integer> c = new java.util.HashMap<>();
        for (Set<String> s : hosts.values()) for (String sh : s) c.merge(sh, 1, Integer::sum);
        if (c.size() != nShards) return false;
        for (int v : c.values()) if (v != 3) return false;
        return true;
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
        Rpc.Client rpc = new Rpc.Client(1500);
        try {
            for (String n : ClusterConfig.INITIAL_NODES) spawn(n);
            ClusterClient client = new ClusterClient();
            waitUntil(() -> pdLeader(rpc, Set.of()) != null, 25000, "initial PD leader elected");
            String leader1 = pdLeader(rpc, Set.of());
            System.out.println("PD Raft group up; leader = " + leader1 + " (PD replicas: " + ClusterConfig.PD_NODES + ")");

            for (int i = 0; i < 12; i++) if (!Boolean.TRUE.equals(client.put("key" + i, "val" + i).get("ok"))) throw new AssertionError("put key" + i);
            System.out.println("  wrote 12 keys");

            System.out.println("\nkilling the PD LEADER " + leader1 + " (also a data node) — the control plane must survive");
            procs.get(leader1).destroyForcibly();
            procs.get(leader1).waitFor();

            final String dead = leader1;
            waitUntil(() -> { String l = pdLeader(rpc, Set.of(dead)); return l != null && !l.equals(dead); },
                    30000, "a new PD leader is elected after the old one died");
            String leader2 = pdLeader(rpc, Set.of(dead));
            System.out.println("  new PD leader elected from the surviving replicas: " + leader2);
            if (leader2 == null || leader2.equals(leader1)) throw new AssertionError("no new PD leader");

            waitUntil(() -> {
                Map<String, Set<String>> h = hostsOf(client);
                return !h.containsKey(dead) && allRf3(h, nShards);
            }, 45000, "new PD leader re-replicates the dead node's shards to restore RF");
            for (int i = 0; i < 12; i++) if (!("val" + i).equals(client.get("key" + i))) throw new AssertionError("data lost after PD failover");
            System.out.println("  RF restored to 3 on survivors; all 12 keys intact");

            System.out.println("\nPD FAILOVER OK (Java): the PD leader died; a surviving PD replica took over from the "
                    + "durable Raft log and finished healing the dead node's data — the control plane is no longer a SPOF.");
        } finally {
            for (Process p : procs.values()) p.destroyForcibly();
            for (Process p : procs.values()) p.waitFor();
        }
        System.exit(0);
    }
}
