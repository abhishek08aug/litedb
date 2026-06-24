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

/**
 * RejoinSmoke — proves a reaped node that REJOINS doesn't disrupt the cluster (the Pre-Vote fix;
 * mirror of rejoin_smoke.py). Kill the PD leader (a node), let auto-heal reap it, then RESTART and
 * re-add it. The returning node still has stale shard replicas on disk; WITHOUT pre-vote they keep
 * forcing higher-term elections and leave shards leaderless forever. WITH pre-vote, peers refuse the
 * stale replica's pre-vote while hearing from their leader, so the cluster converges back to fully
 * HEALTHY and stays there.
 *
 * Run with 4 nodes:  LITEDB_CLUSTER_NODES=4 java com.litedb.cluster.RejoinSmoke
 */
public class RejoinSmoke {

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
    static Set<String> readyLeaders(ClusterClient client) {
        Set<String> out = new java.util.HashSet<>();
        for (Map<String, Object> st : client.status()) {
            if (st.get("shards") == null) continue;
            for (Object so : (List<Object>) st.get("shards")) {
                Map<String, Object> sh = (Map<String, Object>) so;
                if ("leader".equals(sh.get("role")) && Boolean.TRUE.equals(sh.get("ready"))) {
                    out.add((String) sh.get("group"));
                }
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    static String pdLeader(Rpc.Client rpc) {
        for (String n : ClusterConfig.PD_NODES) {
            Map<String, Object> r = rpc.call(ClusterConfig.HOST, ClusterConfig.port(n), "pd_status", NodeServer.resp());
            if (Boolean.TRUE.equals(r.get("ok")) && r.get("result") instanceof Map) {
                Map<String, Object> res = (Map<String, Object>) r.get("result");
                if (res.get("leader") != null) return (String) res.get("leader");
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    static Set<String> hostedBy(ClusterClient client, String node) {
        Set<String> out = new java.util.HashSet<>();
        for (Map<String, Object> st : client.status()) {
            if (!node.equals(st.get("node")) || st.get("shards") == null) continue;
            for (Object so : (List<Object>) st.get("shards")) out.add((String) ((Map<String, Object>) so).get("group"));
        }
        return out;
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
        Rpc.Client rpc = new Rpc.Client(1500);
        try {
            for (String n : ClusterConfig.INITIAL_NODES) spawn(n);
            ClusterClient client = new ClusterClient();
            waitUntil(() -> readyLeaders(client).size() == nShards, 30000, "all shards have a leader at start");
            for (int i = 0; i < 12; i++) if (!Boolean.TRUE.equals(client.put("key" + i, "val" + i).get("ok"))) throw new AssertionError("put key" + i);
            String victim = pdLeader(rpc);
            if (victim == null) victim = "node-1";
            System.out.println("cluster healthy; PD leader = " + victim + ". Killing it (a node + the PD leader)...");

            final String dead = victim;
            procs.get(dead).destroyForcibly();
            procs.get(dead).waitFor();
            waitUntil(() -> !ctrl.active().contains(dead) && readyLeaders(client).size() == nShards,
                    70000, "auto-heal after the PD leader died (reaped + RF restored)");
            System.out.println("  auto-healed: " + dead + " reaped, all " + nShards + " shards led on " + ctrl.active());

            System.out.println("restarting " + dead + " and re-adding it (it still has stale shard replicas on disk)...");
            spawn(dead);
            Thread.sleep(2000);
            ctrl.addNode(dead);

            // THE FIX: the rejoining node must CONVERGE to a stable, fully-led cluster. Reconfiguration
            // churn can briefly dip a shard's leader, so we require SUSTAINED health (5 consecutive
            // seconds). Without pre-vote the stale replicas disrupt elections and this never converges.
            int streak = 0;
            long deadline = System.currentTimeMillis() + 90000;
            while (System.currentTimeMillis() < deadline) {
                streak = readyLeaders(client).size() == nShards ? streak + 1 : 0;
                if (streak >= 5) break;
                Thread.sleep(1000);
            }
            if (streak < 5) throw new AssertionError("cluster never stabilized after rejoin — a returning node is disrupting elections");
            for (int i = 0; i < 12; i++) if (!("val" + i).equals(client.get("key" + i))) throw new AssertionError("data lost after rejoin");

            // FENCING: the rejoined node must host only shards it's a voter of — its orphaned stale
            // replicas (shards it was reaped from) must have been dropped + wiped, not left lingering.
            Thread.sleep(6000);
            Map<String, List<String>> placement = ctrl.placement();
            Set<String> hosted = hostedBy(client, dead);
            for (String s : hosted) {
                if (!placement.getOrDefault(s, List.of()).contains(dead)) {
                    throw new AssertionError(dead + " still hosts orphan replica of " + s + " it isn't a voter of");
                }
            }
            System.out.println("  " + dead + " hosts only shards it's a voter of " + hosted + " — orphans fenced + wiped");

            System.out.println("\nREJOIN OK (Java): " + dead + " restarted + re-added with stale replicas did NOT "
                    + "disrupt the cluster (pre-vote refused its elections) — all " + nShards + " shards keep a "
                    + "stable leader, data intact.");
        } finally {
            ctrl.stop();
            for (Process p : procs.values()) p.destroyForcibly();
            for (Process p : procs.values()) p.waitFor();
        }
        System.exit(0);
    }
}
