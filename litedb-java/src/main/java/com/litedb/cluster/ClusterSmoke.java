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

/** Headless end-to-end test: 3 real NodeServer processes driven by ClusterClient. RF-aware. */
public class ClusterSmoke {

    static Map<String, String> leadersMap(ClusterClient client) {
        Map<String, String> m = new TreeMap<>();
        for (Map<String, Object> node : client.status()) {
            if (!Boolean.TRUE.equals(node.get("alive"))) continue;
            @SuppressWarnings("unchecked")
            List<Object> shards = (List<Object>) node.get("shards");
            for (Object so : shards) {
                @SuppressWarnings("unchecked")
                Map<String, Object> sh = (Map<String, Object>) so;
                if ("leader".equals(sh.get("role"))) m.put((String) sh.get("group"), (String) sh.get("node"));
            }
        }
        return m;
    }

    static void waitUntil(java.util.function.BooleanSupplier cond, long timeoutMs, String what) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(200);
        }
        throw new AssertionError("timeout waiting for " + what);
    }

    static void rmrf(String dir) throws Exception {
        Path p = Paths.get(dir);
        if (!Files.exists(p)) return;
        Files.walk(p).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    public static void main(String[] args) throws Exception {
        int rf = ClusterConfig.replicationFactor();
        Partitioner part = ClusterConfig.makePartitioner();
        rmrf(ClusterConfig.dataRoot());

        String cp = System.getProperty("java.class.path");
        String javaBin = System.getProperty("java.home") + "/bin/java";
        Map<String, Process> procs = new LinkedHashMap<>();
        for (String nid : ClusterConfig.nodeIds()) {
            ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", cp, "com.litedb.cluster.NodeServer", nid);
            pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            pb.redirectError(ProcessBuilder.Redirect.DISCARD);
            procs.put(nid, pb.start());
        }
        try {
            ClusterClient client = new ClusterClient();
            System.out.println("Replication factor = " + rf + " (each shard on " + rf + " of "
                    + part.nodes().size() + " nodes)");
            waitUntil(() -> leadersMap(client).size() == 6, 20000, "6 leaders");
            Map<String, String> lm = leadersMap(client);
            Map<String, Integer> spread = new TreeMap<>();
            for (String n : lm.values()) spread.merge(n, 1, Integer::sum);
            System.out.println("  leaders per node: " + spread + "  (multi-raft spreads leadership)");

            Map<String, Integer> hosted = new TreeMap<>();
            int maxHosted = 0;
            for (String n : part.nodes()) { int c = part.shardsOn(n).size(); hosted.put(n, c); maxHosted = Math.max(maxHosted, c); }
            System.out.println("  shards hosted per node: " + hosted);
            if (rf < part.nodes().size() && maxHosted >= part.shardIds().size())
                throw new AssertionError("RF<N should leave some shard off some node");

            System.out.println("Writing 12 keys (routed by consistent hashing)...");
            for (int i = 0; i < 12; i++) {
                if (!Boolean.TRUE.equals(client.put("key" + i, "val" + i).get("ok")))
                    throw new AssertionError("put key" + i);
            }
            boolean allRead = true;
            for (int i = 0; i < 12; i++) allRead &= ("val" + i).equals(client.get("key" + i));
            System.out.println("  all 12 reads correct -> " + allRead);
            if (!allRead) throw new AssertionError("reads");

            // cross-shard txn
            String a = "key0", b = null;
            for (int i = 1; i < 12; i++) if (!part.shardFor("key" + i).equals(part.shardFor("key0"))) { b = "key" + i; break; }
            Map<String, Object> w = new LinkedHashMap<>();
            w.put(a, "alice=900"); w.put(b, "bob=100");
            Map<String, Object> tr = client.txn(w, null);
            System.out.println("  cross-shard 2PC -> ok=" + tr.get("ok") + " shards=" + tr.get("shards"));
            if (!Boolean.TRUE.equals(tr.get("ok"))) throw new AssertionError("2pc");
            if (!"alice=900".equals(client.get(a)) || !"bob=100".equals(client.get(b))) throw new AssertionError("2pc data");

            // snapshot isolation
            Long snap = client.begin();
            client.put(a, "alice=CHANGED");
            String old = client.get(a, snap), now = client.get(a);
            System.out.println("  snapshot=" + old + "  latest=" + now);
            if (!"alice=900".equals(old) || !"alice=CHANGED".equals(now)) throw new AssertionError("snapshot isolation");

            if (rf >= 3) {
                String victim = null; int best = -1;
                for (Map.Entry<String, Integer> e : spread.entrySet()) if (e.getValue() > best) { best = e.getValue(); victim = e.getKey(); }
                System.out.println("Killing " + victim + " (led " + best + " shards)...");
                procs.get(victim).destroyForcibly().waitFor();
                final String dead = victim;
                waitUntil(() -> { Map<String, String> m = leadersMap(client); return m.size() == 6 && !m.containsValue(dead); }, 20000, "failover");
                System.out.println("  re-elected onto survivors");
                if (!Boolean.TRUE.equals(client.put("after-failover", "ok").get("ok"))) throw new AssertionError("post-failover write");
                if (!"alice=CHANGED".equals(client.get(a))) throw new AssertionError("data lost");
                System.out.println("  write + read after failover OK; pre-crash data intact");
            } else {
                System.out.println("(RF=" + rf + " < 3 → single node loss removes a shard's majority; failover not exercised.)");
            }
            System.out.println("\nCLUSTER OK at RF=" + rf + " (Java): partitioning, multi-raft, RF-agnostic routing, "
                    + "cross-shard 2PC, snapshot isolation" + (rf >= 3 ? ", live failover." : "."));
        } finally {
            for (Process p : procs.values()) p.destroyForcibly();
            for (Process p : procs.values()) p.waitFor();
        }
        System.exit(0);
    }
}
