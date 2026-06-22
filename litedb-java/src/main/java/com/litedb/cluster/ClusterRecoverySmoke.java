package com.litedb.cluster;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Proves 2PC coordinator-failure recovery (Java mirror of recovery_smoke.py). */
public class ClusterRecoverySmoke {
    static final String HOST = ClusterConfig.HOST;

    static Map<String, String> leaders(ClusterClient client) {
        Map<String, String> m = new TreeMap<>();
        for (Map<String, Object> node : client.status()) {
            if (!Boolean.TRUE.equals(node.get("alive"))) continue;
            @SuppressWarnings("unchecked")
            List<Object> shards = (List<Object>) node.get("shards");
            if (shards == null) continue;
            for (Object so : shards) {
                @SuppressWarnings("unchecked")
                Map<String, Object> sh = (Map<String, Object>) so;
                if ("leader".equals(sh.get("role")) && Boolean.TRUE.equals(sh.get("ready"))) m.put((String) sh.get("group"), (String) sh.get("node"));
            }
        }
        return m;
    }

    static void waitUntil(java.util.function.BooleanSupplier c, long ms, String what) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) { if (c.getAsBoolean()) return; Thread.sleep(200); }
        throw new AssertionError("timeout: " + what);
    }

    static Process spawn(String nid) throws Exception {
        String cp = System.getProperty("java.class.path");
        String javaBin = System.getProperty("java.home") + "/bin/java";
        ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", cp, "com.litedb.cluster.NodeServer", nid);
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        return pb.start();
    }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(ClusterConfig.dataRoot());
        if (Files.exists(root)) Files.walk(root).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        Partitioner part = ClusterConfig.makePartitioner();
        Map<String, Process> procs = new LinkedHashMap<>();
        for (String nid : ClusterConfig.INITIAL_NODES) procs.put(nid, spawn(nid));
        Rpc.Client rpc = new Rpc.Client(3000);
        try {
            ClusterClient client = new ClusterClient();
            waitUntil(() -> leaders(client).size() == 6, 20000, "6 leaders");
            Map<String, String> lm = leaders(client);

            // two shards with different leaders + a third coordinator node
            String shardA = null, la = null, shardB = null, lb = null;
            List<Map.Entry<String, String>> items = new ArrayList<>(lm.entrySet());
            outer:
            for (int i = 0; i < items.size(); i++)
                for (int j = i + 1; j < items.size(); j++)
                    if (!items.get(i).getValue().equals(items.get(j).getValue())) {
                        shardA = items.get(i).getKey(); la = items.get(i).getValue();
                        shardB = items.get(j).getKey(); lb = items.get(j).getValue();
                        break outer;
                    }
            String coord = null;
            for (String n : ClusterConfig.nodeIds()) if (!n.equals(la) && !n.equals(lb)) { coord = n; break; }
            System.out.println("participants: " + shardA + "@" + la + ", " + shardB + "@" + lb
                    + "; coordinator (will crash): " + coord);

            String ka = keyFor(part, shardA), kb = keyFor(part, shardB);
            String txnId = "txn-recovery-demo-1";
            long commitTs = client.begin();
            for (String[] p : new String[][]{{shardA, la, ka, "A=committed"}, {shardB, lb, kb, "B=committed"}}) {
                Map<String, Object> w = new LinkedHashMap<>();
                w.put(p[2], p[3]);
                Map<String, Object> r = rpc.call(HOST, ClusterConfig.port(p[1]), "shard_prepare",
                        NodeServer.resp("shard", p[0], "txn_id", txnId, "writes", w, "read_ts", commitTs, "commit_ts", commitTs));
                @SuppressWarnings("unchecked")
                Map<String, Object> res = (Map<String, Object>) r.get("result");
                if (!Boolean.TRUE.equals(r.get("ok")) || !Boolean.TRUE.equals(res.get("ok")))
                    throw new AssertionError("prepare " + p[0] + ": " + r);
            }
            System.out.println("both participants PREPARED (staged, holding locks, not committed)");
            if (client.get(ka) != null || client.get(kb) != null) throw new AssertionError("should not be visible yet");
            System.out.println("  → keys not yet visible (prepared only)");

            // coordinator decided COMMIT (fsync), then crashed before sending commits
            List<Object> participants = new ArrayList<>();
            participants.add(new ArrayList<>(List.of(la, shardA)));
            participants.add(new ArrayList<>(List.of(lb, shardB)));
            new TxnLog(ClusterConfig.nodeDataDir(coord)).write(txnId,
                    NodeServer.resp("txn_id", txnId, "status", "committing",
                            "participants", participants, "commit_ts", commitTs));
            System.out.println("dropped a 'committing' record into " + coord + "'s txn log, killing " + coord + "...");
            procs.get(coord).destroyForcibly().waitFor();

            System.out.println("restarting " + coord + " — its recovery sweep should drive the commits...");
            procs.put(coord, spawn(coord));

            final String fka = ka, fkb = kb;
            waitUntil(() -> "A=committed".equals(client.get(fka)) && "B=committed".equals(client.get(fkb)),
                    20000, "recovery to commit the in-doubt txn");
            System.out.println("  → after restart, both keys committed: " + ka + "=" + client.get(ka)
                    + ", " + kb + "=" + client.get(kb));
            System.out.println("\nRECOVERY OK (Java): coordinator died after deciding COMMIT, restarted, finished the 2PC.");
        } finally {
            for (Process p : procs.values()) p.destroyForcibly();
            for (Process p : procs.values()) p.waitFor();
        }
        System.exit(0);
    }

    static String keyFor(Partitioner part, String shard) {
        for (int i = 0; i < 4000; i++) if (part.shardFor("rk" + i).equals(shard)) return "rk" + i;
        throw new AssertionError("no key for " + shard);
    }
}
