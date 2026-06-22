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

/** Proves participant-crash recovery via Raft-replicated intents (Java mirror of
 * participant_recovery_smoke.py): a participant leader holding a prepared intent crashes, a new
 * leader inherits the intent (so isolation holds and the txn can still commit). */
public class ClusterParticipantRecoverySmoke {
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
                if ("leader".equals(sh.get("role")) && Boolean.TRUE.equals(sh.get("ready"))) {
                    m.put((String) sh.get("group"), (String) sh.get("node"));
                }
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
        ProcessBuilder pb = new ProcessBuilder(System.getProperty("java.home") + "/bin/java",
                "-cp", System.getProperty("java.class.path"), "com.litedb.cluster.NodeServer", nid);
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        return pb.start();
    }

    static String keyFor(Partitioner part, String shard) {
        for (int i = 0; i < 4000; i++) if (part.shardFor("pk" + i).equals(shard)) return "pk" + i;
        throw new AssertionError("no key for " + shard);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Path root = Paths.get(ClusterConfig.dataRoot());
        if (Files.exists(root)) Files.walk(root).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        Partitioner part = ClusterConfig.makePartitioner();
        Map<String, Process> procs = new LinkedHashMap<>();
        for (String nid : ClusterConfig.nodeIds()) procs.put(nid, spawn(nid));
        Rpc.Client rpc = new Rpc.Client(3000);
        try {
            ClusterClient client = new ClusterClient();
            waitUntil(() -> leaders(client).size() == 6, 20000, "6 leaders");
            Map<String, String> lm = leaders(client);

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
            String ka = keyFor(part, shardA), kb = keyFor(part, shardB);
            System.out.println("prepare on " + shardA + "@" + la + " (" + ka + ") and "
                    + shardB + "@" + lb + " (" + kb + "); will kill " + la);

            String txnId = "txn-participant-demo-1";
            long commitTs = client.begin();
            for (String[] p : new String[][]{{shardA, la, ka, "A=ok"}, {shardB, lb, kb, "B=ok"}}) {
                Map<String, Object> w = new LinkedHashMap<>();
                w.put(p[2], p[3]);
                Map<String, Object> r = rpc.call(HOST, ClusterConfig.port(p[1]), "shard_prepare",
                        NodeServer.resp("shard", p[0], "txn_id", txnId, "writes", w,
                                "read_ts", commitTs, "commit_ts", commitTs, "coordinator", "test"));
                Map<String, Object> res = (Map<String, Object>) r.get("result");
                if (!Boolean.TRUE.equals(r.get("ok")) || !Boolean.TRUE.equals(res.get("ok")))
                    throw new AssertionError("prepare " + p[0] + ": " + r);
            }
            System.out.println("both PREPARED (intents replicated through each shard's Raft group)");

            if (client.get(ka) != null || client.get(kb) != null) throw new AssertionError("should not be visible");
            Map<String, Object> c = client.put(ka, "conflict");
            if (Boolean.TRUE.equals(c.get("ok")) || !"locked".equals(c.get("error")))
                throw new AssertionError("expected locked, got " + c);
            System.out.println("  → key is locked by the intent (conflicting write rejected)");

            System.out.println("killing the participant leader " + la + " of " + shardA + "...");
            final String fla = la, fShardA = shardA;
            procs.get(la).destroyForcibly().waitFor();
            waitUntil(() -> { String l = leaders(client).get(fShardA); return l != null && !l.equals(fla); },
                    20000, "new leader for shardA");
            String newLeader = leaders(client).get(shardA);
            System.out.println("  → " + shardA + " re-elected: new leader is " + newLeader + " (≠ " + la + ")");

            Map<String, Object> c2 = client.put(ka, "conflict2");
            if (Boolean.TRUE.equals(c2.get("ok")) || !"locked".equals(c2.get("error")))
                throw new AssertionError("new leader lost the intent! isolation broken: " + c2);
            System.out.println("  → new leader still rejects the conflicting write — intent survived leadership change");

            for (String[] sk : new String[][]{{shardA, ka}, {shardB, kb}}) {
                String leader = leaders(client).get(sk[0]);
                Map<String, Object> r = rpc.call(HOST, ClusterConfig.port(leader), "shard_commit",
                        NodeServer.resp("shard", sk[0], "txn_id", txnId));
                Map<String, Object> res = (Map<String, Object>) r.get("result");
                if (!Boolean.TRUE.equals(res.get("ok"))) throw new AssertionError("commit " + sk[0] + ": " + r);
            }
            final String fka = ka, fkb = kb;
            waitUntil(() -> "A=ok".equals(client.get(fka)) && "B=ok".equals(client.get(fkb)),
                    20000, "commit on the inherited intent");
            System.out.println("  → committed on the new leader: " + ka + "=" + client.get(ka) + ", " + kb + "=" + client.get(kb));
            System.out.println("\nPARTICIPANT RECOVERY OK (Java): prepared intent survived a leader crash "
                    + "(replicated via Raft) — isolation preserved and the txn committed on the new leader.");
        } finally {
            for (Process p : procs.values()) p.destroyForcibly();
            for (Process p : procs.values()) p.waitFor();
        }
        System.exit(0);
    }
}
