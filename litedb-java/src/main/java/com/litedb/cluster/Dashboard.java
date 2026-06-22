package com.litedb.cluster;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Dashboard — launcher + live web dashboard for the Java distributed cluster (mirror of the Python
 * dashboard). Spawns one OS process per node, then serves a UI showing the whole system: health,
 * config, the consistent-hash ring, the shard→node placement matrix, one event feed per instance,
 * and a merged system stream. Controls write keys, run a cross-shard transaction, and kill/restart
 * nodes to watch failover.
 *
 * Run:  java com.litedb.cluster.Dashboard      then open  http://127.0.0.1:7180
 */
public final class Dashboard {

    private final Map<String, Process> procs = new LinkedHashMap<>();
    private final ClusterClient client = new ClusterClient();
    private final Rpc.Client rpc = new Rpc.Client(1500);
    private final Partitioner part = ClusterConfig.makePartitioner();

    void startAll() throws Exception {
        Path root = Paths.get(ClusterConfig.dataRoot());
        if (Files.exists(root)) {
            Files.walk(root).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
        for (String nid : ClusterConfig.nodeIds()) startNode(nid);
    }

    void startNode(String nid) throws IOException {
        Process p = procs.get(nid);
        if (p != null && p.isAlive()) return;
        String cp = System.getProperty("java.class.path");
        String javaBin = System.getProperty("java.home") + "/bin/java";
        ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", cp, "com.litedb.cluster.NodeServer", nid);
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        procs.put(nid, pb.start());
    }

    void killNode(String nid) {
        Process p = procs.get(nid);
        if (p != null && p.isAlive()) {
            p.destroyForcibly();
            try { p.waitFor(); } catch (InterruptedException ignored) {}
        }
    }

    void stopAll() {
        for (Process p : procs.values()) p.destroyForcibly();
    }

    Map<String, Object> nodeEvents(String nid, long after) {
        Map<String, Object> r = rpc.call(ClusterConfig.HOST, ClusterConfig.port(nid), "events",
                NodeServer.resp("after", after));
        if (Boolean.TRUE.equals(r.get("ok"))) {
            @SuppressWarnings("unchecked")
            Map<String, Object> res = (Map<String, Object>) r.get("result");
            return res;
        }
        return NodeServer.resp("events", new ArrayList<>(), "next", after, "down", true);
    }

    List<String> crossShardPair() {
        String base = "acct:alice";
        String s0 = part.shardFor(base);
        for (int i = 0; i < 400; i++) {
            String c = "acct:user" + i;
            if (!part.shardFor(c).equals(s0)) return List.of(base, c);
        }
        return List.of(base, "acct:bob");
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> overview() {
        List<Map<String, Object>> statuses = client.status();
        Map<String, Object> live = new LinkedHashMap<>();
        for (Map<String, Object> st : statuses) live.put((String) st.get("node"), st);

        List<Object> placement = new ArrayList<>();
        for (Map<String, Object> p : part.placementList()) {
            String shard = (String) p.get("shard");
            Map<String, Object> hosts = new LinkedHashMap<>();
            String leader = null;
            for (Object rn : (List<Object>) p.get("replicas")) {
                String node = (String) rn;
                String role = null;
                Map<String, Object> st = (Map<String, Object>) live.get(node);
                if (st != null && st.get("shards") != null) {
                    for (Object so : (List<Object>) st.get("shards")) {
                        Map<String, Object> sh = (Map<String, Object>) so;
                        if (shard.equals(sh.get("group"))) {
                            role = (String) sh.get("role");
                            if ("leader".equals(role)) leader = node;
                        }
                    }
                }
                hosts.put(node, role);
            }
            placement.add(NodeServer.resp("shard", shard, "preferred", p.get("preferred"),
                    "replicas", p.get("replicas"), "leader", leader, "hosts", hosts));
        }

        int up = 0;
        for (Map<String, Object> st : statuses) if (Boolean.TRUE.equals(st.get("alive"))) up++;
        int withLeader = 0;
        List<Object> under = new ArrayList<>();
        for (Object o : placement) {
            Map<String, Object> pp = (Map<String, Object>) o;
            if (pp.get("leader") != null) withLeader++;
            int liveReps = 0;
            for (Object r : ((Map<String, Object>) pp.get("hosts")).values()) if (r != null) liveReps++;
            if (liveReps < part.rf()) under.add(pp.get("shard"));
        }
        List<Object> nodesCfg = new ArrayList<>();
        for (String n : ClusterConfig.nodeIds()) {
            nodesCfg.add(NodeServer.resp("id", n, "host", ClusterConfig.HOST, "port", (long) ClusterConfig.port(n)));
        }
        Map<String, Object> config = NodeServer.resp("nodes", nodesCfg,
                "shards", new ArrayList<Object>(part.shardIds()), "rf", (long) part.rf(),
                "ring_size", Partitioner.RING_SIZE);
        Map<String, Object> health = NodeServer.resp("up", (long) up, "total", (long) ClusterConfig.nodeIds().size(),
                "with_leader", (long) withLeader, "total_shards", (long) part.shardIds().size(),
                "under_replicated", under);
        return NodeServer.resp("config", config, "live", live, "placement", placement,
                "ring", part.ringArcs(), "health", health);
    }

    // ---- HTTP -------------------------------------------------------------

    private void serve() throws IOException {
        HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", ClusterConfig.DASHBOARD_PORT), 0);
        String page = PAGE.replace("%NODES%", Json.encode(new ArrayList<Object>(ClusterConfig.nodeIds())))
                          .replace("%SHARDS%", Json.encode(new ArrayList<Object>(ClusterConfig.SHARDS)));
        http.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath();
            try {
                if (path.equals("/")) {
                    sendHtml(ex, page);
                } else if (path.equals("/api/overview")) {
                    sendJson(ex, overview());
                } else if (path.equals("/api/events")) {
                    Map<String, String> q = query(ex);
                    sendJson(ex, nodeEvents(q.getOrDefault("node", ""), Long.parseLong(q.getOrDefault("after", "0"))));
                } else if (path.equals("/api/put")) {
                    Map<String, Object> b = body(ex);
                    sendJson(ex, client.put((String) b.get("key"), (String) b.get("value")));
                } else if (path.equals("/api/get")) {
                    sendJson(ex, client.getFull((String) body(ex).get("key")));
                } else if (path.equals("/api/txn")) {
                    List<String> pair = crossShardPair();
                    Map<String, Object> w = NodeServer.resp(pair.get(0), "balance=900", pair.get(1), "balance=1100");
                    sendJson(ex, client.txn(w, null));
                } else if (path.equals("/api/control")) {
                    Map<String, Object> b = body(ex);
                    if ("kill".equals(b.get("action"))) killNode((String) b.get("node"));
                    else if ("start".equals(b.get("action"))) startNode((String) b.get("node"));
                    sendJson(ex, NodeServer.resp("ok", true));
                } else {
                    sendJson(ex, NodeServer.resp("error", "not found"));
                }
            } catch (Exception e) {
                sendJson(ex, NodeServer.resp("ok", false, "error", String.valueOf(e.getMessage())));
            }
        });
        http.setExecutor(Executors.newFixedThreadPool(8));
        http.start();
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> m = new LinkedHashMap<>();
        String q = ex.getRequestURI().getQuery();
        if (q != null) for (String kv : q.split("&")) {
            String[] p = kv.split("=", 2);
            if (p.length == 2) m.put(p[0], p[1]);
        }
        return m;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> body(HttpExchange ex) throws IOException {
        byte[] b = ex.getRequestBody().readAllBytes();
        if (b.length == 0) return new LinkedHashMap<>();
        return (Map<String, Object>) Json.parse(new String(b, StandardCharsets.UTF_8));
    }

    private static void sendJson(HttpExchange ex, Object obj) throws IOException {
        byte[] body = Json.encode(obj).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(200, body.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(body); }
    }

    private static void sendHtml(HttpExchange ex, String html) throws IOException {
        byte[] body = html.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
        ex.sendResponseHeaders(200, body.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(body); }
    }

    public static void main(String[] args) throws Exception {
        Dashboard d = new Dashboard();
        d.startAll();
        d.serve();
        System.out.println("\n  litedb cluster up — " + ClusterConfig.nodeIds().size() + " instances, "
                + ClusterConfig.SHARDS.size() + " shards, RF " + ClusterConfig.replicationFactor());
        System.out.println("  dashboard:  http://127.0.0.1:" + ClusterConfig.DASHBOARD_PORT + "\n");
        Runtime.getRuntime().addShutdownHook(new Thread(d::stopAll));
        Thread.currentThread().join();
    }

    // The HTML/JS UI — identical in spirit to the Python dashboard.
    static final String PAGE = DashboardPage.HTML;
}
