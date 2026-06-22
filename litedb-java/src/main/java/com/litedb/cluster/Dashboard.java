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
    private final java.util.List<String> ctrlLog = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final Controller controller =
            new Controller(ClusterConfig.INITIAL_NODES, m -> { ctrlLog.add(m); if (ctrlLog.size() > 50) ctrlLog.remove(0); });

    void startAll() throws Exception {
        Path root = Paths.get(ClusterConfig.dataRoot());
        if (Files.exists(root)) {
            Files.walk(root).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
        for (String nid : ClusterConfig.INITIAL_NODES) startNode(nid);
        Thread.sleep(2500);
        controller.broadcastPlacement();
    }

    void addNode() throws IOException {
        String nxt = null;
        for (String n : ClusterConfig.nodeIds()) if (!controller.active().contains(n)) { nxt = n; break; }
        if (nxt == null) return;
        startNode(nxt);
        final String node = nxt;
        new Thread(() -> { try { Thread.sleep(1500); } catch (InterruptedException ignored) {} controller.addNode(node); }).start();
    }

    void removeNode() {
        if (controller.active().size() <= ClusterConfig.INITIAL_NODES.size()) return;
        String victim = controller.active().get(controller.active().size() - 1);
        new Thread(() -> { controller.removeNode(victim, false); killNode(victim); }).start();
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
        List<String> active = controller.active();
        Map<String, Object> live = new LinkedHashMap<>();
        for (Map<String, Object> st : client.status()) live.put((String) st.get("node"), st);

        List<Object> placement = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : controller.placement().entrySet()) {
            String shard = e.getKey();
            Map<String, Object> hosts = new LinkedHashMap<>();
            String leader = null;
            for (String node : e.getValue()) {
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
            placement.add(NodeServer.resp("shard", shard,
                    "preferred", e.getValue().isEmpty() ? null : e.getValue().get(0),
                    "replicas", new ArrayList<>(e.getValue()), "leader", leader, "hosts", hosts));
        }
        placement.sort(java.util.Comparator.comparing(o -> (String) ((Map<String, Object>) o).get("shard")));

        int up = 0;
        for (String n : active) if (Boolean.TRUE.equals(((Map<String, Object>) live.getOrDefault(n, Map.of())).get("alive"))) up++;
        int withLeader = 0;
        List<Object> under = new ArrayList<>();
        int rf = Math.min(part.rf(), active.size());
        for (Object o : placement) {
            Map<String, Object> pp = (Map<String, Object>) o;
            if (pp.get("leader") != null) withLeader++;
            int liveReps = 0;
            for (Object r : ((Map<String, Object>) pp.get("hosts")).values()) if (r != null) liveReps++;
            if (liveReps < rf) under.add(pp.get("shard"));
        }
        List<Object> nodesCfg = new ArrayList<>();
        for (String n : active) {
            nodesCfg.add(NodeServer.resp("id", n, "host", ClusterConfig.HOST, "port", (long) ClusterConfig.port(n)));
        }
        boolean canAdd = false;
        for (String n : ClusterConfig.nodeIds()) if (!active.contains(n)) { canAdd = true; break; }
        Map<String, Object> config = NodeServer.resp("active", new ArrayList<>(active), "nodes", nodesCfg,
                "shards", new ArrayList<Object>(part.shardIds()), "rf", (long) part.rf(),
                "ring_size", Partitioner.RING_SIZE, "can_add", canAdd,
                "can_remove", active.size() > ClusterConfig.INITIAL_NODES.size());
        Map<String, Object> health = NodeServer.resp("up", (long) up, "total", (long) active.size(),
                "with_leader", (long) withLeader, "total_shards", (long) part.shardIds().size(),
                "under_replicated", under);
        return NodeServer.resp("config", config, "live", live, "placement", placement,
                "ring", part.ringArcs(), "control_log", new ArrayList<>(ctrlLog), "health", health);
    }

    // ---- HTTP -------------------------------------------------------------

    private void serve() throws IOException {
        HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", ClusterConfig.DASHBOARD_PORT), 0);
        String page = PAGE.replace("%NODES%", Json.encode(new ArrayList<Object>(ClusterConfig.INITIAL_NODES)))
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
                    String a = (String) b.get("action");
                    if ("kill".equals(a)) killNode((String) b.get("node"));
                    else if ("start".equals(a)) startNode((String) b.get("node"));
                    else if ("add_node".equals(a)) addNode();
                    else if ("remove_node".equals(a)) removeNode();
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
