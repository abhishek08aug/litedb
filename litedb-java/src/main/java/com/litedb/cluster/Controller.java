package com.litedb.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Controller — a thin CLIENT of the Placement Driver Raft group (mirror of controller.py).
 *
 * The control plane is no longer a single in-process orchestrator. The authority lives in a
 * replicated PD Raft group co-located on {@code PD_NODES} (see {@link Pd}): membership decisions are
 * committed to its log and the PD leader runs reconcile + failure detection. This class just submits
 * decisions to the PD leader and reads the authoritative state back, so a dashboard process can come
 * and go freely — the durable decisions and the healing live in the fault-tolerant PD group.
 */
public final class Controller {
    private final Rpc.Client rpc = new Rpc.Client(3000);
    private final Consumer<String> emit;
    private volatile List<String> activeView;
    private volatile Map<String, List<String>> placementView;
    private volatile boolean running = true;

    public Controller(List<String> active, Consumer<String> onEvent) {
        this.emit = onEvent != null ? onEvent : m -> {};
        this.activeView = new ArrayList<>(active);
        this.placementView = ClusterConfig.computePlacement(this.activeView);
        Thread t = new Thread(this::refreshLoop, "controller-refresh");
        t.setDaemon(true);
        t.start();
    }

    public List<String> active() { return activeView; }
    public Map<String, List<String>> placement() { return placementView; }

    @SuppressWarnings("unchecked")
    private Map<String, Object> pdStatus() {
        for (String n : ClusterConfig.PD_NODES) {
            Map<String, Object> r = rpc.call(ClusterConfig.HOST, ClusterConfig.port(n), "pd_status", NodeServer.resp());
            if (Boolean.TRUE.equals(r.get("ok")) && r.get("result") instanceof Map) {
                Map<String, Object> res = (Map<String, Object>) r.get("result");
                if (Boolean.TRUE.equals(res.get("ok"))) return res;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private void refreshLoop() {
        while (running) {
            Map<String, Object> st = pdStatus();
            if (st != null) {
                activeView = (List<String>) st.get("active");
                placementView = (Map<String, List<String>>) st.get("placement");
            }
            sleep(1000);
        }
    }

    public void stop() { running = false; }

    @SuppressWarnings("unchecked")
    private boolean propose(Map<String, Object> decision) {
        for (int i = 0; i < 60; i++) {
            Map<String, Object> st = pdStatus();
            String target = st != null && st.get("leader") != null
                    ? (String) st.get("leader") : ClusterConfig.PD_NODES.get(0);
            Map<String, Object> r = rpc.call(ClusterConfig.HOST, ClusterConfig.port(target),
                    "pd_propose", NodeServer.resp("decision", decision));
            if (Boolean.TRUE.equals(r.get("ok")) && r.get("result") instanceof Map
                    && Boolean.TRUE.equals(((Map<String, Object>) r.get("result")).get("ok"))) {
                return true;
            }
            sleep(200);
        }
        return false;
    }

    public void addNode(String newNode) {
        emit.accept("ADD node " + newNode + ": proposing add_node to the PD Raft group");
        boolean ok = propose(NodeServer.resp("op", "add_node", "node", newNode));
        emit.accept("ADD node " + newNode + ": "
                + (ok ? "committed — PD is reconciling placement" : "FAILED (no PD leader)"));
    }

    public void removeNode(String node, boolean dead) {
        emit.accept("REMOVE node " + node + " (dead=" + dead + "): proposing remove_node to the PD Raft group");
        boolean ok = propose(NodeServer.resp("op", "remove_node", "node", node, "dead", dead));
        emit.accept("REMOVE node " + node + ": "
                + (ok ? "committed — PD is re-replicating to restore RF" : "FAILED (no PD leader)"));
    }

    // kept for call-site compatibility — the failure detector now runs inside the PD leader, and the
    // PD owns placement (nothing to broadcast from here).
    public void startFailureDetector(long intervalMs, long reapAfterMs) {}
    public void startFailureDetector() {}
    public void broadcastPlacement() {}

    private static void sleep(long ms) { try { Thread.sleep(ms); } catch (InterruptedException ignored) {} }
}
