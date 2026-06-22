package com.litedb.cluster;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Controller — the cluster's control plane (a simplified TiKV Placement Driver). Holds the
 * authoritative shard→node placement and orchestrates membership changes when a node is added or
 * removed: it computes an even target placement, diffs it against the current one, and applies the
 * moves one Raft membership change at a time (add a follower → it catches up via Raft → drop the old
 * replica). Single-server-at-a-time keeps every group safe. Mirror of controller.py.
 */
public final class Controller {
    private final Rpc.Client rpc = new Rpc.Client(3000);
    private final List<String> active;
    private Map<String, List<String>> placement;
    private final Consumer<String> emit;

    public Controller(List<String> active, Consumer<String> onEvent) {
        this.active = new ArrayList<>(active);
        this.placement = ClusterConfig.computePlacement(this.active);
        this.emit = onEvent != null ? onEvent : m -> {};
    }

    public List<String> active() { return active; }
    public Map<String, List<String>> placement() { return placement; }

    @SuppressWarnings("unchecked")
    private Map<String, Object> call(String node, String method, Map<String, Object> payload) {
        Map<String, Object> r = rpc.call(ClusterConfig.HOST, ClusterConfig.port(node), method, payload);
        return Boolean.TRUE.equals(r.get("ok")) ? (Map<String, Object>) r.get("result")
                : NodeServer.resp("ok", false, "error", r.get("error"));
    }

    private String leaderOf(String shard) {
        for (int i = 0; i < 40; i++) {
            for (String n : placement.getOrDefault(shard, List.of())) {
                if (!active.contains(n)) continue;
                Map<String, Object> res = call(n, "shard_leader", NodeServer.resp("shard", shard));
                if (Boolean.TRUE.equals(res.get("ok")) && res.get("leader") != null) {
                    return (String) res.get("leader");
                }
            }
            sleep(100);
        }
        return null;
    }

    public void broadcastPlacement() {
        for (String n : active) call(n, "update_placement", NodeServer.resp("placement", placement));
    }

    public void addNode(String newNode) {
        emit.accept("ADD node " + newNode + ": rebalancing shards onto it");
        if (!active.contains(newNode)) active.add(newNode);
        rebalance(ClusterConfig.computePlacement(active), null, false);
        emit.accept("ADD node " + newNode + ": done");
    }

    public void removeNode(String node, boolean dead) {
        emit.accept("REMOVE node " + node + " (dead=" + dead + "): re-replicating to restore RF");
        active.remove(node);
        rebalance(ClusterConfig.computePlacement(active), node, dead);
        emit.accept("REMOVE node " + node + ": done");
    }

    private void rebalance(Map<String, List<String>> target, String departing, boolean dead) {
        for (String shard : ClusterConfig.SHARDS) {
            Set<String> cur = new LinkedHashSet<>(placement.getOrDefault(shard, List.of()));
            Set<String> want = new LinkedHashSet<>(target.getOrDefault(shard, List.of()));
            for (String n : sortedDiff(want, cur)) addReplica(shard, n);
            for (String n : sortedDiff(cur, want)) removeReplica(shard, n, dead && n.equals(departing));
        }
        this.placement = target;
        broadcastPlacement();
    }

    private void addReplica(String shard, String node) {
        List<String> cur = placement.getOrDefault(shard, List.of());
        call(node, "host_shard", NodeServer.resp("shard", shard, "voters", new ArrayList<>(cur)));
        String leader = leaderOf(shard);
        if (leader == null) { emit.accept("  " + shard + ": no leader; cannot add " + node); return; }
        Set<String> nv = new LinkedHashSet<>(cur);
        nv.add(node);
        call(leader, "reconfigure", NodeServer.resp("shard", shard, "voters", new ArrayList<>(nv)));
        placement.put(shard, new ArrayList<>(nv));
        emit.accept("  " + shard + (waitVoter(shard, node) ? ": +" + node + " (data caught up via Raft)"
                : ": +" + node + " (still catching up)"));
    }

    private void removeReplica(String shard, String node, boolean dead) {
        Set<String> nv = new LinkedHashSet<>(placement.getOrDefault(shard, List.of()));
        nv.remove(node);
        if (nv.isEmpty()) return;
        String leader = leaderOf(shard);
        if (leader != null) {
            call(leader, "reconfigure", NodeServer.resp("shard", shard, "voters", new ArrayList<>(nv)));
        }
        placement.put(shard, new ArrayList<>(nv));
        if (!dead) call(node, "drop_shard", NodeServer.resp("shard", shard));
        emit.accept("  " + shard + ": -" + node);
    }

    @SuppressWarnings("unchecked")
    private boolean waitVoter(String shard, String node) {
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> res = call(node, "status", NodeServer.resp());
            if (Boolean.TRUE.equals(res.get("ok"))) {
                for (Object so : (List<Object>) res.get("shards")) {
                    Map<String, Object> sh = (Map<String, Object>) so;
                    if (shard.equals(sh.get("group")) && ((List<Object>) sh.get("voters")).contains(node)) {
                        return true;
                    }
                }
            }
            sleep(150);
        }
        return false;
    }

    private static List<String> sortedDiff(Set<String> a, Set<String> b) {
        List<String> out = new ArrayList<>();
        for (String x : a) if (!b.contains(x)) out.add(x);
        out.sort(String::compareTo);
        return out;
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}
