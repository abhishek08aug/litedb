package com.litedb.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
 *
 * It also runs an opt-in gossip-driven failure detector ({@link #startFailureDetector}): a reconcile
 * loop that reads gossip liveness via node {@code status} and, when a node is reported DEAD by a
 * majority of its live peers past a grace window, auto-fires {@code removeNode(dead=true)} to restore
 * RF — closing the loop from "gossip detected a death" to "redundancy healed" with no human action.
 */
public final class Controller {
    private final Rpc.Client rpc = new Rpc.Client(3000);
    private final List<String> active;
    private Map<String, List<String>> placement;
    private final Consumer<String> emit;
    // membership ops (add/remove/auto-reap) serialize on this so the failure detector and the UI
    // never run two rebalances at once.
    private final Object lock = new Object();
    private volatile boolean fdRunning = false;
    private long fdIntervalMs = 2000;
    private long reapAfterMs = 5000;
    private final Map<String, Long> deadSince = new LinkedHashMap<>();

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
        synchronized (lock) {
            emit.accept("ADD node " + newNode + ": rebalancing shards onto it");
            if (!active.contains(newNode)) active.add(newNode);
            rebalance(ClusterConfig.computePlacement(active), null, false);
            emit.accept("ADD node " + newNode + ": done");
        }
    }

    public void removeNode(String node, boolean dead) {
        synchronized (lock) {
            emit.accept("REMOVE node " + node + " (dead=" + dead + "): re-replicating to restore RF");
            active.remove(node);
            rebalance(ClusterConfig.computePlacement(active), node, dead);
            emit.accept("REMOVE node " + node + ": done");
        }
    }

    // ---- gossip-driven auto-heal (failure detector) -----------------------

    /** Reconcile loop: read gossip liveness via node {@code status}; a node reported DEAD by a
     * majority of its live peers, and held dead past {@code reapAfterMs}, is reaped via
     * removeNode(dead=true) to restore RF — no human action. The grace window lets a quick
     * restart/deploy avoid an expensive re-replication; the majority rule + alive-majority guard
     * prevent acting on one node's false suspicion or a partitioned minority. */
    public void startFailureDetector(long intervalMs, long reapAfterMs) {
        this.fdIntervalMs = intervalMs;
        this.reapAfterMs = reapAfterMs;
        this.fdRunning = true;
        Thread t = new Thread(this::fdLoop, "controller-failure-detector");
        t.setDaemon(true);
        t.start();
    }

    public void startFailureDetector() { startFailureDetector(2000, 5000); }

    public void stop() { fdRunning = false; }

    private void fdLoop() {
        while (fdRunning) {
            sleep(fdIntervalMs);
            try {
                reconcileOnce();
            } catch (RuntimeException e) {  // never let the heal loop die
                emit.accept("failure-detector error: " + e.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void reconcileOnce() {
        List<String> snapshot;
        synchronized (lock) { snapshot = new ArrayList<>(active); }
        if (snapshot.size() <= 1) return;
        // Gather each live node's gossip view (status carries it). A dead node won't respond.
        Map<String, Map<String, Object>> views = new LinkedHashMap<>();
        for (String n : snapshot) {
            Map<String, Object> res = call(n, "status", NodeServer.resp());
            if (Boolean.TRUE.equals(res.get("ok")) && res.get("members") instanceof Map) {
                views.put(n, (Map<String, Object>) res.get("members"));
            }
        }
        // Only act while a MAJORITY is alive — otherwise a Raft config change can't commit anyway
        // (and a partitioned minority must not reap the majority).
        if (views.isEmpty() || views.size() <= snapshot.size() / 2.0) return;
        long now = System.currentTimeMillis();
        List<String> responders = new ArrayList<>(views.keySet());
        for (String cand : snapshot) {
            int others = 0, deadVotes = 0;
            for (String r : responders) {
                if (r.equals(cand)) continue;
                others++;
                Map<String, Object> e = (Map<String, Object>) views.get(r).get(cand);
                if (e != null && "dead".equals(e.get("state"))) deadVotes++;
            }
            if (others == 0) continue;
            if (deadVotes > others / 2.0) {
                long first = deadSince.computeIfAbsent(cand, k -> now);
                if (now - first >= reapAfterMs) {
                    emit.accept("FAILURE DETECTOR: " + cand + " reported DEAD by " + deadVotes + "/"
                            + others + " peers for >=" + (reapAfterMs / 1000) + "s → auto-reaping it, restoring RF");
                    deadSince.remove(cand);
                    removeNode(cand, true);
                    return;  // one membership change per tick; re-evaluate next time
                }
            } else {
                deadSince.remove(cand);  // not dead (recovered or never was)
            }
        }
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
