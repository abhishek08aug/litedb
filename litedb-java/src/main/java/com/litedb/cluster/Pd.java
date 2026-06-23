package com.litedb.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pd — the Placement Driver as its OWN Raft group (the TiKV PD model). Mirror of pd.py.
 *
 * The control plane is no longer a single in-process orchestrator (a SPOF). A small odd set of nodes
 * ({@code PD_NODES}) each run a PD Raft replica; membership DECISIONS (add_node / remove_node) are
 * committed to the PD Raft log, so they survive a PD-leader crash. The PD LEADER runs the
 * orchestration: a gossip failure detector (proposes remove_node THROUGH the log) and an idempotent
 * reconcile loop that moves the data cluster toward compute_placement(active), one membership change
 * per shard per pass. Because reconcile is idempotent and derives "current" from the live cluster, a
 * NEW PD leader simply keeps reconciling — a half-finished rebalance is completed, not lost.
 */
public final class Pd {
    private final NodeServer node;
    private final List<String> active;          // replicated membership (state machine)
    private final Map<String, Long> deadSince = new LinkedHashMap<>();
    private final double reapAfterS = 5.0;
    private volatile boolean running = false;
    final RaftGroup raft;

    public Pd(NodeServer node, List<String> pdNodes) {
        this.node = node;
        this.active = new ArrayList<>(ClusterConfig.INITIAL_NODES);
        List<String> peers = new ArrayList<>();
        for (String n : pdNodes) if (!n.equals(node.nodeId())) peers.add(n);
        this.raft = new RaftGroup(node.nodeId(), "pd", peers, node::pdSend, this::apply,
                ClusterConfig.nodeDataDir(node.nodeId()),
                !pdNodes.isEmpty() && pdNodes.get(0).equals(node.nodeId()), node.events, pdNodes);
    }

    public void start() {
        running = true;
        raft.start();
        Thread t = new Thread(this::controlLoop, "pd-control-" + node.nodeId());
        t.setDaemon(true);
        t.start();
    }

    public void stop() {
        running = false;
        raft.stop();
    }

    // ---- replicated state machine ----------------------------------------

    private void apply(long index, Map<String, Object> command) {
        String op = (String) command.get("op");
        if ("add_node".equals(op)) {
            String n = (String) command.get("node");
            if (!active.contains(n)) active.add(n);
        } else if ("remove_node".equals(op)) {
            active.remove((String) command.get("node"));
        }
        // "noop" / "config" carry no placement decision
    }

    // ---- client-facing (leader only) -------------------------------------

    public Map<String, Object> propose(Map<String, Object> decision) {
        Long idx = raft.propose(decision);
        if (idx == null) return NodeServer.resp("ok", false, "leader", raft.leaderId());
        raft.waitCommit(idx, 5000);
        return NodeServer.resp("ok", true);
    }

    public Map<String, Object> status() {
        List<String> a = new ArrayList<>(active);
        return NodeServer.resp("ok", true, "active", a, "placement", ClusterConfig.computePlacement(a),
                "leader", raft.leaderId(), "is_leader", raft.isLeader());
    }

    // ---- PD-leader control loop: failure detection + reconcile -----------

    private void controlLoop() {
        sleep(2000);
        while (running) {
            sleep(1000);
            try {
                if (raft.isLeader()) {
                    detectFailures();
                    reconcile();
                }
            } catch (RuntimeException e) {
                node.events.emit("config", "PD control error: " + e.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void detectFailures() {
        List<String> snapshot = new ArrayList<>(active);
        if (snapshot.size() <= 1) return;
        Map<String, Map<String, Object>> views = new LinkedHashMap<>();
        for (String n : snapshot) {
            Map<String, Object> res = node.callPublic(n, "status", NodeServer.resp());
            if (Boolean.TRUE.equals(res.get("ok")) && res.get("members") instanceof Map) {
                views.put(n, (Map<String, Object>) res.get("members"));
            }
        }
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
                if (now - first >= reapAfterS * 1000) {
                    node.events.emit("config", "PD FAILURE DETECTOR: " + cand + " reported DEAD by "
                            + deadVotes + "/" + others + " peers → proposing remove_node to the PD Raft log");
                    deadSince.remove(cand);
                    Long idx = raft.propose(NodeServer.resp("op", "remove_node", "node", cand, "dead", true));
                    if (idx != null) raft.waitCommit(idx, 5000);
                    return;
                }
            } else {
                deadSince.remove(cand);
            }
        }
    }

    private void reconcile() {
        Map<String, List<String>> desired = ClusterConfig.computePlacement(new ArrayList<>(active));
        for (String shard : ClusterConfig.SHARDS) {
            List<String> want = desired.getOrDefault(shard, List.of());
            String leader = node.leaderOfPublic(shard);
            if (leader == null) continue;
            List<String> cur = votersOf(shard, leader);
            if (cur == null) continue;
            List<String> add = new ArrayList<>();
            for (String n : want) if (!cur.contains(n)) add.add(n);
            List<String> drop = new ArrayList<>();
            for (String n : cur) if (!want.contains(n)) drop.add(n);
            if (!add.isEmpty()) {
                String n = add.get(0);
                node.callPublic(n, "host_shard", NodeServer.resp("shard", shard, "voters", new ArrayList<>(cur)));
                java.util.TreeSet<String> nv = new java.util.TreeSet<>(cur);
                nv.add(n);
                node.callPublic(leader, "reconfigure", NodeServer.resp("shard", shard, "voters", new ArrayList<>(nv)));
            } else if (!drop.isEmpty()) {
                String n = drop.get(0);
                java.util.TreeSet<String> nv = new java.util.TreeSet<>(cur);
                nv.remove(n);
                if (nv.isEmpty()) continue;
                node.callPublic(leader, "reconfigure", NodeServer.resp("shard", shard, "voters", new ArrayList<>(nv)));
                if (alive(n)) node.callPublic(n, "drop_shard", NodeServer.resp("shard", shard));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> votersOf(String shard, String leader) {
        Map<String, Object> res = node.callPublic(leader, "status", NodeServer.resp());
        if (!Boolean.TRUE.equals(res.get("ok"))) return null;
        for (Object so : (List<Object>) res.getOrDefault("shards", List.of())) {
            Map<String, Object> sh = (Map<String, Object>) so;
            if (shard.equals(sh.get("group"))) {
                List<String> v = new ArrayList<>((List<String>) sh.getOrDefault("voters", List.of()));
                java.util.Collections.sort(v);
                return v;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private boolean alive(String nodeId) {
        Object e = node.gossipView().get(nodeId);
        return !(e instanceof Map) || !"dead".equals(((Map<String, Object>) e).get("state"));
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}
