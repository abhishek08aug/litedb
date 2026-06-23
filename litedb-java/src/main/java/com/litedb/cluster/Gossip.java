package com.litedb.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Gossip — SWIM/Cassandra-style gossip for node DISCOVERY + weak liveness (mirror of gossip.py).
 *
 * The decentralized, eventually-consistent membership substrate real clusters use instead of a static
 * address book (Cassandra's gossiper, Serf/Consul's SWIM, CockroachDB's gossip network). Deliberately
 * NOT Raft: Raft gives strong consistency for a KNOWN group; gossip discovers and disseminates WHO the
 * group is — cheaply, leaderlessly, partition-tolerantly.
 *
 *   * Discovery — a node boots knowing only a SEED address, not the full node list. It gossips to the
 *     seed, learns the seed's id + every other member from the reply, and converges within a few
 *     rounds (transitive: the seed tells a newcomer about everyone it already knows).
 *   * Weak liveness — alive/suspect/dead, derived LOCALLY from how recently a member's heartbeat
 *     advanced (the Cassandra split: gossip carries heartbeats; the failure detector is local).
 *
 * Protocol — one RPC method, "gossip", anti-entropy push-pull:
 *   request payload: {"from": nodeId, "members": {id: entry, ...}}
 *   response result: {"members": {id: entry, ...}}            // receiver's merged view back to sender
 *   entry          : {"addr": [host, port], "generation": long, "heartbeat": long}
 *
 * Merge rule (per id): adopt the entry with the higher (generation, heartbeat). `generation` is the
 * sender's startup timestamp, so a RESTARTED node outranks its own stale heartbeat and is re-adopted
 * as alive automatically. `lastUpdate` is local-only (never sent): when we last saw a member's
 * (generation, heartbeat) increase — the failure detector ages members off it.
 */
public final class Gossip {

    public static final String ALIVE = "alive", SUSPECT = "suspect", DEAD = "dead";

    /** send(host, port, payload) -> the "gossip" result map, or null on failure. */
    public interface Sender {
        Map<String, Object> send(String host, int port, Map<String, Object> payload);
    }

    /** A bare [host, port] — seeds and the result of address resolution. */
    public static final class Addr {
        public final String host;
        public final int port;
        public Addr(String host, int port) { this.host = host; this.port = port; }
    }

    private static final class Member {
        String host;
        int port;
        long generation;
        long heartbeat;
        double lastUpdate;
        Member(String host, int port, long generation, long heartbeat, double lastUpdate) {
            this.host = host; this.port = port; this.generation = generation;
            this.heartbeat = heartbeat; this.lastUpdate = lastUpdate;
        }
    }

    private final String nodeId;
    private final String host;
    private final int port;
    private final Sender sender;
    private final Consumer<String> onEvent;
    private final double intervalS, suspectAfterS, deadAfterS;
    private final int fanout;
    private final long generation;
    private final List<Addr> seeds = new ArrayList<>();
    private final Map<String, Member> members = new LinkedHashMap<>();
    private final Object lock = new Object();
    private volatile boolean running = false;
    private long heartbeat = 0;

    public Gossip(String nodeId, String host, int port, List<Addr> seeds, Sender sender,
                  Consumer<String> onEvent) {
        this(nodeId, host, port, seeds, sender, onEvent, 1.0, 3.0, 6.0, 2);
    }

    public Gossip(String nodeId, String host, int port, List<Addr> seeds, Sender sender,
                  Consumer<String> onEvent, double gossipInterval, double suspectAfter,
                  double deadAfter, int fanout) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.sender = sender;
        this.onEvent = onEvent;
        this.intervalS = gossipInterval;
        this.suspectAfterS = suspectAfter;
        this.deadAfterS = deadAfter;
        this.fanout = fanout;
        // generation = startup wall-clock seconds: monotonic across restarts, so a returning node's
        // fresh gossip still outranks the stale (old gen, high heartbeat) others remember.
        this.generation = System.currentTimeMillis() / 1000;
        for (Addr s : seeds) {
            if (!(s.host.equals(host) && s.port == port)) this.seeds.add(s);
        }
        members.put(nodeId, new Member(host, port, generation, 0, nowS()));
    }

    // ---- lifecycle --------------------------------------------------------

    public void start() {
        running = true;
        Thread t = new Thread(this::loop, "gossip-" + nodeId);
        t.setDaemon(true);
        t.start();
    }

    public void stop() { running = false; }

    // ---- inbound (server side of the "gossip" RPC) ------------------------

    @SuppressWarnings("unchecked")
    public Map<String, Object> handle(Map<String, Object> payload) {
        Object inc = payload.get("members");
        if (inc instanceof Map) merge((Map<String, Object>) inc);
        Map<String, Object> r = new LinkedHashMap<>();
        synchronized (lock) { r.put("members", digest()); }
        return r;
    }

    // ---- outbound loop ----------------------------------------------------

    @SuppressWarnings("unchecked")
    private void loop() {
        while (running) {
            sleep((long) (intervalS * 1000));
            if (!running) break;
            List<Addr> targets;
            Map<String, Object> digest;
            synchronized (lock) {
                heartbeat++;
                Member me = members.get(nodeId);
                me.heartbeat = heartbeat;
                me.lastUpdate = nowS();
                targets = pickTargets();
                digest = digest();
            }
            Map<String, Object> req = new LinkedHashMap<>();
            req.put("from", nodeId);
            req.put("members", digest);
            for (Addr t : targets) {
                Map<String, Object> resp = sender.send(t.host, t.port, req);
                if (resp != null && resp.get("members") instanceof Map) {
                    merge((Map<String, Object>) resp.get("members"));
                }
            }
        }
    }

    /** Up to `fanout` random non-dead peers, plus any seed not yet learned by address. Caller holds lock. */
    private List<Addr> pickTargets() {
        double now = nowS();
        List<Addr> peers = new ArrayList<>();
        for (Map.Entry<String, Member> en : members.entrySet()) {
            if (en.getKey().equals(nodeId)) continue;
            if (now - en.getValue().lastUpdate <= deadAfterS) {
                peers.add(new Addr(en.getValue().host, en.getValue().port));
            }
        }
        Collections.shuffle(peers);
        List<Addr> targets = new ArrayList<>(peers.subList(0, Math.min(fanout, peers.size())));
        Set<String> known = new HashSet<>();
        for (Member m : members.values()) known.add(m.host + ":" + m.port);
        for (Addr s : seeds) {
            boolean inTargets = false;
            for (Addr t : targets) if (t.host.equals(s.host) && t.port == s.port) { inTargets = true; break; }
            if (!known.contains(s.host + ":" + s.port) && !inTargets) targets.add(s);
        }
        return targets;
    }

    // ---- merge / failure detection ---------------------------------------

    @SuppressWarnings("unchecked")
    private void merge(Map<String, Object> incoming) {
        double now = nowS();
        synchronized (lock) {
            for (Map.Entry<String, Object> en : incoming.entrySet()) {
                String nid = en.getKey();
                if (nid.equals(nodeId)) continue;  // sole authority about ourselves
                try {
                    Map<String, Object> e = (Map<String, Object>) en.getValue();
                    List<Object> addr = (List<Object>) e.get("addr");
                    String h = (String) addr.get(0);
                    int p = ((Number) addr.get(1)).intValue();
                    long gen = ((Number) e.get("generation")).longValue();
                    long hb = ((Number) e.get("heartbeat")).longValue();
                    Member cur = members.get(nid);
                    if (cur == null) {
                        members.put(nid, new Member(h, p, gen, hb, now));
                        emit("discovered " + nid + " at " + h + ":" + p + " via gossip");
                    } else if (gen > cur.generation || (gen == cur.generation && hb > cur.heartbeat)) {
                        boolean wasDead = (now - cur.lastUpdate) > deadAfterS;
                        cur.host = h; cur.port = p; cur.generation = gen; cur.heartbeat = hb;
                        cur.lastUpdate = now;
                        if (wasDead) emit(nid + " is alive again (rejoined) — re-adopted via gossip");
                    }
                } catch (RuntimeException ex) {
                    // ignore a malformed entry rather than crash the gossip round
                }
            }
        }
    }

    private Map<String, Object> digest() {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Member> en : members.entrySet()) {
            Member m = en.getValue();
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("addr", List.of(m.host, (long) m.port));
            e.put("generation", m.generation);
            e.put("heartbeat", m.heartbeat);
            out.put(en.getKey(), e);
        }
        return out;
    }

    private String state(String nid, double now) {
        if (nid.equals(nodeId)) return ALIVE;
        double age = now - members.get(nid).lastUpdate;
        if (age > deadAfterS) return DEAD;
        if (age > suspectAfterS) return SUSPECT;
        return ALIVE;
    }

    // ---- views (routing, status, dashboard) -------------------------------

    public Addr addrOf(String nid) {
        synchronized (lock) {
            Member m = members.get(nid);
            return m == null ? null : new Addr(m.host, m.port);
        }
    }

    public Map<String, Object> view() {
        double now = nowS();
        Map<String, Object> out = new LinkedHashMap<>();
        synchronized (lock) {
            List<String> ids = new ArrayList<>(members.keySet());
            Collections.sort(ids);
            for (String nid : ids) {
                Member m = members.get(nid);
                Map<String, Object> e = new LinkedHashMap<>();
                e.put("addr", List.of(m.host, (long) m.port));
                e.put("state", state(nid, now));
                e.put("heartbeat", m.heartbeat);
                e.put("generation", m.generation);
                out.put(nid, e);
            }
        }
        return out;
    }

    private void emit(String m) { if (onEvent != null) onEvent.accept(m); }

    private static double nowS() { return System.currentTimeMillis() / 1000.0; }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}
