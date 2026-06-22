package com.litedb.cluster;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Partitioner — the cluster's partition map: key → shard (consistent hashing) and shard → replica
 * nodes (placement). Self-contained ring (a TreeMap of vnode positions → shard) so it can also
 * report token ranges for the dashboard's ring visualization.
 *
 * With RF &lt; node-count each shard lives on only RF nodes, so a node may not host a given shard —
 * routing then forwards to a node that does.
 */
public final class Partitioner {

    public static final long RING_SIZE = 1L << 32;

    private final List<String> shardIds;
    private final List<String> nodes;
    private final int rf;
    private final TreeMap<Long, String> ring = new TreeMap<>();          // position → shard
    private final Map<String, List<String>> placement = new LinkedHashMap<>();

    public Partitioner(List<String> shardIds, List<String> nodes, int replicationFactor, int vnodes) {
        this.shardIds = new ArrayList<>(shardIds);
        this.nodes = new ArrayList<>(nodes);
        this.rf = replicationFactor;
        if (rf > nodes.size()) throw new IllegalArgumentException("RF > node count");
        for (String s : shardIds) {
            for (int v = 0; v < vnodes; v++) {
                ring.put(hash(s + ":vnode:" + v), s);
            }
        }
        for (int i = 0; i < shardIds.size(); i++) {
            List<String> reps = new ArrayList<>();
            for (int j = 0; j < rf; j++) reps.add(nodes.get((i + j) % nodes.size()));
            placement.put(shardIds.get(i), reps);
        }
    }

    private static long hash(String key) {
        try {
            byte[] d = MessageDigest.getInstance("MD5").digest(key.getBytes("UTF-8"));
            return new BigInteger(1, d).mod(BigInteger.valueOf(RING_SIZE)).longValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String shardFor(String key) {
        long h = hash(key);
        Map.Entry<Long, String> e = ring.ceilingEntry(h);
        return e != null ? e.getValue() : ring.firstEntry().getValue();
    }

    public List<String> replicas(String shardId) { return placement.get(shardId); }
    public String preferredLeader(String shardId) { return placement.get(shardId).get(0); }
    public List<String> shardIds() { return shardIds; }
    public List<String> nodes() { return nodes; }
    public int rf() { return rf; }

    public List<String> shardsOn(String nodeId) {
        List<String> out = new ArrayList<>();
        for (String s : shardIds) if (placement.get(s).contains(nodeId)) out.add(s);
        return out;
    }

    /** key groups → shard, for deciding single- vs cross-shard transactions. */
    public Map<String, List<String>> shardsForKeys(Iterable<String> keys) {
        Map<String, List<String>> groups = new LinkedHashMap<>();
        for (String k : keys) groups.computeIfAbsent(shardFor(k), x -> new ArrayList<>()).add(k);
        return groups;
    }

    // ---- introspection for the dashboard ----------------------------------

    public List<Map<String, Object>> placementList() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String s : shardIds) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("shard", s);
            m.put("preferred", placement.get(s).get(0));
            m.put("replicas", new ArrayList<Object>(placement.get(s)));
            out.add(m);
        }
        return out;
    }

    public List<Map<String, Object>> ringArcs() {
        List<Map<String, Object>> arcs = new ArrayList<>();
        long prev = 0;
        String firstShard = null;
        long lastEnd = 0;
        for (Map.Entry<Long, String> e : ring.entrySet()) {
            if (firstShard == null) firstShard = e.getValue();
            arcs.add(arc(e.getValue(), prev, e.getKey()));
            prev = e.getKey();
            lastEnd = e.getKey();
        }
        if (!arcs.isEmpty() && lastEnd < RING_SIZE) {
            arcs.add(arc(firstShard, lastEnd, RING_SIZE));   // wrap segment
        }
        return arcs;
    }

    private static Map<String, Object> arc(String shard, long start, long end) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("shard", shard);
        m.put("start", start);
        m.put("end", end);
        return m;
    }
}
