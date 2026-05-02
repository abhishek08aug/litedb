package com.litedb.sharding;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * ConsistentHashRing — Consistent Hashing with virtual nodes.
 *
 * CONCEPT:
 *   Consistent hashing solves the problem of distributing data across N nodes
 *   such that when a node is added or removed, only K/N keys need to move
 *   (where K = total keys, N = number of nodes).
 *
 *   Naive modulo hashing: key → node = hash(key) % N
 *     Problem: when N changes, almost ALL keys move → massive reshuffling
 *
 *   Consistent hashing:
 *     - Imagine a ring of 2^32 positions (0 to 2^32-1)
 *     - Each node is placed at multiple positions (virtual nodes / vnodes)
 *     - Each key maps to the NEXT node clockwise on the ring
 *     - Adding/removing a node only affects keys between it and its predecessor
 *     - With V virtual nodes per server, load is evenly distributed
 *
 *   Used by: Amazon DynamoDB, Apache Cassandra, Riak, Memcached (ketama)
 *
 *   Virtual nodes:
 *     - Without vnodes: uneven load if nodes have different hash positions
 *     - With V=150 vnodes per server: load variance < 5%
 *     - Cassandra uses 256 vnodes per node by default
 */
public class ConsistentHashRing {

    private final int                    virtualNodes;
    private final TreeMap<Long, String>  ring = new TreeMap<>();
    private final Set<String>            nodes = new LinkedHashSet<>();

    public ConsistentHashRing(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    // ------------------------------------------------------------------ //
    //  Ring management                                                    //
    // ------------------------------------------------------------------ //

    /** Add a node to the ring (places virtualNodes points on the ring). */
    public void addNode(String node) {
        if (nodes.contains(node)) return;
        nodes.add(node);
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(node + "#vnode" + i);
            ring.put(hash, node);
        }
        System.out.println("[Ring] Added node '" + node + "' (" + virtualNodes + " vnodes)");
    }

    /** Remove a node from the ring. */
    public void removeNode(String node) {
        if (!nodes.contains(node)) return;
        nodes.remove(node);
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(node + "#vnode" + i);
            ring.remove(hash);
        }
        System.out.println("[Ring] Removed node '" + node + "'");
    }

    /** Get the node responsible for a given key. */
    public String getNode(String key) {
        if (ring.isEmpty()) throw new IllegalStateException("Ring is empty");
        long keyHash = hash(key);
        // Find the first node with hash >= keyHash (clockwise)
        Map.Entry<Long, String> entry = ring.ceilingEntry(keyHash);
        if (entry == null) entry = ring.firstEntry(); // wrap around
        return entry.getValue();
    }

    /** Get N replica nodes for a key (for replication). */
    public List<String> getReplicaNodes(String key, int replicas) {
        if (ring.isEmpty()) return Collections.emptyList();
        List<String> result = new ArrayList<>();
        Set<String>  seen   = new LinkedHashSet<>();
        long keyHash = hash(key);

        // Walk clockwise from keyHash, collect distinct physical nodes
        NavigableMap<Long, String> tail = ring.tailMap(keyHash, true);
        for (String node : tail.values()) {
            if (seen.add(node)) result.add(node);
            if (seen.size() == replicas) return result;
        }
        // Wrap around
        for (String node : ring.values()) {
            if (seen.add(node)) result.add(node);
            if (seen.size() == replicas) return result;
        }
        return result;
    }

    /** Return load distribution: how many ring slots each node owns. */
    public Map<String, Integer> loadDistribution() {
        Map<String, Integer> dist = new LinkedHashMap<>();
        for (String node : nodes) dist.put(node, 0);
        for (String node : ring.values()) dist.merge(node, 1, Integer::sum);
        return dist;
    }

    public Set<String> getNodes() { return Collections.unmodifiableSet(nodes); }
    public int ringSize()         { return ring.size(); }

    // ------------------------------------------------------------------ //
    //  Hash function (MD5 → long)                                        //
    // ------------------------------------------------------------------ //

    private static long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            // Take first 8 bytes as a long
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFFL);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) {
        System.out.println("============================================================");
        System.out.println("CONSISTENT HASHING DEMO");
        System.out.println("============================================================\n");

        ConsistentHashRing ring = new ConsistentHashRing(150);

        // Add 3 nodes
        System.out.println("[Step 1] Add 3 nodes");
        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.addNode("node-C");
        System.out.println("  Ring size: " + ring.ringSize() + " positions");

        // Route some keys
        System.out.println("\n[Step 2] Route keys to nodes");
        String[] keys = {"user:1001","user:1002","order:5001","product:99","session:abc"};
        Map<String, String> initialRouting = new LinkedHashMap<>();
        for (String key : keys) {
            String node = ring.getNode(key);
            initialRouting.put(key, node);
            System.out.println("  " + key + " → " + node);
        }

        // Replication: 2 replicas per key
        System.out.println("\n[Step 3] Replica nodes (RF=2)");
        for (String key : keys) {
            System.out.println("  " + key + " → " + ring.getReplicaNodes(key, 2));
        }

        // Load distribution
        System.out.println("\n[Step 4] Load distribution (150 vnodes each)");
        ring.loadDistribution().forEach((node, slots) ->
            System.out.printf("  %-10s: %d slots (%.1f%%)%n",
                node, slots, 100.0 * slots / ring.ringSize()));

        // Add a 4th node — show minimal key movement
        System.out.println("\n[Step 5] Add node-D — observe minimal key movement");
        ring.addNode("node-D");
        int moved = 0;
        for (String key : keys) {
            String newNode = ring.getNode(key);
            boolean changed = !newNode.equals(initialRouting.get(key));
            if (changed) moved++;
            System.out.println("  " + key + " → " + newNode + (changed ? " *** MOVED ***" : ""));
        }
        System.out.println("  Keys moved: " + moved + "/" + keys.length
                + " (" + (100 * moved / keys.length) + "%)");

        // Remove a node
        System.out.println("\n[Step 6] Remove node-B");
        ring.removeNode("node-B");
        for (String key : keys) {
            System.out.println("  " + key + " → " + ring.getNode(key));
        }

        System.out.println("\n[Done] Consistent hashing demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. Adding/removing a node moves only ~1/N of keys");
        System.out.println("  2. Virtual nodes ensure even load distribution");
        System.out.println("  3. getReplicaNodes() gives you replication targets");
        System.out.println("  4. Cassandra uses 256 vnodes; DynamoDB uses similar approach");
        System.out.println("  5. The ring is a sorted map — O(log N) lookup per key");
    }
}