package com.litedb.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GossipSmoke — prove gossip-based DISCOVERY and failure detection over real TCP (mirror of
 * gossip_smoke.py). node-A is the seed; node-B and node-C are told ONLY node-A's address, yet all
 * three discover each other (transitively). Killing node-C → A and B age it out and mark it DEAD.
 *
 * Run:  java com.litedb.cluster.GossipSmoke
 */
public final class GossipSmoke {

    private static final int BASE = 7600;
    private static final List<String> NAMES = List.of("node-A", "node-B", "node-C");

    private static final class Node {
        Gossip gossip;
        Rpc.Server server;
        Rpc.Client client;
    }

    private static Node make(String nodeId, int port, List<Gossip.Addr> seeds) throws Exception {
        Node n = new Node();
        n.client = new Rpc.Client(1000);
        Gossip.Sender send = (host, p, payload) -> {
            Map<String, Object> r = n.client.call(host, p, "gossip", payload);
            @SuppressWarnings("unchecked")
            Map<String, Object> res = Boolean.TRUE.equals(r.get("ok"))
                    ? (Map<String, Object>) r.get("result") : null;
            return res;
        };
        n.gossip = new Gossip(nodeId, "127.0.0.1", port, seeds, send, null, 0.3, 1.0, 2.0, 2);
        Map<String, Rpc.Handler> handlers = new LinkedHashMap<>();
        handlers.put("gossip", n.gossip::handle);
        n.server = new Rpc.Server(port, handlers);
        n.server.start();
        n.gossip.start();
        return n;
    }

    @SuppressWarnings("unchecked")
    private static String stateOf(Gossip g, String subject) {
        Map<String, Object> e = (Map<String, Object>) g.view().get(subject);
        return e == null ? null : (String) e.get("state");
    }

    private static boolean wait(java.util.function.BooleanSupplier cond, int tries) {
        for (int i = 0; i < tries; i++) {
            if (cond.getAsBoolean()) return true;
            try { Thread.sleep(250); } catch (InterruptedException ignored) {}
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        Map<String, Node> nodes = new LinkedHashMap<>();
        nodes.put("node-A", make("node-A", BASE, List.of()));
        nodes.put("node-B", make("node-B", BASE + 1, List.of(new Gossip.Addr("127.0.0.1", BASE))));
        nodes.put("node-C", make("node-C", BASE + 2, List.of(new Gossip.Addr("127.0.0.1", BASE))));

        boolean converged = wait(() -> {
            for (Node n : nodes.values()) {
                Map<String, Object> v = n.gossip.view();
                if (!v.keySet().equals(new java.util.HashSet<>(NAMES))) return false;
                for (String s : NAMES) if (!"alive".equals(stateOf(n.gossip, s))) return false;
            }
            return true;
        }, 80);
        if (!converged) throw new AssertionError("gossip did not converge");
        System.out.println("DISCOVERY: all 3 nodes found each other from a single seed (node-A) —");
        for (Map.Entry<String, Node> e : nodes.entrySet()) {
            String seeded = e.getKey().equals("node-A") ? "seed" : "seed=node-A only";
            List<String> known = new ArrayList<>(e.getValue().gossip.view().keySet());
            java.util.Collections.sort(known);
            System.out.printf("  %-8s (%-16s) knows: %s%n", e.getKey(), seeded, known);
        }

        Node c = nodes.get("node-C");
        c.gossip.stop();
        c.server.stop();
        c.client.close();
        boolean dead = wait(() ->
                "dead".equals(stateOf(nodes.get("node-A").gossip, "node-C"))
                && "dead".equals(stateOf(nodes.get("node-B").gossip, "node-C")), 80);
        if (!dead) throw new AssertionError("failure detector did not mark node-C dead");
        System.out.println("\nFAILURE DETECTION: node-C killed → A and B aged its heartbeat out and marked it DEAD");

        for (String nid : List.of("node-A", "node-B")) {
            nodes.get(nid).gossip.stop();
            nodes.get(nid).server.stop();
            nodes.get(nid).client.close();
        }
        System.out.println("\nGOSSIP SMOKE PASSED");
    }
}
