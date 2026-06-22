package com.litedb.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ClusterClient — a client for the distributed database. It can contact ANY node: the node routes
 * single-key ops to the owning shard's leader and coordinates cross-shard transactions. So the
 * client just shuffles the node list and retries the next on failure (which also rides through a
 * node being killed mid-demo).
 */
public final class ClusterClient {
    private final Rpc.Client rpc = new Rpc.Client(4000);
    private final List<String> nodes = ClusterConfig.nodeIds();

    @SuppressWarnings("unchecked")
    private Map<String, Object> call(String method, Map<String, Object> payload) {
        List<String> order = new ArrayList<>(nodes);
        Collections.shuffle(order);
        Map<String, Object> last = NodeServer.resp("ok", false, "error", "all_nodes_unreachable");
        for (String nid : order) {
            Map<String, Object> r = rpc.call(ClusterConfig.HOST, ClusterConfig.port(nid), method, payload);
            if (Boolean.TRUE.equals(r.get("ok"))) return (Map<String, Object>) r.get("result");
            last = NodeServer.resp("ok", false, "error", r.get("error"));
        }
        return last;
    }

    public Map<String, Object> put(String key, String value) {
        return call("put", NodeServer.resp("key", key, "value", value));
    }

    public Map<String, Object> delete(String key) {
        return call("put", NodeServer.resp("key", key, "value", null));
    }

    public String get(String key) {
        Map<String, Object> r = call("get", NodeServer.resp("key", key, "read_ts", null));
        return Boolean.TRUE.equals(r.get("ok")) ? (String) r.get("value") : null;
    }

    public String get(String key, long readTs) {
        Map<String, Object> r = call("get", NodeServer.resp("key", key, "read_ts", readTs));
        return Boolean.TRUE.equals(r.get("ok")) ? (String) r.get("value") : null;
    }

    public Map<String, Object> getFull(String key) {
        return call("get", NodeServer.resp("key", key, "read_ts", null));
    }

    public Long begin() {
        Map<String, Object> r = call("begin", new LinkedHashMap<>());
        return Boolean.TRUE.equals(r.get("ok")) ? ((Number) r.get("read_ts")).longValue() : null;
    }

    public Map<String, Object> txn(Map<String, Object> writes, Long readTs) {
        return call("txn", NodeServer.resp("writes", writes, "read_ts", readTs));
    }

    public List<Map<String, Object>> status() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String nid : nodes) {
            Map<String, Object> r = rpc.call(ClusterConfig.HOST, ClusterConfig.port(nid), "status", new LinkedHashMap<>());
            if (Boolean.TRUE.equals(r.get("ok"))) {
                @SuppressWarnings("unchecked")
                Map<String, Object> res = (Map<String, Object>) r.get("result");
                out.add(res);
            } else {
                out.add(NodeServer.resp("node", nid, "alive", false));
            }
        }
        return out;
    }
}
