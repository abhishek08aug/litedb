package com.litedb.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * EventLog — a per-node, human-readable event log. Each node threads its {@code emit} into its Raft
 * groups, shard replicas, and request handlers, so every meaningful action records a sentence
 * explaining what happened and why. The dashboard polls these incrementally (by sequence number)
 * and shows one live feed per instance plus a merged system stream.
 */
public final class EventLog implements RaftGroup.Events {
    private final int capacity;
    private final List<Map<String, Object>> events = new ArrayList<>();
    private long seq = 0;

    public EventLog() { this(600); }
    public EventLog(int capacity) { this.capacity = capacity; }

    @Override
    public synchronized void emit(String category, String message) {
        seq++;
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("seq", seq);
        e.put("t", System.currentTimeMillis() / 1000.0);
        e.put("cat", category);
        e.put("msg", message);
        events.add(e);
        if (events.size() > capacity) {
            events.subList(0, events.size() - capacity).clear();
        }
    }

    public synchronized Map<String, Object> since(long afterSeq) {
        List<Object> out = new ArrayList<>();
        for (Map<String, Object> e : events) {
            if (((Number) e.get("seq")).longValue() > afterSeq) out.add(e);
        }
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("events", out);
        r.put("next", seq);
        return r;
    }
}
