package com.litedb.cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RaftGroup — one Raft replica of one replication group (shard), over the {@link Rpc} transport.
 *
 * The classic Raft algorithm (election, log replication, the up-to-date-log voting safety rule,
 * commit advancement) made production-shaped: transport is injected (network RPC, not method
 * calls); current term, vote, and log are persisted with fsync so a restarted replica recovers;
 * committed entries are handed to an injected state machine (the real storage engine). A node runs
 * MANY of these — one per shard it replicates (multi-raft).
 */
public final class RaftGroup {

    public interface Transport {
        Map<String, Object> send(String peerNode, String kind, Map<String, Object> payload);
    }

    public interface StateMachine {
        void apply(long index, Map<String, Object> command);
    }

    public interface Events {
        void emit(String category, String message);
    }

    static final long ELECTION_MIN_MS = 600;
    static final long ELECTION_MAX_MS = 1200;
    static final long HEARTBEAT_MS = 150;

    enum Role { FOLLOWER, CANDIDATE, LEADER }

    public final String nodeId;
    public final String groupId;
    // Membership = a CONFIGURATION (voting members incl. self), carried in the log as
    // {"op":"config","voters":[...]} entries; the latest in the log wins. `peers` (replication / vote
    // targets) is derived = voters - self.
    private final Set<String> initialVoters;
    private Set<String> voters;
    private List<String> peers;
    private final Transport transport;
    private final StateMachine stateMachine;
    private final Events events;
    private final boolean preferred;

    private final File metaPath;
    private final File logPath;

    // persistent state
    private long currentTerm = 0;
    private String votedFor = null;
    private final List<Map<String, Object>> log = new ArrayList<>();

    // volatile state
    private long commitIndex = 0;
    private long lastApplied = 0;
    private long lastAppliedTerm = 0;  // term of the most-recently-applied entry (for readiness)
    private Role role = Role.FOLLOWER;
    private volatile String leaderId = null;
    private final Set<String> votesReceived = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition commitCv = lock.newCondition();
    private long lastHeartbeatNs = System.nanoTime();
    private long electionTimeoutMs;
    private volatile boolean running = false;
    private FileOutputStream logFh;

    public RaftGroup(String nodeId, String groupId, List<String> peers, Transport transport,
                     StateMachine stateMachine, String dataDir, boolean preferred, Events events) {
        this(nodeId, groupId, peers, transport, stateMachine, dataDir, preferred, events, null);
    }

    public RaftGroup(String nodeId, String groupId, List<String> peers, Transport transport,
                     StateMachine stateMachine, String dataDir, boolean preferred, Events events,
                     List<String> voters) {
        this.nodeId = nodeId;
        this.groupId = groupId;
        // A joining node passes `voters` = the current config WITHOUT itself (it's a non-voting
        // follower until the add-config entry replicates to it). Default: peers + self are voters.
        this.initialVoters = new LinkedHashSet<>(voters != null ? voters : peers);
        if (voters == null) this.initialVoters.add(nodeId);
        this.voters = new LinkedHashSet<>(this.initialVoters);
        this.peers = peersFromVoters();
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.preferred = preferred;
        this.events = events != null ? events : (c, m) -> {};
        new File(dataDir).mkdirs();
        this.metaPath = new File(dataDir, "raft-" + groupId + ".meta.json");
        this.logPath = new File(dataDir, "raft-" + groupId + ".log");
        this.electionTimeoutMs = newTimeout();
        recover();
    }

    private List<String> peersFromVoters() {
        List<String> p = new ArrayList<>();
        for (String v : voters) if (!v.equals(nodeId)) p.add(v);
        return p;
    }

    // ---- persistence ------------------------------------------------------

    private long newTimeout() {
        long base = ThreadLocalRandom.current().nextLong(ELECTION_MIN_MS, ELECTION_MAX_MS);
        return preferred ? (long) (base * 0.4) : base;
    }

    @SuppressWarnings("unchecked")
    private void recover() {
        try {
            if (metaPath.exists()) {
                byte[] b = readAll(metaPath);
                Map<String, Object> meta = (Map<String, Object>) Json.parse(new String(b, "UTF-8"));
                currentTerm = ((Number) meta.getOrDefault("term", 0L)).longValue();
                votedFor = (String) meta.get("voted_for");
            }
            if (logPath.exists()) {
                try (BufferedReader r = new BufferedReader(
                        new InputStreamReader(new FileInputStream(logPath), "UTF-8"))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        line = line.trim();
                        if (!line.isEmpty()) {
                            log.add((Map<String, Object>) Json.parse(line));
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("raft recover failed", e);
        }
        refreshVotersFromLog();
    }

    private static byte[] readAll(File f) throws IOException {
        try (FileInputStream in = new FileInputStream(f)) {
            return in.readAllBytes();
        }
    }

    private void persistMeta() {
        try {
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("term", currentTerm);
            meta.put("voted_for", votedFor);
            File tmp = new File(metaPath.getPath() + ".tmp");
            try (FileOutputStream fos = new FileOutputStream(tmp)) {
                fos.write(Json.encode(meta).getBytes("UTF-8"));
                fos.flush();
                fos.getFD().sync();
            }
            if (!tmp.renameTo(metaPath)) {
                java.nio.file.Files.move(tmp.toPath(), metaPath.toPath(),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            }
        } catch (IOException e) {
            throw new RuntimeException("persistMeta failed", e);
        }
    }

    private void appendLogPersist(Map<String, Object> entry) {
        try {
            logFh.write((Json.encode(entry) + "\n").getBytes("UTF-8"));
            logFh.flush();
            logFh.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException("appendLog failed", e);
        }
    }

    private void rewriteLog() {
        try {
            if (logFh != null) logFh.close();
            File tmp = new File(logPath.getPath() + ".tmp");
            try (Writer w = new OutputStreamWriter(new FileOutputStream(tmp), "UTF-8")) {
                for (Map<String, Object> e : log) {
                    w.write(Json.encode(e) + "\n");
                }
                w.flush();
            }
            java.nio.file.Files.move(tmp.toPath(), logPath.toPath(),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            logFh = new FileOutputStream(logPath, true);
        } catch (IOException e) {
            throw new RuntimeException("rewriteLog failed", e);
        }
    }

    // ---- lifecycle --------------------------------------------------------

    public void start() {
        try {
            logFh = new FileOutputStream(logPath, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        running = true;
        Thread e = new Thread(this::electionLoop, "raft-elect-" + groupId);
        Thread l = new Thread(this::leaderLoop, "raft-lead-" + groupId);
        e.setDaemon(true);
        l.setDaemon(true);
        e.start();
        l.start();
    }

    public void stop() {
        running = false;
        if (logFh != null) {
            try { logFh.close(); } catch (IOException ignored) {}
        }
    }

    public boolean isLeader() {
        lock.lock();
        try { return role == Role.LEADER; } finally { lock.unlock(); }
    }

    public boolean isReady() {
        lock.lock();
        try { return role == Role.LEADER && lastAppliedTerm == currentTerm; } finally { lock.unlock(); }
    }

    public String leaderId() { return leaderId; }

    // ---- role transitions -------------------------------------------------

    private void becomeFollower(long term) {
        boolean changed = term != currentTerm || votedFor != null;
        role = Role.FOLLOWER;
        currentTerm = term;
        votedFor = null;
        lastHeartbeatNs = System.nanoTime();
        electionTimeoutMs = newTimeout();
        if (changed) persistMeta();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = nodeId;
        long nxt = log.size() + 1;
        for (String p : peers) {
            nextIndex.put(p, nxt);
            matchIndex.put(p, 0L);
        }
        // Commit a no-op in this term so inherited committed entries (incl. prepared intents) get
        // applied (Raft commit-point rule). Until it applies, the leader is NOT ready. Appended
        // directly — we already hold the lock; propose() would re-enter it.
        Map<String, Object> noop = new LinkedHashMap<>();
        noop.put("term", currentTerm);
        noop.put("index", (long) (log.size() + 1));
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("op", "noop");
        noop.put("command", cmd);
        log.add(noop);
        appendLogPersist(noop);
        events.emit("leader", groupId + ": won a majority of votes → I am now the LEADER for term "
                + currentTerm + "; committing a no-op to become ready");
    }

    private int majority() {
        return voters.size() / 2 + 1;
    }

    @SuppressWarnings("unchecked")
    private void refreshVotersFromLog() {
        Set<String> v = initialVoters;
        for (int i = log.size() - 1; i >= 0; i--) {
            Object c = log.get(i).get("command");
            if (c instanceof Map && "config".equals(((Map<String, Object>) c).get("op"))) {
                v = new LinkedHashSet<>((List<String>) ((Map<String, Object>) c).get("voters"));
                break;
            }
        }
        setVoters(v);
    }

    private void setVoters(Set<String> v) {
        if (v.equals(voters)) return;
        voters = new LinkedHashSet<>(v);
        peers = peersFromVoters();
        long nxt = log.size() + 1;
        for (String p : peers) {
            nextIndex.putIfAbsent(p, nxt);
            matchIndex.putIfAbsent(p, 0L);
        }
        events.emit("config", groupId + ": configuration is now " + new java.util.TreeSet<>(voters));
        // NB: a leader removed from the config keeps leading until that config COMMITS, then steps
        // down (handled in applyCommitted), so the new config is durable first.
    }

    // ---- election ---------------------------------------------------------

    private void electionLoop() {
        while (running) {
            try { Thread.sleep(20); } catch (InterruptedException e) { return; }
            lock.lock();
            try {
                if (role == Role.LEADER || !voters.contains(nodeId)) continue;  // non-voters don't elect
                long elapsedMs = (System.nanoTime() - lastHeartbeatNs) / 1_000_000;
                if (elapsedMs >= electionTimeoutMs) startElection();
            } finally {
                lock.unlock();
            }
        }
    }

    private void startElection() {
        role = Role.CANDIDATE;
        currentTerm += 1;
        votedFor = nodeId;
        votesReceived.clear();
        votesReceived.add(nodeId);
        lastHeartbeatNs = System.nanoTime();
        electionTimeoutMs = newTimeout();
        persistMeta();
        events.emit("election", groupId + ": election timeout — no heartbeat from a leader, so I'm "
                + "starting an election for term " + currentTerm + " and requesting votes from " + peers);
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("term", currentTerm);
        req.put("candidate_id", nodeId);
        req.put("last_log_index", (long) log.size());
        req.put("last_log_term", log.isEmpty() ? 0L : num(log.get(log.size() - 1), "term"));
        for (String p : peers) {
            final String peer = p;
            Thread t = new Thread(() -> requestVote(peer, req));
            t.setDaemon(true);
            t.start();
        }
    }

    @SuppressWarnings("unchecked")
    private void requestVote(String peer, Map<String, Object> req) {
        Map<String, Object> resp = transport.send(peer, "vote", req);
        if (!Boolean.TRUE.equals(resp.get("ok"))) return;
        Map<String, Object> r = (Map<String, Object>) resp.get("result");
        lock.lock();
        try {
            long term = num(r, "term");
            if (role != Role.CANDIDATE || term < currentTerm) return;
            if (term > currentTerm) { becomeFollower(term); return; }
            if (Boolean.TRUE.equals(r.get("vote_granted"))) {
                votesReceived.add((String) r.get("voter_id"));
                long votes = votesReceived.stream().filter(voters::contains).count();
                if (votes >= majority()) becomeLeader();
            }
        } finally {
            lock.unlock();
        }
    }

    public Map<String, Object> handleVote(Map<String, Object> req) {
        lock.lock();
        try {
            long term = num(req, "term");
            if (term > currentTerm) becomeFollower(term);
            boolean granted = false;
            String cand = (String) req.get("candidate_id");
            if (term >= currentTerm && (votedFor == null || votedFor.equals(cand))) {
                long myTerm = log.isEmpty() ? 0 : num(log.get(log.size() - 1), "term");
                long myIndex = log.size();
                long candTerm = num(req, "last_log_term");
                long candIndex = num(req, "last_log_index");
                boolean upToDate = candTerm > myTerm || (candTerm == myTerm && candIndex >= myIndex);
                if (upToDate) {
                    granted = true;
                    votedFor = cand;
                    lastHeartbeatNs = System.nanoTime();
                    persistMeta();
                    events.emit("vote", groupId + ": granted my vote to " + cand + " for term " + term
                            + " (its log is at least as up-to-date as mine — safe to elect)");
                }
            }
            return resp("term", currentTerm, "vote_granted", granted, "voter_id", nodeId);
        } finally {
            lock.unlock();
        }
    }

    // ---- log replication --------------------------------------------------

    private void leaderLoop() {
        while (running) {
            try { Thread.sleep(HEARTBEAT_MS); } catch (InterruptedException e) { return; }
            List<String> targets;
            lock.lock();
            try {
                if (role != Role.LEADER) continue;
                targets = new ArrayList<>(peers);
            } finally {
                lock.unlock();
            }
            for (String p : targets) {
                final String peer = p;
                Thread t = new Thread(() -> replicateTo(peer));
                t.setDaemon(true);
                t.start();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void replicateTo(String peer) {
        Map<String, Object> req;
        lock.lock();
        try {
            if (role != Role.LEADER) return;
            long nextIdx = nextIndex.getOrDefault(peer, 1L);
            long prevIndex = nextIdx - 1;
            long prevTerm = (prevIndex > 0 && prevIndex <= log.size())
                    ? num(log.get((int) prevIndex - 1), "term") : 0;
            List<Map<String, Object>> entries = new ArrayList<>(log.subList((int) nextIdx - 1, log.size()));
            if (!entries.isEmpty()) {
                events.emit("replication", groupId + ": replicating idx " + num(entries.get(0), "index")
                        + ".." + num(entries.get(entries.size() - 1), "index") + " to follower " + peer
                        + " (waiting for a majority to ack before committing)");
            }
            req = new LinkedHashMap<>();
            req.put("term", currentTerm);
            req.put("leader_id", nodeId);
            req.put("prev_log_index", prevIndex);
            req.put("prev_log_term", prevTerm);
            req.put("entries", entries);
            req.put("leader_commit", commitIndex);
        } finally {
            lock.unlock();
        }
        Map<String, Object> resp = transport.send(peer, "append", req);
        if (!Boolean.TRUE.equals(resp.get("ok"))) return;
        Map<String, Object> r = (Map<String, Object>) resp.get("result");
        lock.lock();
        try {
            long term = num(r, "term");
            if (term > currentTerm) { becomeFollower(term); return; }
            if (role != Role.LEADER) return;
            if (Boolean.TRUE.equals(r.get("success"))) {
                long mi = num(r, "match_index");
                matchIndex.put(peer, mi);
                nextIndex.put(peer, mi + 1);
                advanceCommit();
            } else {
                // jump to the follower's reported log end (O(1) catch-up for a far-behind/new replica)
                nextIndex.put(peer, Math.max(1, num(r, "match_index") + 1));
            }
        } finally {
            lock.unlock();
        }
    }

    private void advanceCommit() {
        for (long idx = log.size(); idx > commitIndex; idx--) {
            if (num(log.get((int) idx - 1), "term") != currentTerm) continue;
            int count = voters.contains(nodeId) ? 1 : 0;   // count voters only (learners don't count)
            for (String p : peers) if (voters.contains(p) && matchIndex.getOrDefault(p, 0L) >= idx) count++;
            if (count >= majority()) {
                commitIndex = idx;
                applyCommitted();
                break;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> handleAppend(Map<String, Object> req) {
        lock.lock();
        try {
            long term = num(req, "term");
            if (term > currentTerm) becomeFollower(term);
            if (term < currentTerm) {
                return resp("term", currentTerm, "success", false, "follower_id", nodeId, "match_index", 0L);
            }
            lastHeartbeatNs = System.nanoTime();
            String lid = (String) req.get("leader_id");
            if (leaderId == null || !leaderId.equals(lid)) {
                events.emit("leader", groupId + ": accepting " + lid + " as the leader for term " + term
                        + " (received its AppendEntries) — I am a follower for this shard");
            }
            leaderId = lid;
            if (role == Role.CANDIDATE) becomeFollower(term);

            long prevIndex = num(req, "prev_log_index");
            if (prevIndex > 0) {
                if (log.size() < prevIndex) {
                    return resp("term", currentTerm, "success", false, "follower_id", nodeId,
                            "match_index", (long) log.size());
                }
                if (num(log.get((int) prevIndex - 1), "term") != num(req, "prev_log_term")) {
                    truncate((int) prevIndex - 1);
                    rewriteLog();
                    return resp("term", currentTerm, "success", false, "follower_id", nodeId,
                            "match_index", (long) log.size());
                }
            }

            List<Object> entries = (List<Object>) req.get("entries");
            boolean rewrote = false;
            for (Object eo : entries) {
                Map<String, Object> entry = (Map<String, Object>) eo;
                int idx = (int) num(entry, "index");
                if (idx <= log.size()) {
                    if (num(log.get(idx - 1), "term") != num(entry, "term")) {
                        truncate(idx - 1);
                        log.add(entry);
                        rewrote = true;
                    }
                } else {
                    log.add(entry);
                    if (!rewrote) appendLogPersist(entry);
                }
            }
            if (rewrote) rewriteLog();
            if (!entries.isEmpty()) refreshVotersFromLog();  // a replicated config entry changes membership

            long leaderCommit = num(req, "leader_commit");
            if (leaderCommit > commitIndex) {
                commitIndex = Math.min(leaderCommit, log.size());
                applyCommitted();
            }
            if (!entries.isEmpty()) {
                long first = num((Map<String, Object>) entries.get(0), "index");
                long last = num((Map<String, Object>) entries.get(entries.size() - 1), "index");
                events.emit("replication", groupId + ": received " + entries.size()
                        + (entries.size() == 1 ? " replicated entry " : " replicated entries ")
                        + "(idx " + first + ".." + last + ") from leader " + lid
                        + " — appended to my log (durably, fsync'd)");
            }
            return resp("term", currentTerm, "success", true, "follower_id", nodeId,
                    "match_index", (long) log.size());
        } finally {
            lock.unlock();
        }
    }

    private void truncate(int newSize) {
        while (log.size() > newSize) log.remove(log.size() - 1);
    }

    // ---- state machine apply ----------------------------------------------

    @SuppressWarnings("unchecked")
    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied += 1;
            Map<String, Object> entry = log.get((int) lastApplied - 1);
            lastAppliedTerm = num(entry, "term");
            Map<String, Object> command = (Map<String, Object>) entry.get("command");
            stateMachine.apply(num(entry, "index"), command);
            events.emit("apply", groupId + ": entry idx " + num(entry, "index")
                    + " reached a majority → COMMITTED; applied to the storage engine ("
                    + summarize(command) + ")");
        }
        // A leader removed from the (now-committed) configuration steps down.
        if (role == Role.LEADER && !voters.contains(nodeId)) {
            events.emit("config", groupId + ": I was removed from the config and it committed → "
                    + "stepping down as leader");
            role = Role.FOLLOWER;
            leaderId = null;
        }
        commitCv.signalAll();
    }

    @SuppressWarnings("unchecked")
    private static String summarize(Map<String, Object> command) {
        List<Object> writes = command == null ? null : (List<Object>) command.get("writes");
        if (writes == null || writes.isEmpty()) return "no-op";
        List<String> parts = new ArrayList<>();
        for (Object wo : writes) {
            List<Object> kv = (List<Object>) wo;
            Object v = kv.get(1);
            parts.add(v == null ? kv.get(0) + "=∅(delete)" : kv.get(0) + "=" + v);
        }
        return String.join(", ", parts);
    }

    // ---- client-facing (leader only) --------------------------------------

    public Long propose(Map<String, Object> command) {
        lock.lock();
        try {
            if (role != Role.LEADER) return null;
            long index = log.size() + 1;
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("term", currentTerm);
            entry.put("index", index);
            entry.put("command", command);
            log.add(entry);
            appendLogPersist(entry);
            return index;
        } finally {
            lock.unlock();
        }
    }

    /** Leader-only single-server membership change (one voter added/removed at a time so old and new
     * majorities overlap). Config takes effect immediately (latest-in-log) and replicates as a normal
     * entry. Returns the entry index or null. */
    public Long reconfigure(List<String> newVoters) {
        lock.lock();
        try {
            if (role != Role.LEADER) return null;
            Set<String> want = new LinkedHashSet<>(newVoters);
            Set<String> sym = new LinkedHashSet<>(voters);
            sym.addAll(want);
            int diff = 0;
            for (String n : sym) if (voters.contains(n) != want.contains(n)) diff++;
            if (diff != 1) {
                events.emit("config", groupId + ": REFUSED config change (must change one voter at a time)");
                return null;
            }
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("op", "config");
            cmd.put("voters", new ArrayList<>(want));
            Long index = propose(cmd);
            refreshVotersFromLog();
            return index;
        } finally {
            lock.unlock();
        }
    }

    public boolean waitCommit(long index, long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000;
        lock.lock();
        try {
            while (lastApplied < index) {
                if (role != Role.LEADER) return false;
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return false;
                try {
                    commitCv.await(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public Map<String, Object> status() {
        lock.lock();
        try {
            return resp("group", groupId, "node", nodeId, "role", role.name().toLowerCase(),
                    "ready", role == Role.LEADER && lastAppliedTerm == currentTerm,
                    "term", currentTerm, "leader", leaderId, "log_len", (long) log.size(),
                    "commit_index", commitIndex, "last_applied", lastApplied,
                    "voters", new ArrayList<>(new java.util.TreeSet<>(voters)));
        } finally {
            lock.unlock();
        }
    }

    // ---- helpers ----------------------------------------------------------

    private static long num(Map<String, Object> m, String k) {
        return ((Number) m.get(k)).longValue();
    }

    private static Map<String, Object> resp(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }
}
