package com.litedb.raft;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * RaftNode — Simplified Raft consensus algorithm.
 *
 * CONCEPT:
 *   Raft is a consensus algorithm that ensures all nodes in a cluster agree
 *   on the same sequence of log entries, even in the presence of failures.
 *
 *   Raft roles:
 *     FOLLOWER  — default state; accepts log entries from leader
 *     CANDIDATE — starts an election when it hasn't heard from leader
 *     LEADER    — handles all client writes; replicates to followers
 *
 *   Key properties:
 *     - Leader election: nodes vote for a candidate; majority wins
 *     - Log replication: leader appends entries, sends to followers
 *     - Safety: a log entry is "committed" only when majority acknowledges it
 *     - Terms: monotonically increasing epoch numbers prevent split-brain
 *
 *   This is how etcd, CockroachDB, TiKV, and Consul achieve consensus.
 *
 *   Simplified here: single-threaded simulation (no real network I/O).
 *   Real Raft also handles: log compaction, snapshots, membership changes.
 */
public class RaftNode {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    // ------------------------------------------------------------------ //
    //  Log entry                                                          //
    // ------------------------------------------------------------------ //

    public static class LogEntry {
        public final int    term;
        public final int    index;
        public final String command;

        public LogEntry(int term, int index, String command) {
            this.term    = term;
            this.index   = index;
            this.command = command;
        }

        @Override public String toString() {
            return "[term=" + term + ",idx=" + index + "] " + command;
        }
    }

    // ------------------------------------------------------------------ //
    //  Node state                                                         //
    // ------------------------------------------------------------------ //

    public final String          nodeId;
    private volatile Role        role        = Role.FOLLOWER;
    private volatile int         currentTerm = 0;
    private volatile String      votedFor    = null;
    private volatile String      leaderId    = null;
    private final List<LogEntry> log         = new ArrayList<>();
    private volatile int         commitIndex = -1;
    private volatile int         lastApplied = -1;

    // Cluster peers (simulated in-process)
    private final List<RaftNode> peers = new ArrayList<>();

    // Vote tracking during election
    private final AtomicInteger  votesReceived = new AtomicInteger(0);

    public RaftNode(String nodeId) {
        this.nodeId = nodeId;
    }

    public void addPeer(RaftNode peer) { peers.add(peer); }

    // ------------------------------------------------------------------ //
    //  Leader election                                                    //
    // ------------------------------------------------------------------ //

    /** Follower times out and starts an election. */
    public void startElection() {
        currentTerm++;
        role     = Role.CANDIDATE;
        votedFor = nodeId;
        votesReceived.set(1); // vote for self
        System.out.println("[Raft] " + nodeId + " starts election for term " + currentTerm);

        for (RaftNode peer : peers) {
            boolean granted = peer.requestVote(currentTerm, nodeId,
                    log.size() - 1,
                    log.isEmpty() ? 0 : log.get(log.size() - 1).term);
            if (granted) {
                int votes = votesReceived.incrementAndGet();
                System.out.println("[Raft] " + nodeId + " received vote from " + peer.nodeId
                        + " (total=" + votes + ")");
            }
        }

        int majority = (peers.size() + 1) / 2 + 1;
        if (votesReceived.get() >= majority) {
            becomeLeader();
        } else {
            System.out.println("[Raft] " + nodeId + " lost election (votes=" + votesReceived.get()
                    + ", needed=" + majority + ")");
            role = Role.FOLLOWER;
        }
    }

    /** Handle a RequestVote RPC from a candidate. */
    public synchronized boolean requestVote(int term, String candidateId,
                                             int lastLogIndex, int lastLogTerm) {
        if (term < currentTerm) return false;
        if (term > currentTerm) {
            currentTerm = term;
            role        = Role.FOLLOWER;
            votedFor    = null;
        }
        // Grant vote if we haven't voted yet and candidate's log is at least as up-to-date
        boolean logOk = lastLogIndex >= log.size() - 1;
        if ((votedFor == null || votedFor.equals(candidateId)) && logOk) {
            votedFor = candidateId;
            System.out.println("[Raft] " + nodeId + " grants vote to " + candidateId
                    + " for term " + term);
            return true;
        }
        return false;
    }

    private void becomeLeader() {
        role     = Role.LEADER;
        leaderId = nodeId;
        System.out.println("[Raft] *** " + nodeId + " becomes LEADER for term " + currentTerm + " ***");
        // Notify peers
        for (RaftNode peer : peers) {
            peer.receiveHeartbeat(currentTerm, nodeId);
        }
    }

    /** Receive heartbeat / AppendEntries with no entries (just leadership assertion). */
    public synchronized void receiveHeartbeat(int term, String leaderId) {
        if (term >= currentTerm) {
            currentTerm  = term;
            role         = Role.FOLLOWER;
            this.leaderId = leaderId;
            votedFor     = null;
        }
    }

    // ------------------------------------------------------------------ //
    //  Log replication                                                    //
    // ------------------------------------------------------------------ //

    /** Leader appends a command and replicates to followers. */
    public boolean appendCommand(String command) {
        if (role != Role.LEADER) {
            System.out.println("[Raft] " + nodeId + " is not leader (leader=" + leaderId + ")");
            return false;
        }
        LogEntry entry = new LogEntry(currentTerm, log.size(), command);
        log.add(entry);
        System.out.println("[Raft] Leader " + nodeId + " appends " + entry);

        // Replicate to followers
        int acks = 1; // self
        for (RaftNode peer : peers) {
            boolean ok = peer.appendEntries(currentTerm, nodeId,
                    log.size() - 2, // prevLogIndex
                    log.size() >= 2 ? log.get(log.size() - 2).term : 0,
                    entry, commitIndex);
            if (ok) acks++;
        }

        // Commit if majority acknowledged
        int majority = (peers.size() + 1) / 2 + 1;
        if (acks >= majority) {
            commitIndex = entry.index;
            applyCommitted();
            // Notify followers of new commitIndex
            for (RaftNode peer : peers) {
                peer.updateCommitIndex(commitIndex);
            }
            System.out.println("[Raft] Entry committed at index=" + commitIndex
                    + " (acks=" + acks + "/" + (peers.size() + 1) + ")");
            return true;
        }
        System.out.println("[Raft] Entry NOT committed — insufficient acks (" + acks + ")");
        return false;
    }

    /** Follower receives AppendEntries RPC. */
    public synchronized boolean appendEntries(int term, String leaderId,
                                               int prevLogIndex, int prevLogTerm,
                                               LogEntry entry, int leaderCommit) {
        if (term < currentTerm) return false;
        currentTerm   = term;
        role          = Role.FOLLOWER;
        this.leaderId = leaderId;

        // Consistency check
        if (prevLogIndex >= 0 && (log.size() <= prevLogIndex
                || log.get(prevLogIndex).term != prevLogTerm)) {
            return false; // log inconsistency
        }

        // Append entry
        if (log.size() > entry.index) {
            log.subList(entry.index, log.size()).clear(); // truncate conflicting
        }
        log.add(entry);

        if (leaderCommit > commitIndex) {
            commitIndex = Math.min(leaderCommit, log.size() - 1);
            applyCommitted();
        }
        return true;
    }

    public synchronized void updateCommitIndex(int idx) {
        if (idx > commitIndex) {
            commitIndex = idx;
            applyCommitted();
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            System.out.println("[Raft] " + nodeId + " applies: " + log.get(lastApplied));
        }
    }

    // ------------------------------------------------------------------ //
    //  Accessors                                                          //
    // ------------------------------------------------------------------ //

    public Role   getRole()        { return role; }
    public int    getTerm()        { return currentTerm; }
    public String getLeaderId()    { return leaderId; }
    public int    getCommitIndex() { return commitIndex; }
    public List<LogEntry> getLog() { return Collections.unmodifiableList(log); }

    @Override public String toString() {
        return nodeId + "[" + role + ",term=" + currentTerm + ",log=" + log.size() + "]";
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) {
        System.out.println("============================================================");
        System.out.println("RAFT CONSENSUS DEMO");
        System.out.println("============================================================\n");

        // Create a 5-node cluster
        RaftNode n1 = new RaftNode("node-1");
        RaftNode n2 = new RaftNode("node-2");
        RaftNode n3 = new RaftNode("node-3");
        RaftNode n4 = new RaftNode("node-4");
        RaftNode n5 = new RaftNode("node-5");

        List<RaftNode> cluster = List.of(n1, n2, n3, n4, n5);
        for (RaftNode a : cluster)
            for (RaftNode b : cluster)
                if (a != b) a.addPeer(b);

        // Step 1: Election
        System.out.println("[Step 1] node-1 starts election");
        n1.startElection();
        System.out.println();

        // Step 2: Append commands
        System.out.println("[Step 2] Leader appends commands");
        n1.appendCommand("SET x=1");
        n1.appendCommand("SET y=2");
        n1.appendCommand("DELETE z");
        System.out.println();

        // Step 3: Show log on all nodes
        System.out.println("[Step 3] Log state across cluster");
        for (RaftNode n : cluster) {
            System.out.println("  " + n + " log=" + n.getLog());
        }

        // Step 4: Simulate node failure — n2 goes down, n1 still has majority
        System.out.println("\n[Step 4] n2 'fails' — leader still has majority (4/5)");
        // Remove n2 from n1's peers (simulate network partition)
        n1.peers.remove(n2);
        n1.appendCommand("SET z=99");
        System.out.println("  Committed: " + (n1.getCommitIndex() >= 3));

        System.out.println("\n[Done] Raft demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. Leader elected by majority vote — prevents split-brain");
        System.out.println("  2. Log entries committed only when majority acknowledges");
        System.out.println("  3. Terms are monotonically increasing — detect stale leaders");
        System.out.println("  4. Followers apply committed entries to their state machine");
        System.out.println("  5. etcd, CockroachDB, TiKV, Consul all use Raft");
    }
}