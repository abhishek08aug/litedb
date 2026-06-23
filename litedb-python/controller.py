"""
controller.py — the cluster's control plane (a simplified TiKV "Placement Driver").

Holds the authoritative shard→node placement and orchestrates membership changes when a node is
added or removed. It computes a target placement (even spread), diffs it against the current one,
and applies the moves one Raft membership change at a time:

  add a replica:    create a follower on the target node → leader adds it to the config →
                    it catches up via Raft replication (this is the data "moving")
  remove a replica: leader drops it from the config → tell the node to discard its copy

Single-server-at-a-time keeps every Raft group safe (old and new majorities overlap). Demo scope:
the controller itself is a single orchestrator (a real PD is its own Raft group), and placement is
broadcast to nodes (eventual consistency; stale routing just gets a `not_hosted` and re-resolves).

It also runs an opt-in **failure detector** (`start_failure_detector`): a reconcile loop that reads
gossip liveness via node `status` and, when a node is reported DEAD by a majority of its live peers
past a grace window, auto-fires `remove_node(dead=True)` to restore RF — closing the loop from
"gossip detected a death" to "redundancy healed" with no human action.
"""

import threading
import time
from typing import Callable, Optional

from cluster_config import INITIAL_NODES, NODES, SHARDS, compute_placement
from rpc import RPCClient


class Controller:
    def __init__(self, active: Optional[list[str]] = None,
                 on_event: Optional[Callable[[str], None]] = None):
        self.rpc = RPCClient(timeout=3.0)
        self.active = list(active if active is not None else INITIAL_NODES)
        self.placement: dict[str, list[str]] = compute_placement(self.active)
        self._emit = on_event or (lambda m: None)
        # membership ops (add/remove/auto-reap) serialize on this so the failure detector and the UI
        # never run two rebalances at once.
        self._lock = threading.RLock()
        # gossip-driven auto-heal (opt-in via start_failure_detector)
        self._fd_running = False
        self._fd_interval = 2.0
        self._reap_after = 5.0
        self._dead_since: dict[str, float] = {}

    # ---- RPC helpers ------------------------------------------------------

    def _call(self, node: str, method: str, payload: dict, timeout: float = 3.0) -> dict:
        host, port = NODES[node]
        resp = self.rpc.call(host, port, method, payload, timeout=timeout)
        return resp["result"] if resp.get("ok") else {"ok": False, "error": resp.get("error")}

    def _leader_of(self, shard: str, retries: int = 40) -> Optional[str]:
        for _ in range(retries):
            for n in self.placement.get(shard, []):
                if n not in self.active:
                    continue
                res = self._call(n, "shard_leader", {"shard": shard})
                if res.get("ok") and res.get("leader"):
                    return res["leader"]
            time.sleep(0.1)
        return None

    def broadcast_placement(self) -> None:
        for n in self.active:
            self._call(n, "update_placement", {"placement": self.placement})

    # ---- membership operations -------------------------------------------

    def add_node(self, new_node: str) -> None:
        with self._lock:
            self._emit(f"ADD node {new_node}: rebalancing shards onto it")
            if new_node not in self.active:
                self.active.append(new_node)
            self._rebalance(compute_placement(self.active))
            self._emit(f"ADD node {new_node}: done")

    def remove_node(self, node: str, dead: bool = False) -> None:
        with self._lock:
            self._emit(f"REMOVE node {node} (dead={dead}): re-replicating its shards to restore RF")
            if node in self.active:
                self.active.remove(node)
            self._rebalance(compute_placement(self.active), departing=node, dead=dead)
            self._emit(f"REMOVE node {node}: done")

    # ---- gossip-driven auto-heal (failure detector) -----------------------

    def start_failure_detector(self, interval: float = 2.0, reap_after: float = 5.0) -> None:
        """Reconcile loop: read gossip liveness via node `status`; a node reported DEAD by a majority
        of its live peers, and held dead past `reap_after`, is reaped via `remove_node(dead=True)` to
        restore RF — no human action needed. The grace window lets a quick restart/deploy avoid an
        expensive re-replication; the majority rule + alive-majority guard prevent acting on one
        node's false suspicion or a partitioned minority."""
        self._fd_interval = interval
        self._reap_after = reap_after
        self._fd_running = True
        threading.Thread(target=self._fd_loop, daemon=True).start()

    def stop(self) -> None:
        self._fd_running = False

    def _fd_loop(self) -> None:
        while self._fd_running:
            time.sleep(self._fd_interval)
            try:
                self._reconcile_once()
            except Exception as e:  # never let the heal loop die
                self._emit(f"failure-detector error: {e}")

    def _reconcile_once(self) -> None:
        with self._lock:
            active = list(self.active)
        if len(active) <= 1:
            return
        # Gather each live node's gossip view (status now carries it). A dead node won't respond.
        views: dict[str, dict] = {}
        for n in active:
            res = self._call(n, "status", {})
            if res.get("ok") and isinstance(res.get("members"), dict):
                views[n] = res["members"]
        # Only act while a MAJORITY is alive — otherwise a Raft config change can't commit anyway
        # (and a partitioned minority must not reap the majority).
        if not views or len(views) <= len(active) / 2:
            return
        now = time.time()
        responders = list(views)
        for cand in active:
            others = [r for r in responders if r != cand]
            if not others:
                continue
            dead_votes = sum(1 for r in others
                             if views[r].get(cand, {}).get("state") == "dead")
            if dead_votes > len(others) / 2:
                first = self._dead_since.setdefault(cand, now)
                if now - first >= self._reap_after:
                    self._emit(f"FAILURE DETECTOR: {cand} reported DEAD by {dead_votes}/{len(others)}"
                               f" peers for ≥{self._reap_after:.0f}s → auto-reaping it, restoring RF")
                    self._dead_since.pop(cand, None)
                    self.remove_node(cand, dead=True)
                    return  # one membership change per tick; re-evaluate next time
            else:
                self._dead_since.pop(cand, None)  # not dead (recovered or never was)

    def _rebalance(self, target: dict[str, list[str]],
                   departing: Optional[str] = None, dead: bool = False) -> None:
        for shard in SHARDS:
            cur = set(self.placement.get(shard, []))
            want = set(target.get(shard, []))
            for n in sorted(want - cur):       # add new replicas first (catch up)...
                self._add_replica(shard, n)
            for n in sorted(cur - want):       # ...then drop the old ones
                self._remove_replica(shard, n, dead=(dead and n == departing))
        self.placement = target
        self.broadcast_placement()

    def _add_replica(self, shard: str, node: str) -> None:
        cur = list(self.placement.get(shard, []))
        # 1) create a follower replica on `node` with the CURRENT config (it's a non-voter until added)
        self._call(node, "host_shard", {"shard": shard, "voters": cur})
        # 2) the leader adds `node` to the configuration (one-server change)
        leader = self._leader_of(shard)
        if not leader:
            self._emit(f"  {shard}: no leader; cannot add {node}")
            return
        new_voters = sorted(set(cur) | {node})
        self._call(leader, "reconfigure", {"shard": shard, "voters": new_voters})
        self.placement[shard] = new_voters
        # 3) wait for `node` to become a voter (it has caught up the log incl. the add-config entry)
        if self._wait_voter(shard, node):
            self._emit(f"  {shard}: +{node} (data caught up via Raft)")
        else:
            self._emit(f"  {shard}: +{node} (still catching up)")

    def _remove_replica(self, shard: str, node: str, dead: bool = False) -> None:
        cur = list(self.placement.get(shard, []))
        new_voters = sorted(set(cur) - {node})
        if not new_voters:
            return
        leader = self._leader_of(shard)
        if leader:
            self._call(leader, "reconfigure", {"shard": shard, "voters": new_voters})
        self.placement[shard] = new_voters
        if not dead:
            self._call(node, "drop_shard", {"shard": shard})
        self._emit(f"  {shard}: -{node}")

    def _wait_voter(self, shard: str, node: str, timeout: float = 8.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            res = self._call(node, "status", {})
            if res.get("ok"):
                for sh in res.get("shards", []):
                    if sh["group"] == shard and node in sh.get("voters", []):
                        return True
            time.sleep(0.15)
        return False
