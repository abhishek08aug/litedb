"""
pd.py — the Placement Driver as its OWN Raft group (the TiKV PD model).

The control plane used to be a single in-process orchestrator: a SPOF whose death could leave a
rebalance half-applied. Here it is a real **replicated state machine**. A small odd set of nodes
(`PD_NODES`) each run a PD Raft replica; membership *decisions* — `add_node` / `remove_node` — are
committed to the PD Raft log, so they are durable and survive a PD-leader crash. The PD **leader**
runs the orchestration:

  * failure detector — reads gossip liveness; a node confirmed DEAD by a majority of peers past a
    grace window is proposed as `remove_node(dead)` THROUGH the PD log (durable decision).
  * reconcile — an idempotent control loop that moves the data cluster toward
    `compute_placement(active)`, one membership change per shard per pass (add a replica → it catches
    up via Raft → drop the extra). Because it's idempotent and derives "current" from the live
    cluster, a NEW PD leader simply keeps reconciling — a half-finished rebalance is completed, not
    lost. This is the same desired-state-reconciliation a real placement driver does.

So the loop "gossip detected a death → restore RF" now runs inside a fault-tolerant, Raft-replicated
control plane: kill the PD leader and a new one takes over and finishes the job.
"""

import threading
import time

from cluster_config import INITIAL_NODES, SHARDS, compute_placement, node_data_dir
from raft_node import RaftGroup


class Pd:
    def __init__(self, node, pd_nodes: list[str]):
        self.node = node                       # the owning NodeServer (reused for RPC + gossip)
        self.pd_nodes = list(pd_nodes)
        self.active: list[str] = list(INITIAL_NODES)   # replicated membership (state machine)
        self._dead_since: dict[str, float] = {}
        self._running = False
        self._reap_after = 5.0
        self.raft = RaftGroup(
            node_id=node.node_id, group_id="pd",
            peers=[n for n in pd_nodes if n != node.node_id],
            send_fn=node._pd_send, apply_fn=self._apply,
            data_dir=node_data_dir(node.node_id),
            preferred=(bool(pd_nodes) and pd_nodes[0] == node.node_id),
            on_event=node.events.emit, voters=pd_nodes,
        )

    # ---- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._running = True
        self.raft.start()
        threading.Thread(target=self._control_loop, daemon=True).start()

    def stop(self) -> None:
        self._running = False
        self.raft.stop()

    # ---- replicated state machine ----------------------------------------

    def _apply(self, index: int, command: dict) -> None:
        op = command.get("op")
        if op == "add_node":
            if command["node"] not in self.active:
                self.active.append(command["node"])
        elif op == "remove_node":
            if command["node"] in self.active:
                self.active.remove(command["node"])
        # "noop" / "config" entries carry no placement decision

    # ---- client-facing (leader only) -------------------------------------

    def propose(self, decision: dict) -> dict:
        idx = self.raft.propose(decision)
        if idx is None:
            return {"ok": False, "leader": self.raft.leader_id}
        self.raft.wait_commit(idx, timeout=5.0)
        return {"ok": True}

    def status(self) -> dict:
        active = list(self.active)
        return {"ok": True, "active": active, "placement": compute_placement(active),
                "leader": self.raft.leader_id, "is_leader": self.raft.is_leader()}

    # ---- PD-leader control loop: failure detection + reconcile ------------

    def _control_loop(self) -> None:
        time.sleep(2.0)  # let the PD group elect and the data cluster come up
        while self._running:
            time.sleep(1.0)
            try:
                if self.raft.is_leader():
                    self._detect_failures()
                    self._reconcile()
            except Exception as e:
                self.node.events.emit("config", f"PD control error: {e}")

    def _detect_failures(self) -> None:
        """Read gossip liveness from each live node; a node DEAD by a majority of peers past the
        grace window is proposed as remove_node(dead) THROUGH the PD log (durable decision)."""
        active = list(self.active)
        if len(active) <= 1:
            return
        views: dict[str, dict] = {}
        for n in active:
            res = self.node._call(n, "status", {})
            if res.get("ok") and isinstance(res.get("members"), dict):
                views[n] = res["members"]
        if not views or len(views) <= len(active) / 2:   # need a live majority to act safely
            return
        now = time.time()
        responders = list(views)
        for cand in active:
            others = [r for r in responders if r != cand]
            if not others:
                continue
            dead = sum(1 for r in others if views[r].get(cand, {}).get("state") == "dead")
            if dead > len(others) / 2:
                first = self._dead_since.setdefault(cand, now)
                if now - first >= self._reap_after:
                    self.node.events.emit("config", f"PD FAILURE DETECTOR: {cand} reported DEAD by "
                                          f"{dead}/{len(others)} peers → proposing remove_node to the "
                                          f"PD Raft log (durable decision)")
                    self._dead_since.pop(cand, None)
                    idx = self.raft.propose({"op": "remove_node", "node": cand, "dead": True})
                    if idx is not None:
                        self.raft.wait_commit(idx, timeout=5.0)
                    return
            else:
                self._dead_since.pop(cand, None)

    def _reconcile(self) -> None:
        """Idempotent: move each shard toward compute_placement(active), one membership change at a
        time. 'Current' is observed from the live cluster (the shard leader's Raft config), so a new
        PD leader needs no prior in-memory state — it just keeps reconciling toward the target."""
        desired = compute_placement(self.active)
        for shard in SHARDS:
            want = desired.get(shard, [])
            leader = self.node._leader_of(shard, retries=1)
            if not leader:
                continue
            cur = self._voters_of(shard, leader)
            if cur is None:
                continue
            add = [n for n in want if n not in cur]
            drop = [n for n in cur if n not in want]
            if add:
                n = add[0]
                self.node._call(n, "host_shard", {"shard": shard, "voters": cur})
                self.node._call(leader, "reconfigure",
                                {"shard": shard, "voters": sorted(set(cur) | {n})})
            elif drop:
                n = drop[0]
                new = sorted(set(cur) - {n})
                if not new:
                    continue
                self.node._call(leader, "reconfigure", {"shard": shard, "voters": new})
                if self._alive(n):
                    self.node._call(n, "drop_shard", {"shard": shard})

    def _voters_of(self, shard: str, leader: str):
        res = self.node._call(leader, "status", {})
        if not res.get("ok"):
            return None
        for sh in res.get("shards", []):
            if sh["group"] == shard:
                return sorted(sh.get("voters", []))
        return None

    def _alive(self, node_id: str) -> bool:
        return self.node.gossip.view().get(node_id, {}).get("state") != "dead"
