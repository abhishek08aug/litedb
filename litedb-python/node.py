"""
node.py — a single database instance (one process).

Hosts one replica of every shard placed on this node (here: every shard, RF=3). Concretely this
process runs MANY Raft groups at once — leader of some shards, follower of others — which is the
multi-raft model that makes sharding scale (one leader per shard, spread across nodes), as opposed
to a single cluster-wide Raft leader that would bottleneck all writes.

Responsibilities:
  - dispatch inbound Raft RPCs (vote/append) to the right shard's group
  - act as a router: a client can hit ANY node; if it isn't the leader for a key's shard it
    forwards to the node that is
  - act as the 2PC coordinator for cross-shard transactions
  - expose status for the dashboard

Run as a process:  python node.py <node-id>
"""

import os
import shutil
import sys
import threading
import time
from typing import Optional

import _loader  # noqa: F401  (puts this dir on sys.path)
from cluster_config import (
    INITIAL_NODES,
    NODES,
    PD_NODES,
    compute_placement,
    make_partitioner,
    node_data_dir,
    seed_addrs,
)
from events import EventLog
from gossip import Gossip
from hlc import HLC
from pd import Pd
from raft_node import RPC_TIMEOUT
from rpc import RPCClient, RPCServer
from shard_replica import ShardReplica
from txn_log import TxnLog

FORWARD_TIMEOUT = 3.0
SWEEP_INTERVAL = 2.0    # how often the coordinator re-drives in-doubt transactions
PREPARE_TIMEOUT = 10.0  # a txn stuck 'preparing' this long → the coordinator died mid-prepare → abort


class NodeServer:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.host, self.port = NODES[node_id]
        self.partitioner = make_partitioner()  # key -> shard (static) + ring viz
        self.hlc = HLC()
        self.events = EventLog()
        self.client = RPCClient(timeout=RPC_TIMEOUT)
        self.data_dir = node_data_dir(node_id)
        self.txnlog = TxnLog(self.data_dir)
        self._running = False
        # fencing: a rejoining node may resurrect stale replicas of shards it's no longer a voter of.
        # Pre-vote keeps them harmless; the fence loop then drops + wipes them. `_orphan_since`
        # debounces so we don't fence during a transient (e.g. a shard mid-add).
        self._orphan_since: dict[str, float] = {}
        self._fence_after = 4.0

        # Gossip: discover peers (and their liveness) from a SMALL seed set instead of the static
        # pool. Membership learned here also feeds address resolution (with a static fallback).
        self.gossip = Gossip(
            node_id, [self.host, self.port], seed_addrs(node_id),
            send_fn=self._gossip_send,
            on_event=lambda m: self.events.emit("gossip", m),
        )

        # The Placement Driver is a Raft group co-located on PD_NODES; only those nodes run a replica.
        self.pd: Optional[Pd] = Pd(self, PD_NODES) if node_id in PD_NODES else None

        # shard -> replica nodes. Dynamic: the controller updates it as nodes are added/removed.
        # Starts from the initial even placement; a node added later starts empty and is told what
        # to host + what the placement is.
        self.placement: dict[str, list[str]] = compute_placement(INITIAL_NODES)
        self.shards: dict[str, ShardReplica] = {}
        for s, nodes in self.placement.items():
            if node_id in nodes:
                self._create_replica(s, nodes)

        self.handlers = {
            # raft transport
            "vote": lambda p: self.shards[p["shard"]].raft.handle_vote(p),
            "append": lambda p: self.shards[p["shard"]].raft.handle_append(p),
            # client entry points (any node)
            "put": self._on_put,
            "get": self._on_get,
            "txn": self._on_txn,
            "begin": self._on_begin,
            "status": self._on_status,
            "events": lambda p: self.events.since(p.get("after", 0)),
            "gossip": lambda p: self.gossip.handle(p),
            # placement-driver Raft group (control plane)
            "pd_vote": lambda p: self.pd.raft.handle_vote(p) if self.pd else {"error": "no_pd"},
            "pd_append": lambda p: self.pd.raft.handle_append(p) if self.pd else {"error": "no_pd"},
            "pd_propose": self._on_pd_propose,
            "pd_status": self._on_pd_status,
            "shard_leader": self._on_shard_leader,
            # internal shard ops (coordinator -> shard leader); guarded for shards not hosted here
            "shard_write": lambda p: self._with_shard(
                p, lambda r: r.commit_write(p["writes"], p.get("read_ts"))),
            "shard_get": self._on_shard_get,
            "shard_prepare": lambda p: self._with_shard(
                p, lambda r: r.prepare(p["txn_id"], p["writes"], p["read_ts"], p["commit_ts"],
                                       p.get("coordinator"))),
            "shard_commit": lambda p: self._with_shard(p, lambda r: r.commit_prepared(p["txn_id"])),
            "shard_abort": lambda p: self._with_shard(p, lambda r: r.abort_prepared(p["txn_id"])),
            # cluster membership / rebalancing (driven by the controller)
            "host_shard": self._on_host_shard,
            "drop_shard": self._on_drop_shard,
            "reconfigure": self._on_reconfigure,
            "update_placement": self._on_update_placement,
        }
        self.server = RPCServer(self.host, self.port, self.handlers)

    def _create_replica(self, shard: str, voters: list[str]) -> None:
        """Create (and, if already running, start) a replica of `shard` with the given Raft
        configuration. A node being added passes the current config NOT including itself, so it
        joins as a non-voting follower and becomes a voter when the add-config entry replicates."""
        if shard in self.shards:
            return
        peers = [n for n in voters if n != self.node_id]
        rep = ShardReplica(
            node_id=self.node_id, shard_id=shard, peers=peers, send_fn=self._make_send(shard),
            data_dir=self.data_dir, hlc=self.hlc,
            preferred=(bool(voters) and voters[0] == self.node_id),
            on_event=self.events.emit, voters=voters,
        )
        self.shards[shard] = rep
        if self._running:
            rep.start()

    def _on_host_shard(self, p: dict) -> dict:
        self._create_replica(p["shard"], p["voters"])
        self.events.emit("config", f"asked to host {p['shard']} (config {sorted(p['voters'])}) — "
                                    f"created a follower replica; will catch up via Raft")
        return {"ok": True}

    def _on_drop_shard(self, p: dict) -> dict:
        shard = p["shard"]
        rep = self.shards.pop(shard, None)
        if rep is not None:
            rep.stop()
            self._wipe_shard_files(shard)  # no longer hosting it → wipe, so a re-add starts clean
            self.events.emit("config", f"dropped + wiped my replica of {shard} (no longer assigned here)")
        return {"ok": True}

    def _wipe_shard_files(self, shard: str) -> None:
        """Delete a shard's on-disk state on this node (its MVCC/LSM data dir + its Raft log dir), so
        a fenced/dropped replica can't resurrect stale state on a later restart."""
        for sub in (f"shard-{shard}-data", f"shard-{shard}-raft"):
            shutil.rmtree(os.path.join(self.data_dir, sub), ignore_errors=True)

    # ------------------------------------------------------------------ #
    #  Fencing — drop + wipe replicas this node is no longer a voter of    #
    # ------------------------------------------------------------------ #

    def _pd_placement(self) -> Optional[dict]:
        """The PD's authoritative shard→voters placement, or None if no PD replica answered."""
        for n in PD_NODES:
            res = self._call(n, "pd_status", {})
            if res.get("ok") and isinstance(res.get("placement"), dict):
                return res["placement"]
        return None

    def _fence_loop(self) -> None:
        time.sleep(3.0)  # let the PD elect and our own replicas come up first
        while self._running:
            try:
                self._fence_orphans()
            except Exception as e:
                self.events.emit("config", f"fence error: {e}")
            time.sleep(2.0)

    def _fence_orphans(self) -> None:
        placement = self._pd_placement()
        if placement is None:
            return  # PD unreachable — never fence on a guess
        now = time.time()
        for shard in list(self.shards):
            if self.node_id in placement.get(shard, []):
                self._orphan_since.pop(shard, None)
                continue
            first = self._orphan_since.setdefault(shard, now)
            if now - first >= self._fence_after:
                self._orphan_since.pop(shard, None)
                self.events.emit("config", f"FENCING orphan replica of {shard}: the PD no longer "
                                           f"lists me as a voter → stopping and wiping it")
                rep = self.shards.pop(shard, None)
                if rep is not None:
                    rep.stop()
                self._wipe_shard_files(shard)

    def _on_reconfigure(self, p: dict) -> dict:
        rep = self.shards.get(p["shard"])
        if rep is None:
            return {"ok": False, "error": "not_hosted"}
        idx = rep.reconfigure(p["voters"])
        return {"ok": idx is not None, "index": idx}

    def _on_update_placement(self, p: dict) -> dict:
        self.placement = {s: list(ns) for s, ns in p["placement"].items()}
        return {"ok": True}

    def _addr(self, node: str) -> list:
        """Resolve a node's address — prefer what gossip discovered, fall back to the static pool
        (so routing keeps working during the convergence window and in single-machine runs)."""
        return self.gossip.addr_of(node) or list(NODES[node])

    def _gossip_send(self, host: str, port: int, payload: dict) -> Optional[dict]:
        resp = self.client.call(host, port, "gossip", payload, timeout=1.0)
        return resp["result"] if resp.get("ok") else None

    def _pd_send(self, peer_node: str, kind: str, payload: dict) -> dict:
        """Transport for the PD Raft group: a vote/append rides its own pd_* RPC method so it doesn't
        collide with the per-shard Raft routing (which keys off the shard id)."""
        host, port = self._addr(peer_node)
        return self.client.call(host, port, "pd_" + kind, payload, timeout=RPC_TIMEOUT)

    def _on_pd_propose(self, p: dict) -> dict:
        if self.pd is None:
            return {"ok": False, "error": "no_pd"}
        return self.pd.propose(p["decision"])

    def _on_pd_status(self, _p: dict) -> dict:
        if self.pd is None:
            return {"ok": False, "error": "no_pd"}
        return self.pd.status()

    def _make_send(self, shard_id: str):
        def send(peer_node: str, kind: str, payload: dict) -> dict:
            host, port = self._addr(peer_node)
            msg = dict(payload)
            msg["shard"] = shard_id  # tell the peer which group this RPC is for
            return self.client.call(host, port, kind, msg, timeout=RPC_TIMEOUT)
        return send

    def start(self) -> None:
        self._running = True
        self.server.start()
        self.gossip.start()
        if self.pd is not None:
            self.pd.start()
        for rep in self.shards.values():
            rep.start()
        threading.Thread(target=self._sweep_loop, daemon=True).start()
        threading.Thread(target=self._fence_loop, daemon=True).start()

    def _sweep_loop(self) -> None:
        """Drives in-doubt 2PC to completion. On restart it (a) re-stages any txns this node had
        prepared as a participant — re-acquiring their locks — and (b) re-drives any txns this node
        was coordinating. Then it keeps re-driving periodically so a participant that was down during
        commit/abort gets resolved when it returns."""
        time.sleep(1.5)  # let the RPC server + peers come up and elect leaders
        # Prepared intents are replicated through each shard's Raft log, so a restarted replica (or a
        # new leader after a leadership change) rebuilds them automatically — no participant-side
        # recovery step is needed here. This loop drives the COORDINATOR side to completion.
        while True:
            try:
                self._sweep_once()
            except Exception as e:
                self.events.emit("txn", f"sweep error: {e}")
            time.sleep(SWEEP_INTERVAL)

    def _sweep_once(self) -> None:
        for rec in self.txnlog.pending():
            status = rec["status"]
            if status == "preparing":
                # A live coordinator advances 'preparing' to committing/aborted in milliseconds; if
                # it's still 'preparing' past the timeout, the coordinator crashed mid-prepare → abort.
                if time.time() - rec.get("ts", 0) > PREPARE_TIMEOUT:
                    self.events.emit("txn", f"RECOVERY: txn {rec['txn_id']} stuck 'preparing' past "
                                            f"timeout → deciding ABORT")
                    self._drive_txn(self._txn_write(rec["txn_id"], "aborted",
                                                    rec["participants"], rec["commit_ts"]))
            else:  # committing | aborted — re-drive until every participant has acked
                self.events.emit("txn", f"RECOVERY: re-driving {status} txn {rec['txn_id']} "
                                        f"(a participant had not acked)")
                self._drive_txn(rec)

    def stop(self) -> None:
        self.gossip.stop()
        if self.pd is not None:
            self.pd.stop()
        for rep in self.shards.values():
            rep.stop()
        self.server.stop()

    # ------------------------------------------------------------------ #
    #  Local/remote dispatch helper                                        #
    # ------------------------------------------------------------------ #

    def _call(self, node: str, method: str, payload: dict) -> dict:
        """Invoke a handler on `node`, unwrapping the RPC envelope. Local calls dispatch directly."""
        if node == self.node_id:
            return self.handlers[method](payload)
        host, port = self._addr(node)
        resp = self.client.call(host, port, method, payload, timeout=FORWARD_TIMEOUT)
        if not resp.get("ok"):
            return {"ok": False, "error": resp.get("error", "rpc_failed")}
        return resp["result"]

    def _with_shard(self, p: dict, fn):
        """Run an op against a locally-hosted shard replica, or report that we don't host it."""
        shard = p["shard"]
        rep = self.shards.get(shard)
        if rep is None:
            return {"ok": False, "error": "not_hosted", "shard": shard}
        return fn(rep)

    def _on_shard_leader(self, p: dict) -> dict:
        shard = p["shard"]
        rep = self.shards.get(shard)
        if rep is None:
            return {"ok": False, "error": "not_hosted"}
        return {"ok": True, "leader": rep.leader_id()}

    def _call_ready(self, node: str, method: str, payload: dict, retries: int = 25) -> dict:
        """Like _call, but transparently retries while a just-elected leader is not yet ready to
        serve conflict-checked writes (it's committing its no-op)."""
        res = self._call(node, method, payload)
        for _ in range(retries):
            if res.get("error") != "not_ready":
                return res
            time.sleep(0.1)
            res = self._call(node, method, payload)
        return res

    def _leader_of(self, shard: str, retries: int = 15) -> Optional[str]:
        """Resolve a shard's leader whether or not THIS node hosts the shard. If it does, read the
        local Raft view; otherwise ask the shard's replica nodes (works for any replication factor)."""
        for _ in range(retries):
            if shard in self.shards:
                lid = self.shards[shard].leader_id()
                if lid:
                    return lid
            else:
                for rep_node in self.placement.get(shard, []):
                    if rep_node == self.node_id:
                        continue
                    res = self._call(rep_node, "shard_leader", {"shard": shard})
                    if res.get("ok") and res.get("leader"):
                        return res["leader"]
            time.sleep(0.1)
        return None

    # ------------------------------------------------------------------ #
    #  Client entry points                                                 #
    # ------------------------------------------------------------------ #

    def _on_begin(self, _p: dict) -> dict:
        # A snapshot timestamp: reads pinned to it see everything committed before now and nothing after.
        return {"ok": True, "read_ts": self.hlc.now()}

    def _on_put(self, p: dict) -> dict:
        return self._on_txn({"writes": {p["key"]: p.get("value")}, "read_ts": p.get("read_ts")})

    def _my_relation_to(self, shard: str) -> str:
        rep = self.shards.get(shard)
        if rep is None:
            return "I don't host this shard"
        return "I'm its leader" if rep.is_leader() else "I'm a follower of this shard"

    def _on_get(self, p: dict) -> dict:
        key = p["key"]
        shard = self.partitioner.shard_for(key)
        relation = self._my_relation_to(shard)
        leader = self._leader_of(shard)
        if not leader:
            return {"ok": False, "error": "no_leader", "shard": shard}
        if leader == self.node_id:
            rep = self.shards[shard]
            self.events.emit("routing", f"GET {key} → consistent hashing maps it to {shard}; "
                                        f"{relation} → I serve the read locally (MVCC snapshot read)")
            return {"ok": True, "value": rep.read(key, p.get("read_ts")),
                    "shard": shard, "snapshot_ts": rep.snapshot_ts()}
        self.events.emit("routing", f"GET {key} → maps to {shard} (consistent hashing); {relation}, "
                                    f"so I resolved its leader = {leader} and forward the read there "
                                    f"(leader read = linearizable)")
        return self._call_ready(leader, "shard_get",
                                {"shard": shard, "key": key, "read_ts": p.get("read_ts")})

    def _on_shard_get(self, p: dict) -> dict:
        rep = self.shards.get(p["shard"])
        if rep is None:
            return {"ok": False, "error": "not_hosted", "shard": p["shard"]}
        return {"ok": True, "value": rep.read(p["key"], p.get("read_ts")),
                "shard": p["shard"], "snapshot_ts": rep.snapshot_ts()}

    def _on_txn(self, p: dict) -> dict:
        """Atomically apply a set of writes. Single-shard → one Raft commit; multi-shard → 2PC."""
        writes: dict = p["writes"]
        read_ts = p.get("read_ts")
        groups: dict[str, dict] = {}
        for k, v in writes.items():
            groups.setdefault(self.partitioner.shard_for(k), {})[k] = v

        if len(groups) == 1:
            shard, w = next(iter(groups.items()))
            leader = self._leader_of(shard)
            if not leader:
                return {"ok": False, "error": "no_leader", "shard": shard}
            keys = ", ".join(w.keys())
            relation = self._my_relation_to(shard)
            if leader == self.node_id:
                self.events.emit("routing", f"WRITE [{keys}] → all in {shard} (consistent hashing); "
                                            f"{relation} → single-shard commit via Raft")
            else:
                self.events.emit("routing", f"WRITE [{keys}] → all in {shard} (consistent hashing); "
                                            f"{relation}, so I resolved its leader = {leader} → "
                                            f"forwarding the write there")
            res = self._call_ready(leader, "shard_write", {"shard": shard, "writes": w, "read_ts": read_ts})
            res.setdefault("shards", [shard])
            return res

        self.events.emit("txn", f"WRITE spans {len(groups)} shards {sorted(groups)} → these keys do "
                                f"NOT live together, so a single Raft commit can't be atomic; "
                                f"coordinating a 2-phase commit across the shard leaders")
        return self._coordinate_2pc(groups, read_ts)

    # ------------------------------------------------------------------ #
    #  2PC coordinator                                                     #
    # ------------------------------------------------------------------ #

    def _coordinate_2pc(self, groups: dict[str, dict], read_ts: Optional[int]) -> dict:
        txn_id = f"txn-{self.node_id}-{self.hlc.now()}"
        commit_ts = self.hlc.now()
        if read_ts is None:
            read_ts = commit_ts  # blind writes: snapshot = now, so no stale-read conflict

        # Resolve all participant leaders first, then durably record the txn as undecided.
        participants = []
        for shard in groups:
            leader = self._leader_of(shard)
            if not leader:
                return {"ok": False, "error": "no_leader", "shard": shard}
            participants.append([leader, shard])
        self._txn_write(txn_id, "preparing", participants, commit_ts)

        for leader, shard in participants:
            self.events.emit("txn", f"2PC {txn_id}: PREPARE {shard} on leader {leader} "
                                    f"(validate no conflict + stage writes durably, holding a lock)")
            res = self._call_ready(leader, "shard_prepare", {
                "shard": shard, "txn_id": txn_id, "writes": groups[shard],
                "read_ts": read_ts, "commit_ts": commit_ts, "coordinator": self.node_id,
            })
            if not res.get("ok"):
                self.events.emit("txn", f"2PC {txn_id}: {shard} voted NO ({res.get('error')}) → "
                                        f"deciding ABORT for the whole transaction (atomicity)")
                # record the ABORT decision durably and drive it (the sweep retries any that fail)
                self._drive_txn(self._txn_write(txn_id, "aborted", participants, commit_ts))
                return {"ok": False, "error": "prepare_failed", "shard": shard, "detail": res}

        # COMMIT POINT: all voted YES → durably record the decision (fsync) BEFORE committing, so a
        # crash here — or a participant that's down right now — is recoverable by the sweep.
        self.events.emit("txn", f"2PC {txn_id}: all {len(participants)} shards voted YES → durably "
                                f"recorded the COMMIT decision (fsync) → phase 2: COMMIT on every "
                                f"participant (each via its own Raft group)")
        rec = self._txn_write(txn_id, "committing", participants, commit_ts)
        if self._drive_txn(rec):
            return {"ok": True, "commit_ts": commit_ts, "txn_id": txn_id, "shards": list(groups)}
        # a participant was unreachable; it's durably committing and the sweep will finish it
        return {"ok": True, "commit_ts": commit_ts, "txn_id": txn_id, "shards": list(groups),
                "pending_recovery": True}

    def _txn_write(self, txn_id: str, status: str, participants: list, commit_ts: int) -> dict:
        rec = {"txn_id": txn_id, "status": status, "participants": participants,
               "commit_ts": commit_ts, "ts": time.time()}
        self.txnlog.write(txn_id, rec)
        return rec

    def _drive_txn(self, rec: dict) -> bool:
        """Send the decision (commit/abort) to every participant; remove the record once all ack.
        Idempotent: re-sending to an already-resolved participant is a no-op. Returns True if fully
        resolved."""
        method = "shard_commit" if rec["status"] == "committing" else "shard_abort"
        all_ok = True
        for recorded_leader, shard in rec["participants"]:
            # Re-resolve the CURRENT leader: leadership may have moved since prepare. The intent is
            # replicated, so the new leader has it and can apply the commit/abort.
            leader = self._leader_of(shard, retries=1) or recorded_leader
            if not self._call(leader, method, {"shard": shard, "txn_id": rec["txn_id"]}).get("ok"):
                all_ok = False
        if all_ok:
            self.txnlog.remove(rec["txn_id"])
        return all_ok

    # ------------------------------------------------------------------ #
    #  Status (dashboard)                                                  #
    # ------------------------------------------------------------------ #

    def _on_status(self, _p: dict) -> dict:
        shard_status = []
        for s in sorted(self.shards):
            rep = self.shards[s]
            st = rep.status()
            placed = self.placement.get(s, [])
            st["preferred"] = (bool(placed) and placed[0] == self.node_id)
            shard_status.append(st)
        return {"ok": True, "node": self.node_id, "alive": True, "shards": shard_status,
                "members": self.gossip.view()}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in NODES:
        print(f"usage: python node.py <{'|'.join(NODES)}>")
        sys.exit(1)
    node_id = sys.argv[1]
    server = NodeServer(node_id)
    server.start()
    print(f"[{node_id}] up on {server.host}:{server.port}, "
          f"hosting shards: {sorted(server.shards)}", flush=True)
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
