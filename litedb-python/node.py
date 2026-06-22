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

import sys
import threading
import time
from typing import Optional

import _loader  # noqa: F401  (puts this dir on sys.path)
from cluster_config import NODES, make_partitioner, node_data_dir
from events import EventLog
from hlc import HLC
from raft_node import RPC_TIMEOUT
from rpc import RPCClient, RPCServer
from shard_replica import ShardReplica
from txn_log import TxnLog

FORWARD_TIMEOUT = 3.0


class NodeServer:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.host, self.port = NODES[node_id]
        self.partitioner = make_partitioner()
        self.hlc = HLC()
        self.events = EventLog()
        self.client = RPCClient(timeout=RPC_TIMEOUT)
        data_dir = node_data_dir(node_id)
        self.txnlog = TxnLog(data_dir)

        self.shards: dict[str, ShardReplica] = {}
        for s in self.partitioner.shards_on(node_id):
            peers = [n for n in self.partitioner.replicas(s) if n != node_id]
            self.shards[s] = ShardReplica(
                node_id=node_id, shard_id=s, peers=peers, send_fn=self._make_send(s),
                data_dir=data_dir, hlc=self.hlc,
                preferred=(self.partitioner.preferred_leader(s) == node_id),
                on_event=self.events.emit,
            )

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
            "shard_leader": self._on_shard_leader,
            # internal shard ops (coordinator -> shard leader); guarded for shards not hosted here
            "shard_write": lambda p: self._with_shard(
                p, lambda r: r.commit_write(p["writes"], p.get("read_ts"))),
            "shard_get": self._on_shard_get,
            "shard_prepare": lambda p: self._with_shard(
                p, lambda r: r.prepare(p["txn_id"], p["writes"], p["read_ts"], p["commit_ts"])),
            "shard_commit": lambda p: self._with_shard(p, lambda r: r.commit_prepared(p["txn_id"])),
            "shard_abort": lambda p: self._with_shard(p, lambda r: r.abort_prepared(p["txn_id"])),
        }
        self.server = RPCServer(self.host, self.port, self.handlers)

    def _make_send(self, shard_id: str):
        def send(peer_node: str, kind: str, payload: dict) -> dict:
            host, port = NODES[peer_node]
            msg = dict(payload)
            msg["shard"] = shard_id  # tell the peer which group this RPC is for
            return self.client.call(host, port, kind, msg, timeout=RPC_TIMEOUT)
        return send

    def start(self) -> None:
        self.server.start()
        for rep in self.shards.values():
            rep.start()
        threading.Thread(target=self._recovery_loop, daemon=True).start()

    def _recovery_loop(self) -> None:
        """On restart, finish any 2PC this node was coordinating when it died."""
        time.sleep(2.0)  # let the RPC server + peers come up and elect leaders
        for rec in self.txnlog.pending():
            txn_id = rec["txn_id"]
            commit = rec["status"] == "committing"
            verb = "COMMIT" if commit else "ABORT"
            self.events.emit("txn", f"RECOVERY: in-doubt txn {txn_id} was '{rec['status']}' when I "
                                    f"crashed → resolving as {verb} on restart")
            for leader, shard in rec["participants"]:
                self._call(leader, "shard_commit" if commit else "shard_abort",
                           {"shard": shard, "txn_id": txn_id})
            self.txnlog.remove(txn_id)

    def stop(self) -> None:
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
        host, port = NODES[node]
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

    def _leader_of(self, shard: str, retries: int = 15) -> Optional[str]:
        """Resolve a shard's leader whether or not THIS node hosts the shard. If it does, read the
        local Raft view; otherwise ask the shard's replica nodes (works for any replication factor)."""
        for _ in range(retries):
            if shard in self.shards:
                lid = self.shards[shard].leader_id()
                if lid:
                    return lid
            else:
                for rep_node in self.partitioner.replicas(shard):
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
        return self._call(leader, "shard_get", {"shard": shard, "key": key, "read_ts": p.get("read_ts")})

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
            res = self._call(leader, "shard_write", {"shard": shard, "writes": w, "read_ts": read_ts})
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
        self.txnlog.write(txn_id, {"txn_id": txn_id, "status": "preparing",
                                   "participants": participants, "commit_ts": commit_ts})

        prepared: list[tuple[str, str]] = []
        for leader, shard in participants:
            self.events.emit("txn", f"2PC {txn_id}: PREPARE {shard} on leader {leader} "
                                    f"(validate no conflict + stage writes, holding a lock)")
            res = self._call(leader, "shard_prepare", {
                "shard": shard, "txn_id": txn_id, "writes": groups[shard],
                "read_ts": read_ts, "commit_ts": commit_ts,
            })
            if not res.get("ok"):
                self.events.emit("txn", f"2PC {txn_id}: {shard} voted NO ({res.get('error')}) → "
                                        f"ABORTING the whole transaction (atomicity)")
                self._abort_all(prepared, txn_id)
                self.txnlog.remove(txn_id)
                return {"ok": False, "error": "prepare_failed", "shard": shard, "detail": res}
            prepared.append((leader, shard))

        # COMMIT POINT: all voted YES → durably record the decision BEFORE committing, so a crash
        # here is recoverable (the restart sweep will re-send the commits).
        self.txnlog.write(txn_id, {"txn_id": txn_id, "status": "committing",
                                   "participants": participants, "commit_ts": commit_ts})
        self.events.emit("txn", f"2PC {txn_id}: all {len(prepared)} shards voted YES → durably "
                                f"recorded the COMMIT decision (fsync) → phase 2: COMMIT on every "
                                f"participant (each via its own Raft group)")
        for leader, shard in prepared:
            self._call(leader, "shard_commit", {"shard": shard, "txn_id": txn_id})
        self.txnlog.remove(txn_id)
        return {"ok": True, "commit_ts": commit_ts, "txn_id": txn_id, "shards": list(groups.keys())}

    def _abort_all(self, prepared: list[tuple[str, str]], txn_id: str) -> None:
        for leader, shard in prepared:
            self._call(leader, "shard_abort", {"shard": shard, "txn_id": txn_id})

    # ------------------------------------------------------------------ #
    #  Status (dashboard)                                                  #
    # ------------------------------------------------------------------ #

    def _on_status(self, _p: dict) -> dict:
        shard_status = []
        for s in sorted(self.shards):
            rep = self.shards[s]
            st = rep.status()
            st["preferred"] = (self.partitioner.preferred_leader(s) == self.node_id)
            shard_status.append(st)
        return {"ok": True, "node": self.node_id, "alive": True, "shards": shard_status}


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
