"""
gossip.py — SWIM/Cassandra-style gossip for node DISCOVERY + weak liveness.

This is the decentralized, eventually-consistent membership substrate that real clusters use instead
of a static address book (Cassandra's gossiper, Serf/Consul's SWIM, CockroachDB's gossip network).
It is deliberately NOT Raft: Raft gives strong consistency for a KNOWN group; gossip discovers and
disseminates WHO the group is — cheaply, leaderlessly, and partition-tolerantly.

What it buys us (removing the single-machine assumption that everyone reads the same static pool):

  * Discovery — a node boots knowing only a SEED address, not the full node list. It gossips to the
    seed, learns the seed's id + every other member from the reply, and within a few rounds the whole
    graph converges. New nodes are discovered; departed nodes age out.
  * Weak liveness — alive/suspect/dead, derived LOCALLY from how recently a member's heartbeat
    advanced (the Cassandra split: gossip carries heartbeats; the failure detector is local).

Protocol — one RPC method, "gossip", anti-entropy push-pull:

    request payload: {"from": <node_id>, "members": {node_id: entry, ...}}
    response result: {"members": {node_id: entry, ...}}     # receiver's merged view back to the sender
    entry          : {"addr": [host, port], "generation": int, "heartbeat": int}

Merge rule (per node_id): adopt the entry with the higher (generation, heartbeat). `generation` is the
sender's startup timestamp, so a RESTARTED node outranks its own stale heartbeat and is re-adopted as
alive automatically — no explicit refutation needed. `last_update` is local-only (never sent): it is
the wall-clock instant we last saw a member's (generation, heartbeat) increase, and the failure
detector ages members off it.
"""

import random
import threading
import time
from typing import Callable, Optional

ALIVE, SUSPECT, DEAD = "alive", "suspect", "dead"

# send_fn(host, port, payload) -> response dict (the "gossip" result) or None on failure
SendFn = Callable[[str, int, dict], Optional[dict]]


class Gossip:
    def __init__(self, node_id: str, addr: list, seeds: list, *, send_fn: SendFn,
                 gossip_interval: float = 1.0, suspect_after: float = 3.0, dead_after: float = 6.0,
                 fanout: int = 2, on_event: Optional[Callable[[str], None]] = None):
        self.node_id = node_id
        self.addr = [addr[0], int(addr[1])]
        self._send = send_fn
        self.interval = gossip_interval
        self.suspect_after = suspect_after
        self.dead_after = dead_after
        self.fanout = fanout
        self._emit = on_event or (lambda m: None)

        self._lock = threading.RLock()
        self._running = False
        self._heartbeat = 0
        # generation = startup wall-clock: monotonic across restarts, so a returning node's fresh
        # gossip (gen↑, heartbeat=0) still outranks the stale (old gen, high heartbeat) others remember.
        self._generation = int(time.time())
        now = time.time()
        # membership table: node_id -> {addr, generation, heartbeat, last_update}
        self.members: dict[str, dict] = {
            node_id: {"addr": list(self.addr), "generation": self._generation,
                      "heartbeat": 0, "last_update": now}
        }
        # seeds are bare [host, port] addresses (we don't know their ids until they reply)
        self._seeds = [[s[0], int(s[1])] for s in seeds if [s[0], int(s[1])] != self.addr]

    # ---- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._running = True
        threading.Thread(target=self._loop, daemon=True, name=f"gossip-{self.node_id}").start()

    def stop(self) -> None:
        self._running = False

    # ---- inbound (server side of the "gossip" RPC) ------------------------

    def handle(self, payload: dict) -> dict:
        self._merge(payload.get("members") or {})
        with self._lock:
            return {"members": self._digest()}

    # ---- outbound loop ----------------------------------------------------

    def _loop(self) -> None:
        while self._running:
            time.sleep(self.interval)
            if not self._running:
                break
            with self._lock:
                self._heartbeat += 1
                me = self.members[self.node_id]
                me["heartbeat"] = self._heartbeat
                me["last_update"] = time.time()
                targets = self._pick_targets()
                digest = self._digest()
            for host, port in targets:
                resp = self._send(host, port, {"from": self.node_id, "members": digest})
                if resp and "members" in resp:
                    self._merge(resp["members"])

    def _pick_targets(self) -> list:
        """Up to `fanout` random non-dead peers, plus any seed we haven't learned by address yet (so
        bootstrap always makes progress; once a seed is in the table it just joins the random pool)."""
        now = time.time()
        peers = [list(m["addr"]) for nid, m in self.members.items()
                 if nid != self.node_id and self._age(nid, now) <= self.dead_after]
        random.shuffle(peers)
        targets = peers[: self.fanout]
        known = {(m["addr"][0], int(m["addr"][1])) for m in self.members.values()}
        for s in self._seeds:
            if (s[0], s[1]) not in known and s not in targets:
                targets.append(s)
        return [[t[0], int(t[1])] for t in targets]

    # ---- merge / failure detection ---------------------------------------

    def _merge(self, incoming: dict) -> None:
        now = time.time()
        with self._lock:
            for nid, e in incoming.items():
                if nid == self.node_id:
                    continue  # we are the sole authority about ourselves
                try:
                    gen, hb = int(e["generation"]), int(e["heartbeat"])
                    addr = [e["addr"][0], int(e["addr"][1])]
                except (KeyError, TypeError, ValueError, IndexError):
                    continue  # ignore malformed entries rather than crash the gossip round
                cur = self.members.get(nid)
                if cur is None:
                    self.members[nid] = {"addr": addr, "generation": gen,
                                         "heartbeat": hb, "last_update": now}
                    self._emit(f"discovered {nid} at {addr[0]}:{addr[1]} via gossip")
                elif (gen, hb) > (cur["generation"], cur["heartbeat"]):
                    was_dead = self._age(nid, now) > self.dead_after
                    cur.update(addr=addr, generation=gen, heartbeat=hb, last_update=now)
                    if was_dead:
                        self._emit(f"{nid} is alive again (rejoined) — re-adopted via gossip")

    def _digest(self) -> dict:
        # last_update is LOCAL state (when WE last saw progress); never put it on the wire
        return {nid: {"addr": list(m["addr"]), "generation": m["generation"],
                      "heartbeat": m["heartbeat"]}
                for nid, m in self.members.items()}

    def _age(self, nid: str, now: float) -> float:
        return now - self.members[nid]["last_update"]

    def _state(self, nid: str, now: float) -> str:
        if nid == self.node_id:
            return ALIVE
        age = self._age(nid, now)
        if age > self.dead_after:
            return DEAD
        if age > self.suspect_after:
            return SUSPECT
        return ALIVE

    # ---- views (routing, status, dashboard) -------------------------------

    def addr_of(self, node_id: str) -> Optional[list]:
        with self._lock:
            m = self.members.get(node_id)
            return list(m["addr"]) if m else None

    def alive_nodes(self) -> list:
        now = time.time()
        with self._lock:
            return sorted(nid for nid in self.members if self._state(nid, now) != DEAD)

    def view(self) -> dict:
        """Membership snapshot for status/dashboard: node_id -> {addr, state, heartbeat, generation}."""
        now = time.time()
        with self._lock:
            return {nid: {"addr": list(m["addr"]), "state": self._state(nid, now),
                          "heartbeat": m["heartbeat"], "generation": m["generation"]}
                    for nid, m in sorted(self.members.items())}
