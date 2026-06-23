"""
gossip_smoke.py — prove gossip-based DISCOVERY and failure detection over real TCP.

Scenario (no Raft, just the gossip layer):
  1. node-A starts as THE seed (it has no seeds of its own).
  2. node-B and node-C each start knowing ONLY node-A's address — not each other, not the pool.
  3. Within a few gossip rounds every node has discovered all three (transitive: A tells C about B).
  4. Kill node-C; node-A and node-B locally age it out and mark it DEAD (heartbeat stops advancing).

This is the single-machine demonstration that a node needs just ONE seed to join, exactly as a real
multi-node cluster bootstraps. Run:  python gossip_smoke.py
"""

import _loader  # noqa: F401
from gossip import ALIVE, DEAD, Gossip
from rpc import RPCClient, RPCServer

BASE = 7400
NAMES = ["node-A", "node-B", "node-C"]


def make_node(node_id: str, port: int, seeds: list):
    client = RPCClient(timeout=1.0)

    def send(host: str, p: int, payload: dict):
        resp = client.call(host, p, "gossip", payload, timeout=1.0)
        return resp["result"] if resp.get("ok") else None

    g = Gossip(node_id, ["127.0.0.1", port], seeds, send_fn=send,
               gossip_interval=0.3, suspect_after=1.0, dead_after=2.0, fanout=2)
    server = RPCServer("127.0.0.1", port, {"gossip": lambda pl: g.handle(pl)})
    return g, server, client


def _wait(predicate, tries: int = 60, delay: float = 0.25) -> bool:
    import time
    for _ in range(tries):
        if predicate():
            return True
        time.sleep(delay)
    return False


def main() -> None:
    # node-A = seed; B and C bootstrap from A ONLY (one seed address, nothing else)
    specs = [
        ("node-A", BASE + 0, []),
        ("node-B", BASE + 1, [["127.0.0.1", BASE + 0]]),
        ("node-C", BASE + 2, [["127.0.0.1", BASE + 0]]),
    ]
    nodes = {}
    for nid, port, seeds in specs:
        g, s, c = make_node(nid, port, seeds)
        s.start()
        g.start()
        nodes[nid] = (g, s, c)

    def converged() -> bool:
        for g, _, _ in nodes.values():
            v = g.view()
            if set(v) != set(NAMES) or not all(e["state"] == ALIVE for e in v.values()):
                return False
        return True

    assert _wait(converged), "gossip did not converge: " + \
        str({nid: sorted(g.view()) for nid, (g, _, _) in nodes.items()})
    print("DISCOVERY: all 3 nodes found each other from a single seed (node-A) —")
    for nid, (g, _, _) in nodes.items():
        seeded = "seed" if nid == "node-A" else "seed=node-A only"
        print(f"  {nid:8} ({seeded:16}) knows: {sorted(g.view())}")

    # kill node-C; A and B should locally mark it dead (its heartbeat stops advancing)
    gC, sC, cC = nodes["node-C"]
    gC.stop()
    sC.stop()
    cC.close()

    def c_dead() -> bool:
        return all(nodes[o][0].view().get("node-C", {}).get("state") == DEAD
                   for o in ("node-A", "node-B"))

    assert _wait(c_dead), "failure detector did not mark node-C dead"
    print("\nFAILURE DETECTION: node-C killed → A and B aged its heartbeat out and marked it DEAD")

    for nid in ("node-A", "node-B"):
        nodes[nid][1].stop()
        nodes[nid][2].close()
    print("\nGOSSIP SMOKE PASSED")


if __name__ == "__main__":
    main()
