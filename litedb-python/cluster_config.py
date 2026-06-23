"""
cluster_config.py — shared static topology for the single-machine cluster.

Every node process and the client import this, so they all compute routing identically (no central
router). Three nodes, six shards, replication factor 3 → every node holds a replica of every shard,
and preferred leadership rotates across nodes (node-1 leads shards 0,3; node-2 leads 1,4; etc.).
"""

import os

from partition import Partitioner

# Topology is env-configurable: LITEDB_CLUSTER_NODES (initial nodes), LITEDB_CLUSTER_SHARDS, and
# LITEDB_CLUSTER_RF. Defaults: 3 nodes, 6 shards, RF 3.
_INITIAL_NODE_COUNT = int(os.environ.get("LITEDB_CLUSTER_NODES", "3"))
_SHARD_COUNT = int(os.environ.get("LITEDB_CLUSTER_SHARDS", "6"))
_POOL_SIZE = max(_INITIAL_NODE_COUNT + 3, 6)  # spare nodes (in the address book) to add at runtime

# Address book: the POOL of possible nodes (so any node can reach any other). The cluster starts
# with INITIAL_NODES active; more can be added at runtime (up to this pool), or active ones removed.
NODES: dict[str, list] = {f"node-{i}": ["127.0.0.1", 7000 + i] for i in range(1, _POOL_SIZE + 1)}

INITIAL_NODES: list[str] = [f"node-{i}" for i in range(1, _INITIAL_NODE_COUNT + 1)]

SHARDS: list[str] = [f"shard-{i}" for i in range(_SHARD_COUNT)]

DASHBOARD_PORT = 7080

# Replication factor is configurable so you can run e.g. 3 instances with RF 2 (each shard lives on
# only 2 of the 3 nodes — routing then has to forward across nodes that don't host a shard).
REPLICATION_FACTOR = int(os.environ.get("LITEDB_CLUSTER_RF", "3"))

DATA_ROOT = os.environ.get("LITEDB_CLUSTER_DATA",
                           os.path.join(os.path.dirname(os.path.abspath(__file__)), "_cluster_data"))


def make_partitioner(nodes: list[str] | None = None) -> Partitioner:
    return Partitioner(SHARDS, nodes or INITIAL_NODES, replication_factor=REPLICATION_FACTOR)


def compute_placement(nodes: list[str], rf: int | None = None) -> dict[str, list[str]]:
    """Even round-robin assignment of each shard to `rf` of the active `nodes` — the balancer's
    target. Recomputing it when the node set changes yields the shards that must move."""
    rf = rf or REPLICATION_FACTOR
    rf = min(rf, len(nodes))
    # round-robin order (NOT sorted): replicas[0] rotates per shard, so preferred leadership — and
    # thus write load — spreads across nodes instead of piling on the alphabetically-first one.
    return {SHARDS[i]: [nodes[(i + j) % len(nodes)] for j in range(rf)]
            for i in range(len(SHARDS))}


def seed_addrs(node_id: str) -> list[list]:
    """Bootstrap contacts for gossip: a SMALL set of well-known addresses (NOT the full pool). A
    joining node only needs to reach ONE of these to discover the whole cluster transitively. This
    is the single-machine stand-in for Cassandra `seeds:` / Consul `retry_join`.

    Override with LITEDB_CLUSTER_SEEDS='host:port,host:port'; default = the first two initial nodes
    (two seeds so losing one doesn't break bootstrap), excluding self."""
    raw = os.environ.get("LITEDB_CLUSTER_SEEDS", "").strip()
    if raw:
        out = []
        for tok in raw.split(","):
            tok = tok.strip()
            if tok:
                host, port = tok.rsplit(":", 1)
                out.append([host, int(port)])
        return out
    return [list(NODES[n]) for n in INITIAL_NODES[:2] if n != node_id]


def node_data_dir(node_id: str) -> str:
    return os.path.join(DATA_ROOT, node_id)
