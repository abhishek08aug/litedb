"""
cluster_config.py — shared static topology for the single-machine cluster.

Every node process and the client import this, so they all compute routing identically (no central
router). Three nodes, six shards, replication factor 3 → every node holds a replica of every shard,
and preferred leadership rotates across nodes (node-1 leads shards 0,3; node-2 leads 1,4; etc.).
"""

import os

from partition import Partitioner

NODES: dict[str, list] = {
    "node-1": ["127.0.0.1", 7001],
    "node-2": ["127.0.0.1", 7002],
    "node-3": ["127.0.0.1", 7003],
}

SHARDS: list[str] = [f"shard-{i}" for i in range(6)]

DASHBOARD_PORT = 7080

# Replication factor is configurable so you can run e.g. 3 instances with RF 2 (each shard lives on
# only 2 of the 3 nodes — routing then has to forward across nodes that don't host a shard).
REPLICATION_FACTOR = int(os.environ.get("JARVIS_CLUSTER_RF", "3"))

DATA_ROOT = os.environ.get("JARVIS_CLUSTER_DATA",
                           os.path.join(os.path.dirname(os.path.abspath(__file__)), "_cluster_data"))


def make_partitioner() -> Partitioner:
    return Partitioner(SHARDS, list(NODES.keys()), replication_factor=REPLICATION_FACTOR)


def node_data_dir(node_id: str) -> str:
    return os.path.join(DATA_ROOT, node_id)
