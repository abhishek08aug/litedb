"""
cluster_client.py — a client for the distributed database.

A client can contact ANY node: the node routes single-key ops to the owning shard's leader and
coordinates cross-shard transactions itself. So the client just needs to reach one live node; it
shuffles the node list and retries the next on failure (which is also how it rides through a node
being killed mid-demo).
"""

import random
from typing import Optional

from cluster_config import NODES
from rpc import RPCClient


class ClusterClient:
    def __init__(self, timeout: float = 4.0):
        self._rpc = RPCClient(timeout=timeout)
        self._nodes = list(NODES.keys())

    def _call(self, method: str, payload: dict) -> dict:
        order = self._nodes[:]
        random.shuffle(order)
        last = {"ok": False, "error": "all_nodes_unreachable"}
        for nid in order:
            host, port = NODES[nid]
            resp = self._rpc.call(host, port, method, payload)
            if resp.get("ok"):
                return resp["result"]
            last = {"ok": False, "error": resp.get("error", "rpc_failed")}
        return last

    # ---- KV API -----------------------------------------------------------

    def put(self, key: str, value: str) -> dict:
        return self._call("put", {"key": key, "value": value})

    def delete(self, key: str) -> dict:
        return self._call("put", {"key": key, "value": None})

    def get(self, key: str, read_ts: Optional[int] = None) -> Optional[str]:
        res = self._call("get", {"key": key, "read_ts": read_ts})
        return res.get("value") if res.get("ok") else None

    def get_full(self, key: str, read_ts: Optional[int] = None) -> dict:
        return self._call("get", {"key": key, "read_ts": read_ts})

    # ---- transactions -----------------------------------------------------

    def begin(self) -> Optional[int]:
        res = self._call("begin", {})
        return res.get("read_ts") if res.get("ok") else None

    def txn(self, writes: dict, read_ts: Optional[int] = None) -> dict:
        """Atomically apply {key: value_or_None}. Spans shards via 2PC when needed."""
        return self._call("txn", {"writes": writes, "read_ts": read_ts})

    # ---- introspection ----------------------------------------------------

    def status(self) -> list[dict]:
        out = []
        for nid in self._nodes:
            host, port = NODES[nid]
            resp = self._rpc.call(host, port, "status", {})
            out.append(resp["result"] if resp.get("ok") else {"node": nid, "alive": False})
        return out
