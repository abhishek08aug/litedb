"""
partition.py — the cluster's partition map: key -> shard, and shard -> replica nodes.

Two layers, as in TiKV/Cassandra:
  - Partitioning: a consistent-hash ring (reused from sharding.py) places the SHARDS on the ring;
    a key hashes to the shard that owns its arc. Consistent hashing means adding/removing a shard
    only remaps ~1/N keys.
  - Placement: each shard is replicated onto `replication_factor` nodes (round-robin from the
    shard's index). With 3 nodes and RF=3 every node holds every shard; the round-robin still
    rotates which node is the *preferred leader*, so leadership spreads across the cluster.

This map is static config shared by every node and the client — all of them compute routing the
same way, so there is no central router to be a bottleneck or a single point of failure.
"""

from sharding import ConsistentHashRing

RING_SIZE = 1 << 32  # the consistent-hash ring spans [0, 2^32)


class Partitioner:
    def __init__(self, shard_ids: list[str], nodes: list[str],
                 replication_factor: int | None = None, vnodes: int = 64):
        self.shard_ids = list(shard_ids)
        self.nodes = list(nodes)
        self.rf = replication_factor or len(nodes)
        if self.rf > len(self.nodes):
            raise ValueError("replication_factor cannot exceed node count")

        self._ring = ConsistentHashRing(vnodes_per_node=vnodes)
        for s in self.shard_ids:
            self._ring.add_node(s)

        # shard -> ordered replica nodes (index 0 is the preferred leader)
        self._placement: dict[str, list[str]] = {}
        for i, s in enumerate(self.shard_ids):
            self._placement[s] = [self.nodes[(i + j) % len(self.nodes)] for j in range(self.rf)]

    def shard_for(self, key: str) -> str:
        shard = self._ring.get_node(key)
        assert shard is not None  # ring is non-empty
        return shard

    def replicas(self, shard_id: str) -> list[str]:
        return self._placement[shard_id]

    def preferred_leader(self, shard_id: str) -> str:
        return self._placement[shard_id][0]

    def shards_on(self, node_id: str) -> list[str]:
        return [s for s in self.shard_ids if node_id in self._placement[s]]

    def shards_for_keys(self, keys: list[str]) -> dict[str, list[str]]:
        """Group keys by their owning shard — the basis for deciding single- vs cross-shard txns."""
        groups: dict[str, list[str]] = {}
        for k in keys:
            groups.setdefault(self.shard_for(k), []).append(k)
        return groups

    # ---- introspection for the dashboard ----------------------------------

    def placement(self) -> list[dict]:
        return [{"shard": s, "preferred": self._placement[s][0], "replicas": self._placement[s]}
                for s in self.shard_ids]

    def ring_arcs(self) -> list[dict]:
        """The consistent-hash ring as colored arcs: each shard owns the segments [prev, pos] of its
        vnodes. Used to draw the ring in the UI."""
        arcs = [{"shard": r["node"], "start": r["start"], "end": r["end"]}
                for r in self._ring.get_token_ranges()]
        if arcs and arcs[-1]["end"] < RING_SIZE:
            # the wrap segment (last vnode .. ring end) belongs to the first vnode's shard
            arcs.append({"shard": arcs[0]["shard"], "start": arcs[-1]["end"], "end": RING_SIZE})
        return arcs
