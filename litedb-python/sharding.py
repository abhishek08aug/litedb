"""
sharding.py — Consistent Hashing & Sharding

CONCEPT:
  Sharding splits data across multiple nodes so no single node holds
  everything. Used by: Cassandra, DynamoDB, Redis Cluster, MongoDB.

  Naive sharding (modulo hashing):
    node = hash(key) % num_nodes

    Problem: when you add/remove a node, almost ALL keys move!
    Adding node 4 to a 3-node cluster → 75% of keys reassigned.

  Consistent hashing solves this:
    - Imagine a ring of 2^32 positions (0 to 4,294,967,295)
    - Each node is placed at one or more positions on the ring
    - A key maps to the first node clockwise from hash(key)
    - Adding/removing a node → only ~1/N keys move (N = num nodes)

  Virtual nodes (vnodes):
    Each physical node gets K virtual positions on the ring.
    Benefits:
      - Better load distribution (avoids hot spots)
      - Smoother rebalancing when nodes join/leave
      - Cassandra uses 256 vnodes per node by default

  Replication:
    For fault tolerance, each key is stored on R nodes:
    the primary node + the next R-1 nodes clockwise on the ring.
    Cassandra calls this the "replication factor".

  Ring visualization (4 nodes, 2 vnodes each):

    0 ──── N1_v0 ──── N2_v0 ──── N3_v0 ──── N4_v0
           ↑                                      ↑
    key "alice" hashes here → assigned to N1_v0   |
                                                  |
    4294967295 ──── N4_v1 ──── N3_v1 ──── N2_v1 ──── N1_v1

  Token ranges:
    Each node "owns" the range of the ring between its predecessor
    and itself. Keys in that range are stored on that node.
"""

import hashlib
import bisect
import threading
from dataclasses import dataclass, field
from typing import Optional


# ======================================================================= #
#  Consistent Hash Ring                                                    #
# ======================================================================= #

RING_SIZE = 2 ** 32  # 0 to 4,294,967,295


def _hash(key: str) -> int:
    """Map a string key to a position on the ring [0, 2^32)."""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % RING_SIZE


@dataclass
class VNode:
    """A virtual node on the ring."""
    position: int       # position on the ring [0, 2^32)
    node_id: str        # physical node this vnode belongs to
    vnode_id: int       # which vnode of this physical node

    def __repr__(self):
        return f"VNode({self.node_id}#{self.vnode_id} @{self.position})"


class ConsistentHashRing:
    """
    A consistent hash ring with virtual nodes.

    Operations:
      add_node(node_id)     — add a physical node (creates K vnodes)
      remove_node(node_id)  — remove a physical node
      get_node(key)         — find the primary node for a key
      get_nodes(key, n)     — find the n nodes for a key (for replication)
      get_token_ranges()    — show which node owns which range

    Thread-safe: all mutations hold _lock.
    """

    def __init__(self, vnodes_per_node: int = 150):
        """
        vnodes_per_node: number of virtual nodes per physical node.
          Higher = better distribution but more memory.
          Cassandra default: 256
          We use 150 for demo clarity.
        """
        self.vnodes_per_node = vnodes_per_node
        self._ring: list[int] = []           # sorted list of positions
        self._vnodes: dict[int, VNode] = {}  # position → VNode
        self._nodes: set[str] = set()        # physical node IDs
        self._lock = threading.RLock()

    def add_node(self, node_id: str) -> list[int]:
        """
        Add a physical node to the ring.
        Creates vnodes_per_node virtual nodes at deterministic positions.
        Returns list of positions assigned to this node.
        """
        with self._lock:
            if node_id in self._nodes:
                raise ValueError(f"Node {node_id!r} already in ring")

            positions = []
            for i in range(self.vnodes_per_node):
                # Deterministic position: hash of "node_id:vnode_index"
                pos = _hash(f"{node_id}:vnode:{i}")
                vnode = VNode(position=pos, node_id=node_id, vnode_id=i)
                self._vnodes[pos] = vnode
                bisect.insort(self._ring, pos)
                positions.append(pos)

            self._nodes.add(node_id)
            return positions

    def remove_node(self, node_id: str) -> int:
        """
        Remove a physical node from the ring.
        Returns number of vnodes removed.
        """
        with self._lock:
            if node_id not in self._nodes:
                raise ValueError(f"Node {node_id!r} not in ring")

            removed = 0
            positions_to_remove = [
                pos for pos, vn in self._vnodes.items()
                if vn.node_id == node_id
            ]
            for pos in positions_to_remove:
                del self._vnodes[pos]
                idx = bisect.bisect_left(self._ring, pos)
                if idx < len(self._ring) and self._ring[idx] == pos:
                    self._ring.pop(idx)
                removed += 1

            self._nodes.discard(node_id)
            return removed

    def get_node(self, key: str) -> Optional[str]:
        """
        Find the primary node responsible for a key.
        Returns the node_id of the first node clockwise from hash(key).
        """
        with self._lock:
            if not self._ring:
                return None
            pos = _hash(key)
            idx = bisect.bisect_right(self._ring, pos) % len(self._ring)
            return self._vnodes[self._ring[idx]].node_id

    def get_nodes(self, key: str, n: int) -> list[str]:
        """
        Find the n distinct physical nodes responsible for a key.
        Used for replication: primary + (n-1) replicas.

        Walks clockwise from hash(key), collecting distinct node IDs.
        """
        with self._lock:
            if not self._ring:
                return []
            n = min(n, len(self._nodes))
            pos = _hash(key)
            idx = bisect.bisect_right(self._ring, pos) % len(self._ring)

            nodes = []
            seen = set()
            for _ in range(len(self._ring)):
                vnode = self._vnodes[self._ring[idx]]
                if vnode.node_id not in seen:
                    nodes.append(vnode.node_id)
                    seen.add(vnode.node_id)
                    if len(nodes) == n:
                        break
                idx = (idx + 1) % len(self._ring)

            return nodes

    def get_token_ranges(self) -> list[dict]:
        """
        Return the token range owned by each physical node.
        A node owns the range (prev_position, its_position].
        """
        with self._lock:
            if not self._ring:
                return []

            ranges = []
            for i, pos in enumerate(self._ring):
                prev_pos = self._ring[i - 1] if i > 0 else 0
                node_id = self._vnodes[pos].node_id
                ranges.append({
                    "node": node_id,
                    "start": prev_pos,
                    "end": pos,
                    "size": pos - prev_pos,
                })
            return ranges

    def load_distribution(self) -> dict[str, float]:
        """
        Calculate what percentage of the ring each node owns.
        Ideal: 100% / num_nodes per node.
        """
        with self._lock:
            if not self._ring or not self._nodes:
                return {}

            node_sizes: dict[str, int] = {n: 0 for n in self._nodes}
            ranges = self.get_token_ranges()
            total = sum(r["size"] for r in ranges)

            for r in ranges:
                node_sizes[r["node"]] += r["size"]

            if total == 0:
                return {}

            return {
                node: (size / total) * 100
                for node, size in node_sizes.items()
            }

    def stats(self) -> dict:
        with self._lock:
            return {
                "nodes": len(self._nodes),
                "vnodes": len(self._ring),
                "vnodes_per_node": self.vnodes_per_node,
                "ring_size": RING_SIZE,
            }

    @property
    def nodes(self) -> set[str]:
        with self._lock:
            return set(self._nodes)


# ======================================================================= #
#  Shard Router                                                            #
# ======================================================================= #

class ShardedStore:
    """
    A sharded key-value store using consistent hashing.

    Each shard is an independent dict (in production: a separate LSMEngine).
    The router uses the consistent hash ring to decide which shard owns a key.

    Supports:
      - set(key, value)     — write to correct shard
      - get(key)            — read from correct shard
      - delete(key)         — delete from correct shard
      - add_node(node_id)   — add a shard + rebalance keys
      - remove_node(node_id)— remove a shard + rebalance keys
    """

    def __init__(self, replication_factor: int = 1, vnodes_per_node: int = 150):
        self.ring = ConsistentHashRing(vnodes_per_node)
        self.replication_factor = replication_factor
        self._shards: dict[str, dict[str, str]] = {}  # node_id → {key: value}
        self._lock = threading.RLock()

    def add_node(self, node_id: str) -> dict:
        """
        Add a new node to the cluster.
        Rebalances: moves keys from existing nodes to the new node.
        """
        with self._lock:
            self._shards[node_id] = {}
            self.ring.add_node(node_id)

            # Rebalance: for each key in all shards, check if it should move
            moved = 0
            for existing_node in list(self._shards.keys()):
                if existing_node == node_id:
                    continue
                keys_to_move = []
                for key in list(self._shards[existing_node].keys()):
                    new_primary = self.ring.get_node(key)
                    if new_primary == node_id:
                        keys_to_move.append(key)

                for key in keys_to_move:
                    val = self._shards[existing_node].pop(key)
                    self._shards[node_id][key] = val
                    moved += 1

            return {"node": node_id, "keys_moved": moved}

    def remove_node(self, node_id: str) -> dict:
        """
        Remove a node from the cluster.
        Rebalances: moves its keys to their new owners.
        """
        with self._lock:
            if node_id not in self._shards:
                raise ValueError(f"Node {node_id!r} not found")

            # Move all keys from this node to their new owners
            keys_to_move = dict(self._shards[node_id])
            self.ring.remove_node(node_id)
            del self._shards[node_id]

            moved = 0
            for key, val in keys_to_move.items():
                new_node = self.ring.get_node(key)
                if new_node:
                    self._shards[new_node][key] = val
                    moved += 1

            return {"node": node_id, "keys_moved": moved}

    def set(self, key: str, value: str) -> str:
        """Write key to its primary shard (+ replicas if RF > 1)."""
        with self._lock:
            nodes = self.ring.get_nodes(key, self.replication_factor)
            if not nodes:
                raise RuntimeError("No nodes available")
            for node in nodes:
                self._shards[node][key] = value
            return nodes[0]  # primary node

    def get(self, key: str) -> Optional[str]:
        """Read key from its primary shard."""
        with self._lock:
            node = self.ring.get_node(key)
            if not node:
                return None
            return self._shards[node].get(key)

    def delete(self, key: str) -> bool:
        """Delete key from all replica shards."""
        with self._lock:
            nodes = self.ring.get_nodes(key, self.replication_factor)
            found = False
            for node in nodes:
                if key in self._shards[node]:
                    del self._shards[node][key]
                    found = True
            return found

    def shard_sizes(self) -> dict[str, int]:
        """Return number of keys per shard."""
        with self._lock:
            return {node: len(shard) for node, shard in self._shards.items()}

    def total_keys(self) -> int:
        with self._lock:
            # Count unique keys (with replication, same key may be on multiple shards)
            all_keys: set[str] = set()
            for shard in self._shards.values():
                all_keys.update(shard.keys())
            return len(all_keys)


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

if __name__ == "__main__":
    print("=" * 60)
    print("CONSISTENT HASHING & SHARDING DEMO")
    print("=" * 60)

    # ------------------------------------------------------------------ #
    # Part 1: Naive modulo hashing — the problem                         #
    # ------------------------------------------------------------------ #
    print("\n[Part 1] Naive modulo hashing — the problem")
    print("  3 nodes, hash(key) % 3:")

    def naive_node(key: str, n: int) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % n

    keys = [f"user:{i}" for i in range(12)]
    assignments_3 = {k: naive_node(k, 3) for k in keys}
    assignments_4 = {k: naive_node(k, 4) for k in keys}

    moved = sum(1 for k in keys if assignments_3[k] != assignments_4[k])
    print(f"  Keys with 3 nodes: {assignments_3}")
    print(f"  Keys with 4 nodes: {assignments_4}")
    print(f"  Keys that MOVED when adding node 4: {moved}/{len(keys)} = {moved/len(keys)*100:.0f}%")
    print(f"  ← This is terrible! Consistent hashing fixes this.")

    # ------------------------------------------------------------------ #
    # Part 2: Consistent hash ring                                        #
    # ------------------------------------------------------------------ #
    print("\n[Part 2] Consistent hash ring — basic operations")
    ring = ConsistentHashRing(vnodes_per_node=50)

    ring.add_node("node-A")
    ring.add_node("node-B")
    ring.add_node("node-C")

    print(f"  Ring stats: {ring.stats()}")

    # Show key assignments
    test_keys = ["alice", "bob", "carol", "dave", "eve", "frank",
                 "grace", "henry", "iris", "jack"]
    print("\n  Key → Node assignments:")
    for key in test_keys:
        node = ring.get_node(key)
        pos = _hash(key)
        print(f"    {key!r:8} (hash={pos:10d}) → {node}")

    # ------------------------------------------------------------------ #
    # Part 3: Load distribution with virtual nodes                       #
    # ------------------------------------------------------------------ #
    print("\n[Part 3] Load distribution (% of ring owned per node)")
    dist = ring.load_distribution()
    ideal = 100.0 / len(ring.nodes)
    print(f"  Ideal distribution: {ideal:.1f}% per node")
    for node, pct in sorted(dist.items()):
        bar = "█" * int(pct / 2)
        print(f"  {node}: {pct:5.1f}% {bar}")

    # ------------------------------------------------------------------ #
    # Part 4: Adding a node — minimal key movement                       #
    # ------------------------------------------------------------------ #
    print("\n[Part 4] Adding node-D — how many keys move?")
    assignments_before = {k: ring.get_node(k) for k in test_keys}
    ring.add_node("node-D")
    assignments_after = {k: ring.get_node(k) for k in test_keys}

    moved_keys = [k for k in test_keys if assignments_before[k] != assignments_after[k]]
    print(f"  Keys that moved: {moved_keys}")
    print(f"  Keys moved: {len(moved_keys)}/{len(test_keys)} = {len(moved_keys)/len(test_keys)*100:.0f}%")
    print(f"  Expected ~25% (1/4 of keys) — consistent hashing!")

    dist = ring.load_distribution()
    print(f"  New distribution: { {n: f'{p:.1f}%' for n, p in sorted(dist.items())} }")

    # ------------------------------------------------------------------ #
    # Part 5: Removing a node                                             #
    # ------------------------------------------------------------------ #
    print("\n[Part 5] Removing node-B — keys move to neighbors only")
    assignments_before = {k: ring.get_node(k) for k in test_keys}
    ring.remove_node("node-B")
    assignments_after = {k: ring.get_node(k) for k in test_keys}

    for k in test_keys:
        if assignments_before[k] != assignments_after[k]:
            print(f"  {k!r}: {assignments_before[k]} → {assignments_after[k]}")

    # ------------------------------------------------------------------ #
    # Part 6: Replication factor                                          #
    # ------------------------------------------------------------------ #
    print("\n[Part 6] Replication factor (RF=2)")
    ring2 = ConsistentHashRing(vnodes_per_node=50)
    for n in ["node-1", "node-2", "node-3"]:
        ring2.add_node(n)

    print("  Key → [primary, replica] (RF=2):")
    for key in ["alice", "bob", "carol", "dave", "eve"]:
        nodes = ring2.get_nodes(key, 2)
        print(f"    {key!r:8} → {nodes}")

    # ------------------------------------------------------------------ #
    # Part 7: ShardedStore — full key-value store with rebalancing       #
    # ------------------------------------------------------------------ #
    print("\n[Part 7] ShardedStore — full sharded KV store")
    store = ShardedStore(replication_factor=1, vnodes_per_node=100)

    # Start with 3 nodes
    for n in ["shard-1", "shard-2", "shard-3"]:
        store.add_node(n)

    # Write 100 keys
    for i in range(100):
        store.set(f"key:{i:03d}", f"value_{i}")

    print(f"  After writing 100 keys to 3 shards:")
    sizes = store.shard_sizes()
    for shard, count in sorted(sizes.items()):
        bar = "█" * (count // 2)
        print(f"    {shard}: {count:3d} keys  {bar}")

    # Add a 4th shard — watch keys rebalance
    result = store.add_node("shard-4")
    print(f"\n  Added shard-4: {result['keys_moved']} keys moved")
    sizes = store.shard_sizes()
    for shard, count in sorted(sizes.items()):
        bar = "█" * (count // 2)
        print(f"    {shard}: {count:3d} keys  {bar}")

    # Verify all keys still readable
    missing = [f"key:{i:03d}" for i in range(100) if store.get(f"key:{i:03d}") is None]
    print(f"\n  Keys missing after rebalance: {len(missing)}  ← should be 0")

    # Remove a shard
    result = store.remove_node("shard-2")
    print(f"\n  Removed shard-2: {result['keys_moved']} keys moved")
    sizes = store.shard_sizes()
    for shard, count in sorted(sizes.items()):
        bar = "█" * (count // 2)
        print(f"    {shard}: {count:3d} keys  {bar}")

    missing = [f"key:{i:03d}" for i in range(100) if store.get(f"key:{i:03d}") is None]
    print(f"  Keys missing after removal: {len(missing)}  ← should be 0")

    print("\n[Done] Consistent hashing demo complete.")
    print("\nKey insights:")
    print("  1. Naive modulo: adding 1 node moves ~75% of keys — catastrophic")
    print("  2. Consistent hashing: adding 1 node moves only ~1/N keys")
    print("  3. Virtual nodes give better load distribution (avoid hot spots)")
    print("  4. Replication factor R: each key stored on R consecutive nodes")
    print("  5. This is exactly how Cassandra, DynamoDB, Redis Cluster work")
    print("  6. Token ranges: each node owns a contiguous arc of the ring")