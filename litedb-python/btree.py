"""
btree.py — B-Tree Storage Engine

CONCEPT:
  The B-Tree is the dominant data structure for disk-based databases.
  Used by: PostgreSQL (heap + B-Tree indexes), MySQL InnoDB, SQLite,
           Oracle, SQL Server, MongoDB (WiredTiger).

  Why B-Tree instead of LSM?
    ┌──────────────────────────────────────────────────────────┐
    │              LSM-Tree        B-Tree                      │
    │  Write speed  ★★★★★          ★★★                        │
    │  Read speed   ★★★            ★★★★★                      │
    │  Space amp.   High (versions) Low (in-place update)      │
    │  Write amp.   Low             High (page splits/merges)  │
    │  Use case     Write-heavy     Read-heavy / mixed          │
    │  Examples     Cassandra,      PostgreSQL, MySQL,          │
    │               RocksDB         SQLite, Oracle              │
    └──────────────────────────────────────────────────────────┘

  B-Tree properties:
    1. Every node has at most 2t-1 keys (t = minimum degree)
    2. Every non-root node has at least t-1 keys
    3. All leaves are at the same depth
    4. A node with k keys has k+1 children (if internal)
    5. Keys within a node are sorted

  Operations:
    Search:  O(log n) — traverse from root, binary search at each node
    Insert:  O(log n) — find leaf, insert, split if overflow
    Delete:  O(log n) — find key, remove, rebalance if underflow
    Scan:    O(k + log n) — find start, traverse leaves

  Node splitting (the key insight):
    When a node is full (2t-1 keys), split it into two nodes of t-1 keys
    and push the median key up to the parent.

    Before split (t=2, max 3 keys):
      [10, 20, 30]  ← full!

    After split:
      Parent gets: 20
      Left child:  [10]
      Right child: [30]

  B+ Tree variant (used by most databases):
    - Internal nodes store only keys (no values) — more keys per page
    - All values stored in leaf nodes
    - Leaf nodes linked in a doubly-linked list → fast range scans
    - We implement B+ Tree for better range scan performance
"""

from __future__ import annotations

import bisect
from typing import Iterator, Optional

# ======================================================================= #
#  B+ Tree Node                                                            #
# ======================================================================= #

class BPlusNode:
    """
    A node in a B+ Tree.

    Leaf node:     keys=[k1,k2,k3], values=[v1,v2,v3], next=<next_leaf>
    Internal node: keys=[k1,k2],    children=[c0,c1,c2]
                   (children[i] contains keys < keys[i])
                   (children[-1] contains keys >= keys[-1])
    """

    def __init__(self, is_leaf: bool = True):
        self.is_leaf = is_leaf
        self.keys: list[str] = []
        self.values: list[str] = []          # only for leaf nodes
        self.children: list[BPlusNode] = []  # only for internal nodes
        self.next: Optional[BPlusNode] = None  # leaf linked list

    def is_full(self, order: int) -> bool:
        return len(self.keys) >= 2 * order - 1

    def __repr__(self):
        kind = "Leaf" if self.is_leaf else "Internal"
        return f"{kind}({self.keys})"


# ======================================================================= #
#  B+ Tree                                                                 #
# ======================================================================= #

class BPlusTree:
    """
    B+ Tree with order t (minimum degree).

    Each node has between t-1 and 2t-1 keys.
    Leaf nodes store (key, value) pairs and are linked for range scans.
    Internal nodes store keys as separators between child subtrees.

    This is a simplified in-memory B+ Tree.
    In production (PostgreSQL, SQLite), nodes map to disk pages (4KB or 8KB).
    """

    def __init__(self, order: int = 3):
        """
        order (t): minimum degree.
          t=2 → 2-3 tree (1-3 keys per node)
          t=3 → 2-5 tree (2-5 keys per node)  ← we use this
          t=100 → ~200 keys per node (typical for 4KB pages with 20-byte keys)
        """
        self.order = order
        self.root = BPlusNode(is_leaf=True)
        self._size = 0  # number of key-value pairs

    # ------------------------------------------------------------------ #
    #  Search                                                              #
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> Optional[str]:
        """
        Search for a key. Returns value or None.
        O(log n) — traverse from root to leaf.
        """
        leaf = self._find_leaf(key)
        idx = bisect.bisect_left(leaf.keys, key)
        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            return leaf.values[idx]
        return None

    def _find_leaf(self, key: str) -> BPlusNode:
        """Traverse from root to the leaf that should contain key."""
        node = self.root
        while not node.is_leaf:
            # Find the child to descend into
            idx = bisect.bisect_right(node.keys, key)
            node = node.children[idx]
        return node

    # ------------------------------------------------------------------ #
    #  Insert                                                              #
    # ------------------------------------------------------------------ #

    def set(self, key: str, value: str) -> None:
        """
        Insert or update a key-value pair.

        Algorithm:
          1. Find the leaf node for this key
          2. If key exists, update value
          3. If leaf is not full, insert directly
          4. If leaf is full, split and propagate up
        """
        # Check if key already exists (update)
        leaf = self._find_leaf(key)
        idx = bisect.bisect_left(leaf.keys, key)
        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            leaf.values[idx] = value  # update existing
            return

        self._size += 1

        # If root is full, split root first
        if self.root.is_full(self.order):
            old_root = self.root
            new_root = BPlusNode(is_leaf=False)
            new_root.children.append(old_root)
            self._split_child(new_root, 0)
            self.root = new_root

        self._insert_non_full(self.root, key, value)

    def _insert_non_full(self, node: BPlusNode, key: str, value: str):
        """Insert into a node that is guaranteed not full."""
        if node.is_leaf:
            # Insert into sorted position
            idx = bisect.bisect_left(node.keys, key)
            node.keys.insert(idx, key)
            node.values.insert(idx, value)
        else:
            # Find child to descend into
            idx = bisect.bisect_right(node.keys, key)
            child = node.children[idx]

            if child.is_full(self.order):
                self._split_child(node, idx)
                # After split, determine which of the two children to use
                if key > node.keys[idx]:
                    idx += 1

            self._insert_non_full(node.children[idx], key, value)

    def _split_child(self, parent: BPlusNode, child_idx: int):
        """
        Split parent.children[child_idx] (which is full) into two nodes.

        For a leaf node:
          - Left keeps first t keys
          - Right gets remaining t-1 keys
          - Median key is COPIED up to parent (B+ Tree: key stays in leaf)

        For an internal node:
          - Left keeps first t-1 keys
          - Right gets last t-1 keys
          - Median key is PUSHED UP to parent (removed from children)
        """
        t = self.order
        child = parent.children[child_idx]

        if child.is_leaf:
            # Split leaf: copy median up, keep in both halves
            mid = t  # split point
            new_leaf = BPlusNode(is_leaf=True)
            new_leaf.keys = child.keys[mid:]
            new_leaf.values = child.values[mid:]
            child.keys = child.keys[:mid]
            child.values = child.values[:mid]

            # Link leaves
            new_leaf.next = child.next
            child.next = new_leaf

            # Push median key up to parent
            median_key = new_leaf.keys[0]
            parent.keys.insert(child_idx, median_key)
            parent.children.insert(child_idx + 1, new_leaf)

        else:
            # Split internal node: push median up (remove from children)
            mid = t - 1
            new_internal = BPlusNode(is_leaf=False)
            median_key = child.keys[mid]

            new_internal.keys = child.keys[mid + 1:]
            new_internal.children = child.children[mid + 1:]
            child.keys = child.keys[:mid]
            child.children = child.children[:mid + 1]

            parent.keys.insert(child_idx, median_key)
            parent.children.insert(child_idx + 1, new_internal)

    # ------------------------------------------------------------------ #
    #  Delete                                                              #
    # ------------------------------------------------------------------ #

    def delete(self, key: str) -> bool:
        """
        Delete a key. Returns True if found and deleted, False if not found.

        B+ Tree deletion is complex. We use a simplified approach:
          1. Find the leaf containing the key
          2. Remove it
          3. If leaf underflows (< t-1 keys), try to borrow from sibling
          4. If can't borrow, merge with sibling

        For simplicity, we implement a "lazy" delete that marks keys as
        deleted and rebuilds the tree periodically (like a compaction).
        Production databases use full rebalancing.
        """
        found = self._delete_recursive(self.root, key, None, -1)
        if found:
            self._size -= 1
            # If root became empty internal node, shrink tree height
            if not self.root.is_leaf and len(self.root.keys) == 0:
                self.root = self.root.children[0]
        return found

    def _delete_recursive(self, node: BPlusNode, key: str,
                          parent: Optional[BPlusNode], parent_idx: int) -> bool:
        if node.is_leaf:
            idx = bisect.bisect_left(node.keys, key)
            if idx < len(node.keys) and node.keys[idx] == key:
                node.keys.pop(idx)
                node.values.pop(idx)
                # Rebalance if underflow (simplified: only fix if not root)
                if parent is not None and len(node.keys) < self.order - 1:
                    self._rebalance_leaf(node, parent, parent_idx)
                return True
            return False
        else:
            idx = bisect.bisect_right(node.keys, key)
            found = self._delete_recursive(node.children[idx], key, node, idx)
            # Update separator key if needed
            if found and idx < len(node.keys):
                # Update the separator key to reflect the new minimum of right child
                right_child = node.children[idx + 1] if idx + 1 < len(node.children) else None
                if right_child:
                    node.keys[idx] = self._find_min_key(right_child)
            return found

    def _rebalance_leaf(self, leaf: BPlusNode, parent: BPlusNode, leaf_idx: int):
        """Try to borrow from sibling, or merge."""
        t = self.order

        # Try to borrow from right sibling
        if leaf_idx + 1 < len(parent.children):
            right = parent.children[leaf_idx + 1]
            if len(right.keys) > t - 1:
                # Borrow first key from right sibling
                leaf.keys.append(right.keys.pop(0))
                leaf.values.append(right.values.pop(0))
                parent.keys[leaf_idx] = right.keys[0]
                return

        # Try to borrow from left sibling
        if leaf_idx > 0:
            left = parent.children[leaf_idx - 1]
            if len(left.keys) > t - 1:
                # Borrow last key from left sibling
                leaf.keys.insert(0, left.keys.pop())
                leaf.values.insert(0, left.values.pop())
                parent.keys[leaf_idx - 1] = leaf.keys[0]
                return

        # Merge with a sibling
        if leaf_idx + 1 < len(parent.children):
            # Merge with right sibling
            right = parent.children[leaf_idx + 1]
            leaf.keys.extend(right.keys)
            leaf.values.extend(right.values)
            leaf.next = right.next
            parent.keys.pop(leaf_idx)
            parent.children.pop(leaf_idx + 1)
        elif leaf_idx > 0:
            # Merge with left sibling
            left = parent.children[leaf_idx - 1]
            left.keys.extend(leaf.keys)
            left.values.extend(leaf.values)
            left.next = leaf.next
            parent.keys.pop(leaf_idx - 1)
            parent.children.pop(leaf_idx)

    def _find_min_key(self, node: BPlusNode) -> str:
        while not node.is_leaf:
            node = node.children[0]
        return node.keys[0] if node.keys else ""

    # ------------------------------------------------------------------ #
    #  Range scan                                                          #
    # ------------------------------------------------------------------ #

    def scan(self, start_key: str = "", end_key: str = "\xff\xff\xff") -> Iterator[tuple[str, str]]:
        """
        Range scan from start_key to end_key (inclusive).

        B+ Tree advantage: leaf nodes are linked → O(k + log n) scan
        where k = number of results. No need to traverse internal nodes.
        """
        # Find the first leaf that could contain start_key
        leaf: Optional[BPlusNode] = self._find_leaf(start_key)

        while leaf is not None:
            for i, key in enumerate(leaf.keys):
                if key > end_key:
                    return
                if key >= start_key:
                    yield key, leaf.values[i]
            leaf = leaf.next

    def items(self) -> Iterator[tuple[str, str]]:
        """Iterate all key-value pairs in sorted order."""
        yield from self.scan()

    # ------------------------------------------------------------------ #
    #  Utilities                                                           #
    # ------------------------------------------------------------------ #

    def __len__(self) -> int:
        return self._size

    def height(self) -> int:
        """Return the height of the tree (1 = just root)."""
        h = 1
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
            h += 1
        return h

    def stats(self) -> dict:
        nodes, leaves, internal = self._count_nodes(self.root)
        return {
            "size": self._size,
            "height": self.height(),
            "total_nodes": nodes,
            "leaf_nodes": leaves,
            "internal_nodes": internal,
            "order": self.order,
        }

    def _count_nodes(self, node: BPlusNode) -> tuple[int, int, int]:
        if node.is_leaf:
            return 1, 1, 0
        total, leaves, internal = 1, 0, 1
        for child in node.children:
            t, l, i = self._count_nodes(child)
            total += t
            leaves += l
            internal += i
        return total, leaves, internal

    def visualize(self, max_depth: int = 4) -> str:
        """Return a text visualization of the tree structure."""
        lines = []
        self._viz_node(self.root, 0, max_depth, lines)
        return "\n".join(lines)

    def _viz_node(self, node: BPlusNode, depth: int, max_depth: int, lines: list):
        if depth > max_depth:
            return
        indent = "  " * depth
        if node.is_leaf:
            pairs = list(zip(node.keys, node.values))
            lines.append(f"{indent}LEAF {pairs}")
        else:
            lines.append(f"{indent}INTERNAL keys={node.keys}")
            for child in node.children:
                self._viz_node(child, depth + 1, max_depth, lines)


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

if __name__ == "__main__":
    print("=" * 60)
    print("B+ TREE STORAGE ENGINE DEMO")
    print("=" * 60)

    tree = BPlusTree(order=3)  # each node holds 2-5 keys

    # --- Step 1: Insert ---
    print("\n[Step 1] Inserting 20 key-value pairs...")
    data = [
        ("apple", "red fruit"),
        ("banana", "yellow fruit"),
        ("cherry", "red berry"),
        ("date", "brown fruit"),
        ("elderberry", "dark berry"),
        ("fig", "purple fruit"),
        ("grape", "green or purple"),
        ("honeydew", "green melon"),
        ("kiwi", "brown outside"),
        ("lemon", "yellow citrus"),
        ("mango", "tropical fruit"),
        ("nectarine", "smooth peach"),
        ("orange", "citrus fruit"),
        ("papaya", "tropical orange"),
        ("quince", "yellow pome"),
        ("raspberry", "red berry"),
        ("strawberry", "red berry"),
        ("tangerine", "small orange"),
        ("ugli", "citrus hybrid"),
        ("vanilla", "orchid seed"),
    ]
    for k, v in data:
        tree.set(k, v)

    print(f"  Inserted {len(tree)} items")
    print(f"  Tree stats: {tree.stats()}")

    # --- Step 2: Point lookups ---
    print("\n[Step 2] Point lookups...")
    for key in ["apple", "mango", "zebra", "cherry"]:
        result = tree.get(key)
        print(f"  GET {key!r:12} → {result!r}")

    # --- Step 3: Range scan ---
    print("\n[Step 3] Range scan 'c' to 'g'...")
    for k, v in tree.scan("c", "g"):
        print(f"  {k!r:12} → {v!r}")

    # --- Step 4: Update ---
    print("\n[Step 4] Update existing key...")
    tree.set("apple", "UPDATED: best fruit")
    print(f"  GET apple → {tree.get('apple')!r}")

    # --- Step 5: Delete ---
    print("\n[Step 5] Delete keys...")
    for key in ["banana", "fig", "lemon"]:
        ok = tree.delete(key)
        print(f"  DELETE {key!r:12} → found={ok}")
    print(f"  GET banana → {tree.get('banana')!r}  (None = deleted)")
    print(f"  Tree size after deletes: {len(tree)}")

    # --- Step 6: Full scan (sorted order) ---
    print("\n[Step 6] Full scan (all remaining keys in sorted order)...")
    for k, v in tree.items():
        print(f"  {k!r:12} → {v!r}")

    # --- Step 7: Tree structure ---
    print("\n[Step 7] Tree structure visualization...")
    print(tree.visualize())

    # --- Step 8: Performance test ---
    print("\n[Step 8] Performance: insert 10,000 keys...")
    import time
    big_tree = BPlusTree(order=50)  # larger order = fewer levels
    start = time.time()
    for i in range(10_000):
        big_tree.set(f"key:{i:06d}", f"value_{i}")
    insert_ms = (time.time() - start) * 1000

    start = time.time()
    for i in range(10_000):
        big_tree.get(f"key:{i:06d}")
    read_ms = (time.time() - start) * 1000

    start = time.time()
    count = sum(1 for _ in big_tree.scan("key:001000", "key:002000"))
    scan_ms = (time.time() - start) * 1000

    print(f"  Insert 10K keys: {insert_ms:.1f}ms ({10_000/insert_ms*1000:.0f} ops/sec)")
    print(f"  Read  10K keys:  {read_ms:.1f}ms ({10_000/read_ms*1000:.0f} ops/sec)")
    print(f"  Range scan 1K:   {scan_ms:.2f}ms ({count} results)")
    print(f"  Tree stats: {big_tree.stats()}")

    print("\n[Done] B+ Tree demo complete.")
    print("\nKey insights:")
    print("  1. B+ Tree keeps all data in leaves, internal nodes are just guides")
    print("  2. Leaf nodes are linked → O(k + log n) range scans")
    print("  3. All leaves at same depth → guaranteed O(log n) operations")
    print("  4. Node splits propagate upward → tree grows from root")
    print("  5. In production: each node = one disk page (4KB/8KB)")
    print("  6. PostgreSQL uses B+ Trees for all indexes")
