package com.litedb.btree;

import java.util.*;

/**
 * BTree — B+ Tree implementation (order = degree d, max 2d keys per node).
 *
 * CONCEPT:
 *   The B+ Tree is the index structure used by MySQL InnoDB, PostgreSQL,
 *   SQLite, Oracle, and virtually every relational database.
 *
 *   Properties:
 *     - All data lives in LEAF nodes (internal nodes only hold routing keys)
 *     - Leaf nodes are linked in a doubly-linked list → fast range scans
 *     - Tree is always balanced — all leaves at the same depth
 *     - Each node holds between d and 2d keys (except root: 1 to 2d)
 *     - Height = O(log_d N) → very shallow even for billions of rows
 *
 *   Why B+ Tree over BST?
 *     - BST height = O(log2 N) → 30 levels for 1 billion rows
 *     - B+ Tree height = O(log_d N) with d=100 → only 5 levels!
 *     - Each level = one disk I/O → B+ Tree needs 5 I/Os vs 30 for BST
 *
 *   Operations:
 *     insert(key, value) — O(log N)
 *     get(key)           — O(log N)
 *     delete(key)        — O(log N)
 *     range(lo, hi)      — O(log N + k) where k = result count
 */
public class BTree {

    private static final int ORDER = 3; // d=3: max 6 keys per node, min 3

    // ------------------------------------------------------------------ //
    //  Node types                                                         //
    // ------------------------------------------------------------------ //

    static abstract class Node {
        List<String> keys = new ArrayList<>();
        abstract boolean isLeaf();
    }

    static class InternalNode extends Node {
        List<Node> children = new ArrayList<>();

        @Override boolean isLeaf() { return false; }

        @Override
        public String toString() {
            return "Internal" + keys;
        }
    }

    static class LeafNode extends Node {
        List<String>   values = new ArrayList<>();
        LeafNode       next;   // linked list for range scans
        LeafNode       prev;

        @Override boolean isLeaf() { return true; }

        @Override
        public String toString() {
            return "Leaf" + keys;
        }
    }

    // ------------------------------------------------------------------ //
    //  Tree state                                                         //
    // ------------------------------------------------------------------ //

    private Node     root;
    private int      size = 0;
    private LeafNode firstLeaf; // leftmost leaf (for full scan)

    public BTree() {
        LeafNode leaf = new LeafNode();
        root      = leaf;
        firstLeaf = leaf;
    }

    // ------------------------------------------------------------------ //
    //  Public API                                                         //
    // ------------------------------------------------------------------ //

    public void insert(String key, String value) {
        SplitResult split = insertRecursive(root, key, value);
        if (split != null) {
            // Root was split — create new root
            InternalNode newRoot = new InternalNode();
            newRoot.keys.add(split.promotedKey);
            newRoot.children.add(split.left);
            newRoot.children.add(split.right);
            root = newRoot;
        }
        size++;
    }

    public String get(String key) {
        return searchLeaf(root, key);
    }

    public boolean delete(String key) {
        boolean deleted = deleteRecursive(root, key, null, -1);
        if (deleted) {
            size--;
            // If root is internal and has no keys, collapse
            if (!root.isLeaf() && root.keys.isEmpty()) {
                root = ((InternalNode) root).children.get(0);
            }
        }
        return deleted;
    }

    /** Range scan: returns all (key, value) pairs with lo <= key <= hi. */
    public List<Map.Entry<String, String>> range(String lo, String hi) {
        List<Map.Entry<String, String>> result = new ArrayList<>();
        // Find the leaf containing lo
        LeafNode leaf = findLeaf(root, lo);
        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                String k = leaf.keys.get(i);
                if (k.compareTo(lo) >= 0 && k.compareTo(hi) <= 0) {
                    result.add(new AbstractMap.SimpleImmutableEntry<>(k, leaf.values.get(i)));
                } else if (k.compareTo(hi) > 0) {
                    return result;
                }
            }
            leaf = leaf.next;
        }
        return result;
    }

    public int size() { return size; }

    // ------------------------------------------------------------------ //
    //  Insert helpers                                                     //
    // ------------------------------------------------------------------ //

    private static class SplitResult {
        String promotedKey;
        Node   left, right;
        SplitResult(String key, Node left, Node right) {
            this.promotedKey = key;
            this.left  = left;
            this.right = right;
        }
    }

    private SplitResult insertRecursive(Node node, String key, String value) {
        if (node.isLeaf()) {
            LeafNode leaf = (LeafNode) node;
            int pos = insertSorted(leaf.keys, key);
            // Handle duplicate: update value
            if (pos < 0) {
                leaf.values.set(~pos, value);
                size--; // will be incremented back by caller
                return null;
            }
            leaf.keys.add(pos, key);
            leaf.values.add(pos, value);

            if (leaf.keys.size() > 2 * ORDER) {
                return splitLeaf(leaf);
            }
            return null;
        } else {
            InternalNode internal = (InternalNode) node;
            int childIdx = findChildIndex(internal, key);
            SplitResult split = insertRecursive(internal.children.get(childIdx), key, value);
            if (split == null) return null;

            // Insert promoted key into this internal node
            internal.keys.add(childIdx, split.promotedKey);
            internal.children.set(childIdx, split.left);
            internal.children.add(childIdx + 1, split.right);

            if (internal.keys.size() > 2 * ORDER) {
                return splitInternal(internal);
            }
            return null;
        }
    }

    private SplitResult splitLeaf(LeafNode leaf) {
        int mid = leaf.keys.size() / 2;
        LeafNode right = new LeafNode();

        right.keys.addAll(leaf.keys.subList(mid, leaf.keys.size()));
        right.values.addAll(leaf.values.subList(mid, leaf.values.size()));
        leaf.keys.subList(mid, leaf.keys.size()).clear();
        leaf.values.subList(mid, leaf.values.size()).clear();

        // Maintain linked list
        right.next = leaf.next;
        right.prev = leaf;
        if (leaf.next != null) leaf.next.prev = right;
        leaf.next = right;

        return new SplitResult(right.keys.get(0), leaf, right);
    }

    private SplitResult splitInternal(InternalNode node) {
        int mid = node.keys.size() / 2;
        String promotedKey = node.keys.get(mid);

        InternalNode right = new InternalNode();
        right.keys.addAll(node.keys.subList(mid + 1, node.keys.size()));
        right.children.addAll(node.children.subList(mid + 1, node.children.size()));

        node.keys.subList(mid, node.keys.size()).clear();
        node.children.subList(mid + 1, node.children.size()).clear();

        return new SplitResult(promotedKey, node, right);
    }

    // ------------------------------------------------------------------ //
    //  Search helpers                                                     //
    // ------------------------------------------------------------------ //

    private String searchLeaf(Node node, String key) {
        if (node.isLeaf()) {
            LeafNode leaf = (LeafNode) node;
            int idx = Collections.binarySearch(leaf.keys, key);
            return idx >= 0 ? leaf.values.get(idx) : null;
        }
        InternalNode internal = (InternalNode) node;
        return searchLeaf(internal.children.get(findChildIndex(internal, key)), key);
    }

    private LeafNode findLeaf(Node node, String key) {
        if (node.isLeaf()) return (LeafNode) node;
        InternalNode internal = (InternalNode) node;
        return findLeaf(internal.children.get(findChildIndex(internal, key)), key);
    }

    // ------------------------------------------------------------------ //
    //  Delete helpers                                                     //
    // ------------------------------------------------------------------ //

    private boolean deleteRecursive(Node node, String key, InternalNode parent, int parentIdx) {
        if (node.isLeaf()) {
            LeafNode leaf = (LeafNode) node;
            int idx = Collections.binarySearch(leaf.keys, key);
            if (idx < 0) return false;
            leaf.keys.remove(idx);
            leaf.values.remove(idx);
            // Underflow check (skip for root)
            if (parent != null && leaf.keys.size() < ORDER) {
                rebalanceLeaf(leaf, parent, parentIdx);
            }
            return true;
        }

        InternalNode internal = (InternalNode) node;
        int childIdx = findChildIndex(internal, key);
        boolean deleted = deleteRecursive(internal.children.get(childIdx), key, internal, childIdx);
        if (deleted && internal.keys.size() < ORDER && parent != null) {
            rebalanceInternal(internal, parent, parentIdx);
        }
        return deleted;
    }

    private void rebalanceLeaf(LeafNode leaf, InternalNode parent, int idx) {
        // Try borrow from right sibling
        if (idx < parent.children.size() - 1) {
            LeafNode right = (LeafNode) parent.children.get(idx + 1);
            if (right.keys.size() > ORDER) {
                leaf.keys.add(right.keys.remove(0));
                leaf.values.add(right.values.remove(0));
                parent.keys.set(idx, right.keys.get(0));
                return;
            }
        }
        // Try borrow from left sibling
        if (idx > 0) {
            LeafNode left = (LeafNode) parent.children.get(idx - 1);
            if (left.keys.size() > ORDER) {
                leaf.keys.add(0, left.keys.remove(left.keys.size() - 1));
                leaf.values.add(0, left.values.remove(left.values.size() - 1));
                parent.keys.set(idx - 1, leaf.keys.get(0));
                return;
            }
        }
        // Merge with right sibling
        if (idx < parent.children.size() - 1) {
            LeafNode right = (LeafNode) parent.children.get(idx + 1);
            leaf.keys.addAll(right.keys);
            leaf.values.addAll(right.values);
            leaf.next = right.next;
            if (right.next != null) right.next.prev = leaf;
            parent.keys.remove(idx);
            parent.children.remove(idx + 1);
        } else if (idx > 0) {
            // Merge with left sibling
            LeafNode left = (LeafNode) parent.children.get(idx - 1);
            left.keys.addAll(leaf.keys);
            left.values.addAll(leaf.values);
            left.next = leaf.next;
            if (leaf.next != null) leaf.next.prev = left;
            parent.keys.remove(idx - 1);
            parent.children.remove(idx);
        }
    }

    private void rebalanceInternal(InternalNode node, InternalNode parent, int idx) {
        // Simplified: just merge with right sibling if possible
        if (idx < parent.children.size() - 1) {
            InternalNode right = (InternalNode) parent.children.get(idx + 1);
            node.keys.add(parent.keys.remove(idx));
            node.keys.addAll(right.keys);
            node.children.addAll(right.children);
            parent.children.remove(idx + 1);
        } else if (idx > 0) {
            InternalNode left = (InternalNode) parent.children.get(idx - 1);
            left.keys.add(parent.keys.remove(idx - 1));
            left.keys.addAll(node.keys);
            left.children.addAll(node.children);
            parent.children.remove(idx);
        }
    }

    // ------------------------------------------------------------------ //
    //  Utility                                                            //
    // ------------------------------------------------------------------ //

    /** Insert key into sorted list; return insertion index, or ~existingIndex if duplicate. */
    private static int insertSorted(List<String> list, String key) {
        int idx = Collections.binarySearch(list, key);
        if (idx >= 0) return ~idx; // duplicate
        return -(idx + 1);
    }

    private static int findChildIndex(InternalNode node, String key) {
        int i = 0;
        while (i < node.keys.size() && key.compareTo(node.keys.get(i)) >= 0) i++;
        return i;
    }

    /** Pretty-print the tree structure. */
    public void printTree() {
        printNode(root, 0);
    }

    private void printNode(Node node, int depth) {
        String indent = "  ".repeat(depth);
        if (node.isLeaf()) {
            LeafNode leaf = (LeafNode) node;
            System.out.println(indent + "LEAF " + leaf.keys + " → " + leaf.values);
        } else {
            InternalNode internal = (InternalNode) node;
            System.out.println(indent + "INTERNAL " + internal.keys);
            for (Node child : internal.children) printNode(child, depth + 1);
        }
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) {
        System.out.println("============================================================");
        System.out.println("B+ TREE DEMO");
        System.out.println("============================================================");

        BTree tree = new BTree();

        // Insert
        System.out.println("\n[Step 1] Inserting 15 keys...");
        String[] keys = {"mango","apple","cherry","banana","date","elderberry",
                         "fig","grape","honeydew","kiwi","lemon","nectarine",
                         "orange","papaya","quince"};
        for (int i = 0; i < keys.length; i++) {
            tree.insert(keys[i], "val_" + i);
        }
        System.out.println("  Tree size: " + tree.size());
        System.out.println("\n  Tree structure:");
        tree.printTree();

        // Point lookup
        System.out.println("\n[Step 2] Point lookups...");
        for (String k : new String[]{"apple", "mango", "zebra"}) {
            System.out.println("  GET '" + k + "' → " + tree.get(k));
        }

        // Range scan
        System.out.println("\n[Step 3] Range scan 'b' to 'f'...");
        for (Map.Entry<String, String> e : tree.range("b", "f")) {
            System.out.println("  '" + e.getKey() + "': '" + e.getValue() + "'");
        }

        // Update (re-insert same key)
        System.out.println("\n[Step 4] Update 'apple'...");
        tree.insert("apple", "updated_apple");
        System.out.println("  GET apple → " + tree.get("apple"));
        System.out.println("  Size unchanged: " + tree.size());

        // Delete
        System.out.println("\n[Step 5] Delete 'banana', 'mango'...");
        tree.delete("banana");
        tree.delete("mango");
        System.out.println("  GET banana → " + tree.get("banana") + " (null = deleted)");
        System.out.println("  GET mango  → " + tree.get("mango")  + " (null = deleted)");
        System.out.println("  Tree size: " + tree.size());

        // Full scan via leaf linked list
        System.out.println("\n[Step 6] Full scan via leaf linked list...");
        LeafNode leaf = tree.firstLeaf;
        List<String> allKeys = new ArrayList<>();
        while (leaf != null) {
            allKeys.addAll(leaf.keys);
            leaf = leaf.next;
        }
        System.out.println("  All keys in order: " + allKeys);

        System.out.println("\n[Done] B+ Tree demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. All data in leaf nodes — internal nodes are just routing guides");
        System.out.println("  2. Leaf nodes linked → O(k) range scan after O(log N) seek");
        System.out.println("  3. Always balanced — height = O(log_d N)");
        System.out.println("  4. With d=100, 1 billion rows needs only 5 levels (5 disk I/Os)");
        System.out.println("  5. MySQL InnoDB, PostgreSQL, SQLite all use B+ Trees for indexes");
    }
}