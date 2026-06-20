package com.litedb.index;

import com.litedb.btree.BPlusTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SecondaryIndex — a value index over an opaque key->value store.
 *
 * Maps stored VALUE -> the primary keys holding it, so a query can find keys by value
 * (and by value range) without scanning the whole keyspace. This is the reverse of the
 * base store's key->value mapping.
 *
 * Backed by a B+Tree keyed on a composite (value + SEP + primaryKey). The composite:
 *   - supports duplicate values (many keys with the same value) — the primary key makes
 *     each index entry unique, exactly how real databases build non-unique indexes;
 *   - sorts by value first, so an ordered range scan over values is a B+Tree range scan.
 *
 * The separator is U+0000, which text values and keys never contain, so the composite is
 * unambiguous. Range queries over-capture at the boundary by design and then filter on the
 * parsed value, which stays correct regardless of where separators fall.
 *
 * Maintained by the owning engine on every set/delete. Held in memory and rebuilt from the
 * base data on startup (it is not itself persisted).
 *
 * Note: ranges are lexicographic (string order), consistent with the rest of LiteDB —
 * numeric values stored as strings sort lexicographically (e.g. "100" < "30").
 */
public final class SecondaryIndex {

    private static final char SEP = '\0';   // U+0000 (NUL) - never appears in text values/keys

    private final BPlusTree tree = new BPlusTree();     // composite -> primaryKey
    private int entries = 0;

    private static String composite(String value, String key) {
        return value + SEP + key;
    }

    /** Record that {@code key} now holds {@code value}. */
    public synchronized void add(String key, String value) {
        String c = composite(value, key);
        if (tree.get(c) == null) entries++;
        tree.insert(c, key);
    }

    /** Remove the {@code (value, key)} association. */
    public synchronized void remove(String key, String value) {
        if (tree.delete(composite(value, key))) entries--;
    }

    /** Move {@code key} from {@code oldValue} to {@code newValue}. */
    public synchronized void update(String key, String oldValue, String newValue) {
        if (oldValue != null && !oldValue.equals(newValue)) {
            remove(key, oldValue);
        }
        add(key, newValue);
    }

    /** Primary keys whose value is within [lowValue, highValue] (inclusive). */
    public synchronized List<String> keysInValueRange(String lowValue, String highValue) {
        // Over-capture with a high sentinel, then filter on the parsed value so boundary
        // cases (values that are prefixes of one another) are always correct.
        String lo = lowValue;
        String hi = highValue + Character.MAX_VALUE;   // U+FFFF
        List<String> keys = new ArrayList<>();
        for (Map.Entry<String, String> e : tree.range(lo, hi)) {
            String comp = e.getKey();
            int s = comp.indexOf(SEP);
            String value = (s >= 0) ? comp.substring(0, s) : comp;
            if (value.compareTo(lowValue) >= 0 && value.compareTo(highValue) <= 0) {
                keys.add(e.getValue());   // the primary key
            }
        }
        return keys;
    }

    public synchronized int size() {
        return entries;
    }
}
