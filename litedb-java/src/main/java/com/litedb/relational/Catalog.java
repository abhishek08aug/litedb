package com.litedb.relational;

import com.litedb.mvcc.MVCCEngine;
import com.litedb.mvcc.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Catalog — the system catalog (table schemas + index definitions), persisted under reserved
 * key namespaces and versioned through MVCC like all other data:
 *   __catalog__/table/&lt;table&gt;  -&gt; serialized schema
 *   __catalog__/index/&lt;name&gt;   -&gt; serialized index definition
 *
 * The catalog is loaded into memory once (a snapshot read) and mutated through the owning
 * statement's transaction; the in-memory cache is updated as part of the mutation.
 */
public final class Catalog {

    static final String TABLE_PREFIX = "__catalog__/table/";
    static final String INDEX_PREFIX = "__catalog__/index/";

    private final Map<String, TableSchema> tables = new LinkedHashMap<>();
    private final Map<String, IndexDef> indexes = new LinkedHashMap<>();

    public Catalog(MVCCEngine mvcc) throws IOException {
        Transaction tx = mvcc.begin();
        try {
            for (Map.Entry<String, String> e : tx.scan(TABLE_PREFIX, TABLE_PREFIX + Character.MAX_VALUE)) {
                TableSchema s = TableSchema.deserialize(e.getValue());
                tables.put(s.name, s);
            }
            for (Map.Entry<String, String> e : tx.scan(INDEX_PREFIX, INDEX_PREFIX + Character.MAX_VALUE)) {
                IndexDef d = IndexDef.deserialize(e.getValue());
                indexes.put(d.name, d);
            }
        } finally {
            tx.rollback();
        }
    }

    // ---- reads (from cache) ----------------------------------------------

    public boolean hasTable(String name)     { return tables.containsKey(name); }
    public TableSchema getTable(String name) { return tables.get(name); }
    public Iterable<String> tableNames()     { return tables.keySet(); }
    public boolean hasIndex(String name)     { return indexes.containsKey(name); }
    public IndexDef getIndex(String name)    { return indexes.get(name); }

    public List<IndexDef> indexesForTable(String table) {
        List<IndexDef> out = new ArrayList<>();
        for (IndexDef d : indexes.values()) if (d.table.equals(table)) out.add(d);
        return out;
    }

    public IndexDef indexForColumn(String table, String column) {
        for (IndexDef d : indexes.values()) {
            if (d.table.equals(table) && d.column.equals(column)) return d;
        }
        return null;
    }

    // ---- mutations (staged on the statement's transaction) ----------------

    public void createTable(TableSchema schema, Transaction tx) {
        tx.put(TABLE_PREFIX + schema.name, schema.serialize());
        tables.put(schema.name, schema);
    }

    public void dropTable(String name, Transaction tx) {
        tx.delete(TABLE_PREFIX + name);
        tables.remove(name);
    }

    public void createIndex(IndexDef def, Transaction tx) {
        tx.put(INDEX_PREFIX + def.name, def.serialize());
        indexes.put(def.name, def);
    }

    public void dropIndex(String name, Transaction tx) {
        tx.delete(INDEX_PREFIX + name);
        indexes.remove(name);
    }
}
