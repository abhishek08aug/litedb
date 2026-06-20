package com.litedb.relational;

import com.litedb.engine.StorageEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Catalog — the system catalog: table schemas and index definitions, persisted in the storage
 * engine under reserved key namespaces so they survive restarts:
 *   __catalog__/table/&lt;table&gt;  -&gt; serialized schema
 *   __catalog__/index/&lt;name&gt;   -&gt; serialized index definition
 *
 * On construction the catalog reloads everything by range-scanning each prefix.
 */
public final class Catalog {

    static final String TABLE_PREFIX = "__catalog__/table/";
    static final String INDEX_PREFIX = "__catalog__/index/";

    private final StorageEngine engine;
    private final Map<String, TableSchema> tables = new LinkedHashMap<>();
    private final Map<String, IndexDef> indexes = new LinkedHashMap<>();   // name -> def

    public Catalog(StorageEngine engine) throws IOException {
        this.engine = engine;
        load();
    }

    private void load() throws IOException {
        for (Map.Entry<String, String> e : engine.scan(TABLE_PREFIX, TABLE_PREFIX + Character.MAX_VALUE)) {
            TableSchema s = TableSchema.deserialize(e.getValue());
            tables.put(s.name, s);
        }
        for (Map.Entry<String, String> e : engine.scan(INDEX_PREFIX, INDEX_PREFIX + Character.MAX_VALUE)) {
            IndexDef d = IndexDef.deserialize(e.getValue());
            indexes.put(d.name, d);
        }
    }

    // ---- tables -----------------------------------------------------------

    public boolean hasTable(String name)      { return tables.containsKey(name); }
    public TableSchema getTable(String name)  { return tables.get(name); }
    public Iterable<String> tableNames()      { return tables.keySet(); }

    public void createTable(TableSchema schema) throws IOException {
        if (tables.containsKey(schema.name)) throw new IllegalStateException("table already exists: " + schema.name);
        engine.set(TABLE_PREFIX + schema.name, schema.serialize());
        tables.put(schema.name, schema);
    }

    public void dropTable(String name) throws IOException {
        if (!tables.containsKey(name)) throw new IllegalStateException("no such table: " + name);
        engine.delete(TABLE_PREFIX + name);
        tables.remove(name);
    }

    // ---- indexes ----------------------------------------------------------

    public boolean hasIndex(String name)        { return indexes.containsKey(name); }
    public IndexDef getIndex(String name)       { return indexes.get(name); }
    public Collection<IndexDef> allIndexes()    { return indexes.values(); }

    public List<IndexDef> indexesForTable(String table) {
        List<IndexDef> out = new ArrayList<>();
        for (IndexDef d : indexes.values()) if (d.table.equals(table)) out.add(d);
        return out;
    }

    /** The index on a given (table, column), or null if none. */
    public IndexDef indexForColumn(String table, String column) {
        for (IndexDef d : indexes.values()) {
            if (d.table.equals(table) && d.column.equals(column)) return d;
        }
        return null;
    }

    public void createIndex(IndexDef def) throws IOException {
        if (indexes.containsKey(def.name)) throw new IllegalStateException("index already exists: " + def.name);
        engine.set(INDEX_PREFIX + def.name, def.serialize());
        indexes.put(def.name, def);
    }

    public void dropIndex(String name) throws IOException {
        if (!indexes.containsKey(name)) throw new IllegalStateException("no such index: " + name);
        engine.delete(INDEX_PREFIX + name);
        indexes.remove(name);
    }
}
