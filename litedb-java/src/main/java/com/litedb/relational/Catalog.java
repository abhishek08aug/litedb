package com.litedb.relational;

import com.litedb.engine.StorageEngine;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Catalog — the system catalog: table schemas, persisted in the storage engine under the
 * reserved "__catalog__/" key namespace so they survive restarts.
 *
 * This is how real databases store metadata — in system tables within the same store as the
 * data. On construction the catalog loads every schema by range-scanning its prefix.
 */
public final class Catalog {

    static final String PREFIX = "__catalog__/";

    private final StorageEngine engine;
    private final Map<String, TableSchema> tables = new LinkedHashMap<>();

    public Catalog(StorageEngine engine) throws IOException {
        this.engine = engine;
        load();
    }

    private void load() throws IOException {
        for (Map.Entry<String, String> e : engine.scan(PREFIX, PREFIX + Character.MAX_VALUE)) {
            TableSchema schema = TableSchema.deserialize(e.getValue());
            tables.put(schema.name, schema);
        }
    }

    public boolean hasTable(String name) {
        return tables.containsKey(name);
    }

    public TableSchema getTable(String name) {
        return tables.get(name);
    }

    public Iterable<String> tableNames() {
        return tables.keySet();
    }

    public void createTable(TableSchema schema) throws IOException {
        if (tables.containsKey(schema.name)) {
            throw new IllegalStateException("table already exists: " + schema.name);
        }
        engine.set(PREFIX + schema.name, schema.serialize());   // persist
        tables.put(schema.name, schema);                        // cache
    }

    public void dropTable(String name) throws IOException {
        if (!tables.containsKey(name)) {
            throw new IllegalStateException("no such table: " + name);
        }
        engine.delete(PREFIX + name);
        tables.remove(name);
    }
}
