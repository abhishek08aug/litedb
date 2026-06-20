package com.litedb.relational;

import java.util.ArrayList;
import java.util.List;

/**
 * TableSchema — a table's name and its typed columns.
 *
 * The PRIMARY KEY is the first column (a Phase 1 convention; an explicit PRIMARY KEY clause
 * is a later addition). Rows are stored under the key "&lt;table&gt;/&lt;primaryKeyValue&gt;".
 */
public final class TableSchema {
    public final String name;
    public final List<Column> columns;

    public TableSchema(String name, List<Column> columns) {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("table '" + name + "' needs at least one column");
        }
        this.name = name;
        this.columns = columns;
    }

    /** Convention: the first column is the primary key. */
    public Column primaryKey() {
        return columns.get(0);
    }

    public int columnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name.equals(columnName)) return i;
        }
        return -1;
    }

    public List<String> columnNames() {
        List<String> names = new ArrayList<>();
        for (Column c : columns) names.add(c.name);
        return names;
    }

    // ---- catalog serialization (via RowCodec) -----------------------------

    public String serialize() {
        List<String> fields = new ArrayList<>();
        fields.add(name);
        for (Column c : columns) {
            fields.add(c.name);
            fields.add(c.type);
        }
        return RowCodec.encode(fields);
    }

    public static TableSchema deserialize(String s) {
        List<String> f = RowCodec.decode(s);
        String table = f.get(0);
        List<Column> cols = new ArrayList<>();
        for (int i = 1; i + 1 < f.size(); i += 2) {
            cols.add(new Column(f.get(i), f.get(i + 1)));
        }
        return new TableSchema(table, cols);
    }

    @Override
    public String toString() {
        return name + columns + " PK=" + primaryKey().name;
    }
}
