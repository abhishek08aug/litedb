package com.litedb.relational;

/**
 * Column — a typed column in a table schema.
 *
 * Phase 1 stores all column values as strings; typed, order-preserving encoding
 * (so numeric range queries sort correctly) is a later phase.
 */
public final class Column {
    public final String name;
    public final String type;   // INT | TEXT | FLOAT | BOOL

    public Column(String name, String type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public String toString() {
        return name + " " + type;
    }
}
