package com.litedb.relational;

import java.util.Arrays;
import java.util.List;

/**
 * IndexDef — a named secondary index on one column of a table.
 *
 * Index entries live under the key namespace:
 *   __idx__/&lt;table&gt;/&lt;column&gt;/&lt;columnValue&gt;\0&lt;pk&gt;  -&gt;  &lt;pk&gt;
 *
 * Sorting by columnValue means a predicate on the column becomes an index range-scan.
 */
public final class IndexDef {
    public final String name;
    public final String table;
    public final String column;

    public IndexDef(String name, String table, String column) {
        this.name = name;
        this.table = table;
        this.column = column;
    }

    public String serialize() {
        return RowCodec.encode(Arrays.asList(name, table, column));
    }

    public static IndexDef deserialize(String s) {
        List<String> f = RowCodec.decode(s);
        return new IndexDef(f.get(0), f.get(1), f.get(2));
    }

    @Override
    public String toString() {
        return name + " ON " + table + "(" + column + ")";
    }
}
