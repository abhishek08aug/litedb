package com.litedb.relational;

import com.litedb.engine.StorageEngine;
import com.litedb.lsm.LSMEngine;
import com.litedb.sql.SQLParser;
import com.litedb.sql.SQLParser.ColumnDef;
import com.litedb.sql.SQLParser.CreateTableStatement;
import com.litedb.sql.SQLParser.DeleteStatement;
import com.litedb.sql.SQLParser.DropTableStatement;
import com.litedb.sql.SQLParser.InsertStatement;
import com.litedb.sql.SQLParser.SelectStatement;
import com.litedb.sql.SQLParser.Statement;
import com.litedb.sql.SQLParser.WhereClause;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * RelationalEngine — a thin SQL/relational layer over a {@link StorageEngine}.
 *
 * Maps tables and rows onto the ordered key-value store via key prefixes (the standard
 * "SQL on KV" technique):
 *   __catalog__/&lt;table&gt;  -&gt; serialized schema
 *   &lt;table&gt;/&lt;pk&gt;         -&gt; encoded row
 * The primary key is the table's first column.
 *
 * Phase 1: schema catalog + DDL (CREATE/DROP TABLE).
 * Phase 2: DML — INSERT, SELECT (projection, WHERE, ORDER BY, LIMIT), DELETE, executed by
 *          scanning the table's key range. WHERE comparisons are lexicographic for now;
 *          typed/order-preserving comparison is a later phase. Secondary indexes and a query
 *          planner (index-scan vs full-scan) and transactional atomicity are later phases too.
 */
public final class RelationalEngine {

    private final StorageEngine engine;
    private final Catalog catalog;
    private final SQLParser parser = new SQLParser();

    public RelationalEngine(StorageEngine engine) throws IOException {
        this.engine = engine;
        this.catalog = new Catalog(engine);
    }

    public Catalog catalog() {
        return catalog;
    }

    static String rowKey(String table, String primaryKeyValue) {
        return table + "/" + primaryKeyValue;
    }

    /** Execute one SQL statement; returns a human-readable result. */
    public String execute(String sql) throws IOException {
        Statement stmt;
        try {
            stmt = parser.parse(sql);
        } catch (Exception e) {
            return "ERROR: parse: " + e.getMessage();
        }
        switch (stmt.type()) {
            case "CREATE_TABLE": return createTable((CreateTableStatement) stmt);
            case "DROP_TABLE":   return dropTable((DropTableStatement) stmt);
            case "INSERT":       return insert((InsertStatement) stmt);
            case "SELECT":       return select((SelectStatement) stmt);
            case "DELETE":       return delete((DeleteStatement) stmt);
            default:
                return "ERROR: " + stmt.type() + " not supported yet";
        }
    }

    // ---- DDL --------------------------------------------------------------

    private String createTable(CreateTableStatement s) throws IOException {
        if (catalog.hasTable(s.table)) return "ERROR: table already exists: " + s.table;
        List<Column> cols = new ArrayList<>();
        for (ColumnDef cd : s.columns) cols.add(new Column(cd.name, cd.type));
        TableSchema schema = new TableSchema(s.table, cols);
        catalog.createTable(schema);
        return "OK: created table " + s.table + " (PK=" + schema.primaryKey().name + ")";
    }

    private String dropTable(DropTableStatement s) throws IOException {
        if (!catalog.hasTable(s.table)) return "ERROR: no such table: " + s.table;
        int deleted = 0;
        for (Map.Entry<String, String> e : new ArrayList<>(scanTable(s.table))) {
            engine.delete(e.getKey());
            deleted++;
        }
        catalog.dropTable(s.table);
        return "OK: dropped table " + s.table + " (" + deleted + " rows removed)";
    }

    // ---- DML --------------------------------------------------------------

    private String insert(InsertStatement s) throws IOException {
        TableSchema schema = catalog.getTable(s.table);
        if (schema == null) return "ERROR: no such table: " + s.table;

        List<String> row = buildRow(schema, s.columns, s.values);
        if (row == null) return "ERROR: column/value mismatch for INSERT into " + s.table;

        String pk = row.get(0);                       // PK = first column
        String key = rowKey(s.table, pk);
        if (engine.get(key) != null) return "ERROR: duplicate primary key: " + pk;

        engine.set(key, RowCodec.encode(row));
        return "OK: 1 row inserted";
    }

    private String select(SelectStatement s) throws IOException {
        TableSchema schema = catalog.getTable(s.table);
        if (schema == null) return "ERROR: no such table: " + s.table;

        // resolve projected columns
        List<String> proj = (s.columns.size() == 1 && s.columns.get(0).equals("*"))
                ? schema.columnNames() : s.columns;
        for (String c : proj) {
            if (schema.columnIndex(c) < 0) return "ERROR: no such column: " + c;
        }

        // scan + filter (full rows in schema order)
        List<List<String>> rows = new ArrayList<>();
        for (Map.Entry<String, String> e : scanTable(s.table)) {
            List<String> row = RowCodec.decode(e.getValue());
            if (s.where == null || evalWhere(s.where, schema, row)) rows.add(row);
        }

        // ORDER BY (optional; by any schema column)
        if (s.orderBy != null) {
            int oi = schema.columnIndex(s.orderBy);
            if (oi >= 0) {
                rows.sort(Comparator.comparing(r -> r.get(oi)));
                if (s.orderDesc) Collections.reverse(rows);
            }
        }

        // LIMIT (optional)
        if (s.limit >= 0 && rows.size() > s.limit) rows = rows.subList(0, s.limit);

        // project
        List<List<String>> out = new ArrayList<>();
        for (List<String> row : rows) {
            List<String> r = new ArrayList<>();
            for (String c : proj) r.add(row.get(schema.columnIndex(c)));
            out.add(r);
        }
        return render(proj, out);
    }

    private String delete(DeleteStatement s) throws IOException {
        TableSchema schema = catalog.getTable(s.table);
        if (schema == null) return "ERROR: no such table: " + s.table;

        List<String> toDelete = new ArrayList<>();
        for (Map.Entry<String, String> e : scanTable(s.table)) {
            List<String> row = RowCodec.decode(e.getValue());
            if (s.where == null || evalWhere(s.where, schema, row)) toDelete.add(e.getKey());
        }
        for (String k : toDelete) engine.delete(k);
        return "OK: " + toDelete.size() + (toDelete.size() == 1 ? " row deleted" : " rows deleted");
    }

    // ---- helpers ----------------------------------------------------------

    /** All rows of a table: the key range [table/, table/￿]. */
    private List<Map.Entry<String, String>> scanTable(String table) throws IOException {
        return engine.scan(table + "/", table + "/" + Character.MAX_VALUE);
    }

    /** Build a full row (schema order) from an INSERT's column list + values. */
    private List<String> buildRow(TableSchema schema, List<String> cols, List<String> vals) {
        if (vals == null) return null;
        List<String> row = new ArrayList<>(Collections.nCopies(schema.columns.size(), ""));
        if (cols == null || cols.isEmpty()) {                 // positional
            if (vals.size() != schema.columns.size()) return null;
            for (int i = 0; i < vals.size(); i++) row.set(i, unquote(vals.get(i)));
        } else {                                              // named
            if (cols.size() != vals.size()) return null;
            for (int i = 0; i < cols.size(); i++) {
                int ci = schema.columnIndex(cols.get(i));
                if (ci < 0) return null;
                row.set(ci, unquote(vals.get(i)));
            }
        }
        return row;
    }

    private boolean evalWhere(WhereClause w, TableSchema schema, List<String> row) {
        int ci = schema.columnIndex(w.column);
        if (ci < 0) return false;
        String left = row.get(ci);
        String right = unquote(w.value);
        int cmp = left.compareTo(right);                      // lexicographic (Phase 4: typed)
        switch (w.operator) {
            case "=":    return left.equals(right);
            case "!=":   return !left.equals(right);
            case "<":    return cmp < 0;
            case ">":    return cmp > 0;
            case "<=":   return cmp <= 0;
            case ">=":   return cmp >= 0;
            case "LIKE": return likeMatch(left, right);
            default:     return false;
        }
    }

    private static boolean likeMatch(String s, String pattern) {
        StringBuilder rx = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (c == '%') rx.append(".*");
            else if (c == '_') rx.append('.');
            else rx.append(Pattern.quote(String.valueOf(c)));
        }
        return s.matches(rx.toString());
    }

    private static String unquote(String v) {
        if (v == null) return "";
        if (v.length() >= 2
                && ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith("\"") && v.endsWith("\"")))) {
            return v.substring(1, v.length() - 1);
        }
        return v;
    }

    private static String render(List<String> cols, List<List<String>> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.join(" | ", cols)).append("\n");
        for (List<String> r : rows) sb.append(String.join(" | ", r)).append("\n");
        sb.append("(").append(rows.size()).append(rows.size() == 1 ? " row)" : " rows)");
        return sb.toString();
    }

    // ===================================================================== //
    //  DEMO — Phases 1 + 2: DDL, INSERT, SELECT, DELETE                        //
    // ===================================================================== //

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("litedb_rel_demo_");
        LSMEngine engine = new LSMEngine(dir.toString());
        RelationalEngine db = new RelationalEngine(engine);

        String[] script = {
            "CREATE TABLE users (id INT, name TEXT, age INT)",
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)",
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)",
            "INSERT INTO users (id, name, age) VALUES (1, 'Dup', 99)",   // duplicate PK -> error
            "SELECT * FROM users",
            "SELECT name, age FROM users WHERE age > 25",                // projection + filter
            "SELECT * FROM users ORDER BY age DESC",                     // ordering
            "SELECT name FROM users WHERE id = 2",                       // point predicate
            "DELETE FROM users WHERE id = 1",
            "SELECT * FROM users",
        };
        for (String sql : script) {
            System.out.println("litedb> " + sql);
            System.out.println(db.execute(sql));
            System.out.println();
        }

        engine.close();
        Files.walk(dir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
        System.out.println("[Phase 2 demo complete]");
    }
}
