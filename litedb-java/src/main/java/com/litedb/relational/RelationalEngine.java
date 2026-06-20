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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RelationalEngine — a SQL/relational layer over a {@link StorageEngine}.
 *
 * Key namespaces on the ordered KV store ("SQL on KV"):
 *   __catalog__/table/&lt;t&gt;                       -&gt; schema
 *   __catalog__/index/&lt;name&gt;                    -&gt; index definition
 *   &lt;table&gt;/&lt;pk&gt;                                -&gt; row (RowCodec)
 *   __idx__/&lt;table&gt;/&lt;column&gt;/&lt;value&gt;\0&lt;pk&gt;       -&gt; pk   (secondary index entry)
 *
 * Phase 1: catalog + DDL.  Phase 2: INSERT/SELECT/DELETE via full scan.
 * Phase 3 (this): CREATE/DROP INDEX, multiple secondary indexes maintained on every write,
 * and a query planner that uses an index range-scan instead of a full scan when the WHERE
 * column is indexed with a range/equality operator.
 *
 * Comparisons are still lexicographic (typed, order-preserving encoding is Phase 4) and
 * multi-key writes are not yet atomic (Phase 5).
 */
public final class RelationalEngine {

    private static final String SEP = String.valueOf((char) 0);   // value\0pk separator in index keys
    private static final Pattern CREATE_INDEX =
            Pattern.compile("(?i)\\s*CREATE\\s+INDEX\\s+(\\w+)\\s+ON\\s+(\\w+)\\s*\\(\\s*(\\w+)\\s*\\)\\s*;?\\s*");
    private static final Pattern DROP_INDEX =
            Pattern.compile("(?i)\\s*DROP\\s+INDEX\\s+(\\w+)\\s*;?\\s*");

    private final StorageEngine engine;
    private final Catalog catalog;
    private final SQLParser parser = new SQLParser();

    public RelationalEngine(StorageEngine engine) throws IOException {
        this.engine = engine;
        this.catalog = new Catalog(engine);
    }

    public Catalog catalog() { return catalog; }

    static String rowKey(String table, String pk)            { return table + "/" + pk; }
    static String indexPrefix(String table, String column)   { return "__idx__/" + table + "/" + column + "/"; }
    static String indexKey(String table, String col, String value, String pk) {
        return indexPrefix(table, col) + value + SEP + pk;
    }

    /** Execute one statement; returns a human-readable result. */
    public String execute(String sql) throws IOException {
        // CREATE INDEX / DROP INDEX aren't in SQLParser yet — handle them here.
        Matcher ci = CREATE_INDEX.matcher(sql);
        if (ci.matches()) return createIndex(ci.group(1), ci.group(2), ci.group(3));
        Matcher di = DROP_INDEX.matcher(sql);
        if (di.matches()) return dropIndex(di.group(1));

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
            default:             return "ERROR: " + stmt.type() + " not supported yet";
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
        // drop the table's indexes (entries + defs)
        for (IndexDef idx : catalog.indexesForTable(s.table)) {
            deletePrefix(indexPrefix(s.table, idx.column));
            catalog.dropIndex(idx.name);
        }
        int deleted = 0;
        for (Map.Entry<String, String> e : new ArrayList<>(scanTable(s.table))) {
            engine.delete(e.getKey());
            deleted++;
        }
        catalog.dropTable(s.table);
        return "OK: dropped table " + s.table + " (" + deleted + " rows removed)";
    }

    private String createIndex(String name, String table, String column) throws IOException {
        TableSchema schema = catalog.getTable(table);
        if (schema == null)                          return "ERROR: no such table: " + table;
        if (schema.columnIndex(column) < 0)          return "ERROR: no such column: " + column;
        if (catalog.hasIndex(name))                  return "ERROR: index already exists: " + name;
        if (catalog.indexForColumn(table, column) != null)
            return "ERROR: column already indexed: " + table + "(" + column + ")";

        catalog.createIndex(new IndexDef(name, table, column));
        // build it from existing rows
        int ci = schema.columnIndex(column);
        String type = schema.columnType(column);
        int built = 0;
        for (Map.Entry<String, String> e : scanTable(table)) {
            List<String> row = RowCodec.decode(e.getValue());
            engine.set(indexKey(table, column, TypeCodec.encode(type, row.get(ci)), row.get(0)), row.get(0));
            built++;
        }
        return "OK: created index " + name + " ON " + table + "(" + column + ") — " + built + " entries";
    }

    private String dropIndex(String name) throws IOException {
        IndexDef def = catalog.getIndex(name);
        if (def == null) return "ERROR: no such index: " + name;
        deletePrefix(indexPrefix(def.table, def.column));
        catalog.dropIndex(name);
        return "OK: dropped index " + name;
    }

    // ---- DML --------------------------------------------------------------

    private String insert(InsertStatement s) throws IOException {
        TableSchema schema = catalog.getTable(s.table);
        if (schema == null) return "ERROR: no such table: " + s.table;
        List<String> row = buildRow(schema, s.columns, s.values);
        if (row == null) return "ERROR: column/value mismatch for INSERT into " + s.table;

        String pk = row.get(0);
        if (engine.get(rowKey(s.table, pk)) != null) return "ERROR: duplicate primary key: " + pk;

        engine.set(rowKey(s.table, pk), RowCodec.encode(row));
        addIndexEntries(schema, pk, row);
        return "OK: 1 row inserted";
    }

    private String select(SelectStatement s) throws IOException {
        TableSchema schema = catalog.getTable(s.table);
        if (schema == null) return "ERROR: no such table: " + s.table;

        List<String> proj = (s.columns.size() == 1 && s.columns.get(0).equals("*"))
                ? schema.columnNames() : s.columns;
        for (String c : proj) if (schema.columnIndex(c) < 0) return "ERROR: no such column: " + c;

        // ---- query planner: index-scan vs full-scan ----
        List<List<String>> rows = new ArrayList<>();
        String plan;
        IndexDef idx = (s.where != null) ? catalog.indexForColumn(s.table, s.where.column) : null;
        if (idx != null && isRangeOp(s.where.operator)) {
            rows = indexScan(schema, idx, s.where);
            plan = "index-scan on " + s.where.column + " (" + idx.name + ")";
        } else {
            for (Map.Entry<String, String> e : scanTable(s.table)) {
                List<String> row = RowCodec.decode(e.getValue());
                if (s.where == null || evalWhere(s.where, schema, row)) rows.add(row);
            }
            plan = "full-scan"
                 + (s.where != null && idx == null ? " (no index on " + s.where.column + ")" : "");
        }

        if (s.orderBy != null) {
            int oi = schema.columnIndex(s.orderBy);
            if (oi >= 0) {
                String otype = schema.columns.get(oi).type;
                rows.sort((a, b) -> TypeCodec.compare(otype, a.get(oi), b.get(oi)));
                if (s.orderDesc) Collections.reverse(rows);
            }
        }
        if (s.limit >= 0 && rows.size() > s.limit) rows = rows.subList(0, s.limit);

        List<List<String>> out = new ArrayList<>();
        for (List<String> row : rows) {
            List<String> r = new ArrayList<>();
            for (String c : proj) r.add(row.get(schema.columnIndex(c)));
            out.add(r);
        }
        return "-- plan: " + plan + "\n" + render(proj, out);
    }

    private String delete(DeleteStatement s) throws IOException {
        TableSchema schema = catalog.getTable(s.table);
        if (schema == null) return "ERROR: no such table: " + s.table;

        List<Map.Entry<String, List<String>>> victims = new ArrayList<>();
        for (Map.Entry<String, String> e : scanTable(s.table)) {
            List<String> row = RowCodec.decode(e.getValue());
            if (s.where == null || evalWhere(s.where, schema, row)) {
                victims.add(Map.entry(e.getKey(), row));
            }
        }
        for (Map.Entry<String, List<String>> v : victims) {
            engine.delete(v.getKey());
            removeIndexEntries(schema, v.getValue().get(0), v.getValue());
        }
        int n = victims.size();
        return "OK: " + n + (n == 1 ? " row deleted" : " rows deleted");
    }

    // ---- index maintenance + scan ----------------------------------------

    private void addIndexEntries(TableSchema schema, String pk, List<String> row) throws IOException {
        for (IndexDef idx : catalog.indexesForTable(schema.name)) {
            String enc = TypeCodec.encode(schema.columnType(idx.column),
                                          row.get(schema.columnIndex(idx.column)));
            engine.set(indexKey(schema.name, idx.column, enc, pk), pk);
        }
    }

    private void removeIndexEntries(TableSchema schema, String pk, List<String> row) throws IOException {
        for (IndexDef idx : catalog.indexesForTable(schema.name)) {
            String enc = TypeCodec.encode(schema.columnType(idx.column),
                                          row.get(schema.columnIndex(idx.column)));
            engine.delete(indexKey(schema.name, idx.column, enc, pk));
        }
    }

    /** Resolve a WHERE predicate via an index range-scan, fetching matching rows by PK. */
    private List<List<String>> indexScan(TableSchema schema, IndexDef idx, WhereClause w) throws IOException {
        String table = schema.name;
        String prefix = indexPrefix(table, idx.column);
        String v = TypeCodec.encode(schema.columnType(idx.column), unquote(w.value)); // order-preserving
        String lo, hi;
        switch (w.operator) {
            case "=":            lo = prefix + v; hi = prefix + v + Character.MAX_VALUE; break;
            case ">": case ">=": lo = prefix + v; hi = prefix + Character.MAX_VALUE;     break;
            case "<": case "<=": lo = prefix;     hi = prefix + v + Character.MAX_VALUE; break;
            default:             lo = prefix;     hi = prefix + Character.MAX_VALUE;
        }
        List<List<String>> rows = new ArrayList<>();
        for (Map.Entry<String, String> e : engine.scan(lo, hi)) {
            String key = e.getKey();
            int sep = key.indexOf(SEP, prefix.length());
            if (sep < 0) continue;
            String colValue = key.substring(prefix.length(), sep);
            if (!opMatch(colValue, w.operator, v)) continue;       // over-capture safety filter
            String rowVal = engine.get(rowKey(table, e.getValue()));
            if (rowVal != null) rows.add(RowCodec.decode(rowVal));
        }
        return rows;
    }

    private static boolean isRangeOp(String op) {
        return op.equals("=") || op.equals("<") || op.equals(">") || op.equals("<=") || op.equals(">=");
    }

    private static boolean opMatch(String colValue, String op, String v) {
        int cmp = colValue.compareTo(v);
        switch (op) {
            case "=":  return colValue.equals(v);
            case "<":  return cmp < 0;
            case ">":  return cmp > 0;
            case "<=": return cmp <= 0;
            case ">=": return cmp >= 0;
            default:   return false;
        }
    }

    // ---- helpers ----------------------------------------------------------

    private List<Map.Entry<String, String>> scanTable(String table) throws IOException {
        return engine.scan(table + "/", table + "/" + Character.MAX_VALUE);
    }

    private void deletePrefix(String prefix) throws IOException {
        for (Map.Entry<String, String> e : new ArrayList<>(engine.scan(prefix, prefix + Character.MAX_VALUE))) {
            engine.delete(e.getKey());
        }
    }

    private List<String> buildRow(TableSchema schema, List<String> cols, List<String> vals) {
        if (vals == null) return null;
        List<String> row = new ArrayList<>(Collections.nCopies(schema.columns.size(), ""));
        if (cols == null || cols.isEmpty()) {
            if (vals.size() != schema.columns.size()) return null;
            for (int i = 0; i < vals.size(); i++) row.set(i, unquote(vals.get(i)));
        } else {
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
        if (w.operator.equals("LIKE")) return likeMatch(left, right);
        int cmp = TypeCodec.compare(schema.columns.get(ci).type, left, right);   // typed compare
        switch (w.operator) {
            case "=":  return cmp == 0;
            case "!=": return cmp != 0;
            case "<":  return cmp < 0;
            case ">":  return cmp > 0;
            case "<=": return cmp <= 0;
            case ">=": return cmp >= 0;
            default:   return false;
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
    //  DEMO — Phases 1–3: DDL, DML, CREATE INDEX, query planner                //
    // ===================================================================== //

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("litedb_rel_demo_");
        LSMEngine engine = new LSMEngine(dir.toString());
        RelationalEngine db = new RelationalEngine(engine);

        String[] script = {
            "CREATE TABLE nums (id INT, n INT)",
            "INSERT INTO nums (id, n) VALUES (1, 5)",
            "INSERT INTO nums (id, n) VALUES (2, 100)",
            "INSERT INTO nums (id, n) VALUES (3, 9)",
            "INSERT INTO nums (id, n) VALUES (4, 10)",
            "INSERT INTO nums (id, n) VALUES (5, -7)",
            // Without typed encoding, "10" < "9" < "100" lexicographically — all wrong.
            "SELECT * FROM nums ORDER BY n",          // typed order: -7, 5, 9, 10, 100
            "SELECT * FROM nums WHERE n > 9",         // full-scan, typed: 10, 100
            "CREATE INDEX idx_n ON nums(n)",
            "SELECT * FROM nums WHERE n > 9",         // index-scan, typed: 10, 100 (same result)
            "SELECT * FROM nums WHERE n <= 9 ORDER BY n",  // index-scan: -7, 5, 9
            "SELECT * FROM nums WHERE n = 100",       // index-scan: 100
        };
        for (String sql : script) {
            System.out.println("litedb> " + sql);
            System.out.println(db.execute(sql));
            System.out.println();
        }

        engine.close();
        Files.walk(dir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
        System.out.println("[Phase 4 demo complete]");
    }
}
