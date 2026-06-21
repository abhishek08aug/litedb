package com.litedb.relational;

import com.litedb.lsm.LSMEngine;
import com.litedb.mvcc.ConflictException;
import com.litedb.mvcc.MVCCEngine;
import com.litedb.mvcc.Transaction;
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
 * RelationalEngine — a SQL layer over an {@link MVCCEngine}.
 *
 * Every statement runs in an MVCC transaction: reads at a consistent snapshot, writes buffered and
 * committed atomically with write-write conflict detection. Statements auto-commit by default;
 * explicit {@code BEGIN} / {@code COMMIT} / {@code ROLLBACK} group several statements into one
 * transaction (so concurrent sessions get snapshot isolation).
 *
 * Catalog, rows, and index entries are all stored as MVCC-versioned keys:
 *   __catalog__/table/&lt;t&gt; ,  __catalog__/index/&lt;name&gt; ,  &lt;table&gt;/&lt;pk&gt; ,
 *   __idx__/&lt;table&gt;/&lt;column&gt;/&lt;encodedValue&gt;\0&lt;pk&gt;
 */
public final class RelationalEngine {

    private static final String SEP = String.valueOf((char) 0);
    private static final Pattern CREATE_INDEX =
            Pattern.compile("(?i)\\s*CREATE\\s+INDEX\\s+(\\w+)\\s+ON\\s+(\\w+)\\s*\\(\\s*(\\w+)\\s*\\)\\s*;?\\s*");
    private static final Pattern DROP_INDEX =
            Pattern.compile("(?i)\\s*DROP\\s+INDEX\\s+(\\w+)\\s*;?\\s*");

    private final MVCCEngine mvcc;
    private final Catalog catalog;
    private final SQLParser parser = new SQLParser();
    private Transaction current;   // non-null inside an explicit BEGIN..COMMIT

    public RelationalEngine(MVCCEngine mvcc) throws IOException {
        this.mvcc = mvcc;
        this.catalog = new Catalog(mvcc);
    }

    public Catalog catalog() { return catalog; }

    static String rowKey(String table, String pk)          { return table + "/" + pk; }
    static String indexPrefix(String table, String column) { return "__idx__/" + table + "/" + column + "/"; }
    static String indexKey(String table, String col, String value, String pk) {
        return indexPrefix(table, col) + value + SEP + pk;
    }

    // ---- statement entry point + transaction control ----------------------

    public String execute(String sql) throws IOException {
        String kw = sql.trim().replaceAll(";\\s*$", "");
        if (kw.equalsIgnoreCase("BEGIN")) {
            if (current != null) return "ERROR: already in a transaction";
            current = mvcc.begin();
            return "OK: BEGIN (snapshot ts=" + current.readTs() + ")";
        }
        if (kw.equalsIgnoreCase("COMMIT")) {
            if (current == null) return "ERROR: no active transaction";
            try { long ts = current.commit(); current = null; return "OK: COMMIT (ts=" + ts + ")"; }
            catch (ConflictException ce) { current = null; return "ERROR: " + ce.getMessage(); }
        }
        if (kw.equalsIgnoreCase("ROLLBACK")) {
            if (current == null) return "ERROR: no active transaction";
            current.rollback(); current = null; return "OK: ROLLBACK";
        }

        boolean autoCommit = (current == null);
        Transaction tx = autoCommit ? mvcc.begin() : current;
        String result;
        try {
            result = dispatch(sql, tx);
        } catch (ConflictException ce) {
            if (autoCommit) tx.rollback(); else { current.rollback(); current = null; }
            return "ERROR: " + ce.getMessage();
        }
        if (autoCommit) {
            if (result.startsWith("ERROR") || !tx.hasWrites()) { tx.rollback(); return result; }
            try { tx.commit(); } catch (ConflictException ce) { return "ERROR: " + ce.getMessage(); }
        }
        return result;
    }

    private String dispatch(String sql, Transaction tx) throws IOException {
        Matcher ci = CREATE_INDEX.matcher(sql);
        if (ci.matches()) return createIndex(ci.group(1), ci.group(2), ci.group(3), tx);
        Matcher di = DROP_INDEX.matcher(sql);
        if (di.matches()) return dropIndex(di.group(1), tx);

        Statement stmt;
        try { stmt = parser.parse(sql); }
        catch (Exception e) { return "ERROR: parse: " + e.getMessage(); }
        switch (stmt.type()) {
            case "CREATE_TABLE": return createTable((CreateTableStatement) stmt, tx);
            case "DROP_TABLE":   return dropTable((DropTableStatement) stmt, tx);
            case "INSERT":       return insert((InsertStatement) stmt, tx);
            case "SELECT":       return select((SelectStatement) stmt, tx);
            case "DELETE":       return delete((DeleteStatement) stmt, tx);
            default:             return "ERROR: " + stmt.type() + " not supported yet";
        }
    }

    // ---- DDL --------------------------------------------------------------

    private String createTable(CreateTableStatement s, Transaction tx) {
        if (catalog.hasTable(s.table)) return "ERROR: table already exists: " + s.table;
        List<Column> cols = new ArrayList<>();
        for (ColumnDef cd : s.columns) cols.add(new Column(cd.name, cd.type));
        TableSchema schema = new TableSchema(s.table, cols);
        catalog.createTable(schema, tx);
        return "OK: created table " + s.table + " (PK=" + schema.primaryKey().name + ")";
    }

    private String dropTable(DropTableStatement s, Transaction tx) throws IOException {
        if (!catalog.hasTable(s.table)) return "ERROR: no such table: " + s.table;
        for (IndexDef idx : catalog.indexesForTable(s.table)) {
            deletePrefix(indexPrefix(s.table, idx.column), tx);
            catalog.dropIndex(idx.name, tx);
        }
        int deleted = 0;
        for (Map.Entry<String, String> e : new ArrayList<>(scanTable(s.table, tx))) {
            tx.delete(e.getKey());
            deleted++;
        }
        catalog.dropTable(s.table, tx);
        return "OK: dropped table " + s.table + " (" + deleted + " rows removed)";
    }

    private String createIndex(String name, String table, String column, Transaction tx) throws IOException {
        TableSchema schema = catalog.getTable(table);
        if (schema == null)                 return "ERROR: no such table: " + table;
        if (schema.columnIndex(column) < 0) return "ERROR: no such column: " + column;
        if (catalog.hasIndex(name))         return "ERROR: index already exists: " + name;
        if (catalog.indexForColumn(table, column) != null)
            return "ERROR: column already indexed: " + table + "(" + column + ")";

        catalog.createIndex(new IndexDef(name, table, column), tx);
        int ci = schema.columnIndex(column);
        String type = schema.columnType(column);
        int built = 0;
        for (Map.Entry<String, String> e : scanTable(table, tx)) {
            List<String> row = RowCodec.decode(e.getValue());
            tx.put(indexKey(table, column, TypeCodec.encode(type, row.get(ci)), row.get(0)), row.get(0));
            built++;
        }
        return "OK: created index " + name + " ON " + table + "(" + column + ") — " + built + " entries";
    }

    private String dropIndex(String name, Transaction tx) throws IOException {
        IndexDef def = catalog.getIndex(name);
        if (def == null) return "ERROR: no such index: " + name;
        deletePrefix(indexPrefix(def.table, def.column), tx);
        catalog.dropIndex(name, tx);
        return "OK: dropped index " + name;
    }

    // ---- DML --------------------------------------------------------------

    private String insert(InsertStatement s, Transaction tx) throws IOException {
        TableSchema schema = catalog.getTable(s.table);
        if (schema == null) return "ERROR: no such table: " + s.table;
        List<String> row = buildRow(schema, s.columns, s.values);
        if (row == null) return "ERROR: column/value mismatch for INSERT into " + s.table;

        String pk = row.get(0);
        if (tx.get(rowKey(s.table, pk)) != null) return "ERROR: duplicate primary key: " + pk;

        tx.put(rowKey(s.table, pk), RowCodec.encode(row));
        for (IndexDef idx : catalog.indexesForTable(s.table)) {
            String enc = TypeCodec.encode(schema.columnType(idx.column), row.get(schema.columnIndex(idx.column)));
            tx.put(indexKey(s.table, idx.column, enc, pk), pk);
        }
        return "OK: 1 row inserted";
    }

    private String select(SelectStatement s, Transaction tx) throws IOException {
        TableSchema schema = catalog.getTable(s.table);
        if (schema == null) return "ERROR: no such table: " + s.table;

        List<String> proj = (s.columns.size() == 1 && s.columns.get(0).equals("*"))
                ? schema.columnNames() : s.columns;
        for (String c : proj) if (schema.columnIndex(c) < 0) return "ERROR: no such column: " + c;

        List<List<String>> rows = new ArrayList<>();
        String plan;
        IndexDef idx = (s.where != null) ? catalog.indexForColumn(s.table, s.where.column) : null;
        if (idx != null && isRangeOp(s.where.operator)) {
            rows = indexScan(schema, idx, s.where, tx);
            plan = "index-scan on " + s.where.column + " (" + idx.name + ")";
        } else {
            for (Map.Entry<String, String> e : scanTable(s.table, tx)) {
                List<String> row = RowCodec.decode(e.getValue());
                if (s.where == null || evalWhere(s.where, schema, row)) rows.add(row);
            }
            plan = "full-scan" + (s.where != null && idx == null ? " (no index on " + s.where.column + ")" : "");
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

    private String delete(DeleteStatement s, Transaction tx) throws IOException {
        TableSchema schema = catalog.getTable(s.table);
        if (schema == null) return "ERROR: no such table: " + s.table;

        List<Map.Entry<String, List<String>>> victims = new ArrayList<>();
        for (Map.Entry<String, String> e : scanTable(s.table, tx)) {
            List<String> row = RowCodec.decode(e.getValue());
            if (s.where == null || evalWhere(s.where, schema, row)) victims.add(Map.entry(e.getKey(), row));
        }
        for (Map.Entry<String, List<String>> v : victims) {
            tx.delete(v.getKey());
            String pk = v.getValue().get(0);
            for (IndexDef idx : catalog.indexesForTable(s.table)) {
                String enc = TypeCodec.encode(schema.columnType(idx.column),
                                              v.getValue().get(schema.columnIndex(idx.column)));
                tx.delete(indexKey(s.table, idx.column, enc, pk));
            }
        }
        int n = victims.size();
        return "OK: " + n + (n == 1 ? " row deleted" : " rows deleted");
    }

    // ---- scan + index helpers --------------------------------------------

    private List<Map.Entry<String, String>> scanTable(String table, Transaction tx) throws IOException {
        return tx.scan(table + "/", table + "/" + Character.MAX_VALUE);
    }

    private void deletePrefix(String prefix, Transaction tx) throws IOException {
        for (Map.Entry<String, String> e : new ArrayList<>(tx.scan(prefix, prefix + Character.MAX_VALUE))) {
            tx.delete(e.getKey());
        }
    }

    private List<List<String>> indexScan(TableSchema schema, IndexDef idx, WhereClause w, Transaction tx) throws IOException {
        String table = schema.name;
        String prefix = indexPrefix(table, idx.column);
        String v = TypeCodec.encode(schema.columnType(idx.column), unquote(w.value));
        String lo, hi;
        switch (w.operator) {
            case "=":            lo = prefix + v; hi = prefix + v + Character.MAX_VALUE; break;
            case ">": case ">=": lo = prefix + v; hi = prefix + Character.MAX_VALUE;     break;
            case "<": case "<=": lo = prefix;     hi = prefix + v + Character.MAX_VALUE; break;
            default:             lo = prefix;     hi = prefix + Character.MAX_VALUE;
        }
        List<List<String>> rows = new ArrayList<>();
        for (Map.Entry<String, String> e : tx.scan(lo, hi)) {
            String key = e.getKey();
            int sep = key.indexOf((char) 0, prefix.length());
            if (sep < 0) continue;
            String encColValue = key.substring(prefix.length(), sep);
            if (!opMatch(encColValue, w.operator, v)) continue;
            String rowVal = tx.get(rowKey(table, e.getValue()));
            if (rowVal != null) rows.add(RowCodec.decode(rowVal));
        }
        return rows;
    }

    // ---- pure helpers (unchanged) ----------------------------------------

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
        int cmp = TypeCodec.compare(schema.columns.get(ci).type, left, right);
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
    //  DEMO — SQL through MVCC, plus snapshot isolation across two sessions    //
    // ===================================================================== //

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("litedb_rel_demo_");
        LSMEngine lsm = new LSMEngine(dir.toString());
        MVCCEngine mvcc = new MVCCEngine(lsm);
        RelationalEngine db = new RelationalEngine(mvcc);

        System.out.println("=== SQL through MVCC (auto-commit) ===\n");
        String[] script = {
            "CREATE TABLE nums (id INT, n INT)",
            "INSERT INTO nums (id, n) VALUES (1, 5)",
            "INSERT INTO nums (id, n) VALUES (2, 100)",
            "INSERT INTO nums (id, n) VALUES (3, 9)",
            "CREATE INDEX idx_n ON nums(n)",
            "SELECT * FROM nums WHERE n > 9",     // index-scan, typed
            "SELECT * FROM nums ORDER BY n",      // typed order
        };
        for (String sql : script) { System.out.println("db> " + sql); System.out.println(db.execute(sql) + "\n"); }

        System.out.println("=== Snapshot isolation across two sessions ===\n");
        RelationalEngine sessionA = new RelationalEngine(mvcc);
        RelationalEngine sessionB = new RelationalEngine(mvcc);
        System.out.println("A> BEGIN                : " + sessionA.execute("BEGIN"));
        System.out.println("A> SELECT (snapshot)    :\n" + sessionA.execute("SELECT id FROM nums ORDER BY id"));
        System.out.println("B> INSERT id=4 (commits): " + sessionB.execute("INSERT INTO nums (id, n) VALUES (4, 7)"));
        System.out.println("A> SELECT again         :\n" + sessionA.execute("SELECT id FROM nums ORDER BY id")
                + "   <- A still on its snapshot, no id=4");
        System.out.println("A> COMMIT               : " + sessionA.execute("COMMIT"));
        System.out.println("A> SELECT (new snapshot):\n" + sessionA.execute("SELECT id FROM nums ORDER BY id")
                + "   <- now sees id=4");

        lsm.close();
        Files.walk(dir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
        System.out.println("\n[MVCC-backed SQL demo complete]");
    }
}
