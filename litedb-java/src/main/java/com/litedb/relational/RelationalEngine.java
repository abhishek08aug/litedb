package com.litedb.relational;

import com.litedb.engine.StorageEngine;
import com.litedb.lsm.LSMEngine;
import com.litedb.sql.SQLParser;
import com.litedb.sql.SQLParser.ColumnDef;
import com.litedb.sql.SQLParser.CreateTableStatement;
import com.litedb.sql.SQLParser.DropTableStatement;
import com.litedb.sql.SQLParser.Statement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * RelationalEngine — a thin SQL/relational layer over a {@link StorageEngine}.
 *
 * It maps tables and rows onto the ordered key-value store via key prefixes (the standard
 * "SQL on KV" technique used by CockroachDB / TiDB / FoundationDB layers):
 *
 *   __catalog__/&lt;table&gt;   -&gt; serialized schema
 *   &lt;table&gt;/&lt;pk&gt;          -&gt; encoded row
 *
 * The primary key is the table's first column (Phase 1 convention).
 *
 * Phase 1 implements the schema catalog and DDL (CREATE TABLE / DROP TABLE). DML
 * (INSERT/SELECT/UPDATE/DELETE), secondary indexes, a query planner, typed encoding, and
 * transactional atomicity are later phases.
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

    /** Storage key for a row, given its primary-key value. */
    static String rowKey(String table, String primaryKeyValue) {
        return table + "/" + primaryKeyValue;
    }

    /** Execute one SQL statement; returns a human-readable result line. */
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
            default:
                return "ERROR: " + stmt.type() + " not supported yet (Phase 1 = DDL only)";
        }
    }

    private String createTable(CreateTableStatement s) throws IOException {
        if (catalog.hasTable(s.table)) {
            return "ERROR: table already exists: " + s.table;
        }
        List<Column> cols = new ArrayList<>();
        for (ColumnDef cd : s.columns) {
            cols.add(new Column(cd.name, cd.type));
        }
        TableSchema schema = new TableSchema(s.table, cols);
        catalog.createTable(schema);
        return "OK: created table " + s.table + " (PK=" + schema.primaryKey().name + ")";
    }

    private String dropTable(DropTableStatement s) throws IOException {
        if (!catalog.hasTable(s.table)) {
            return "ERROR: no such table: " + s.table;
        }
        // remove all rows under the table prefix, then the catalog entry
        String lo = s.table + "/";
        String hi = s.table + "/" + Character.MAX_VALUE;
        int deleted = 0;
        for (Map.Entry<String, String> e : new ArrayList<>(engine.scan(lo, hi))) {
            engine.delete(e.getKey());
            deleted++;
        }
        catalog.dropTable(s.table);
        return "OK: dropped table " + s.table + " (" + deleted + " rows removed)";
    }

    // ===================================================================== //
    //  DEMO — Phase 1: catalog, DDL, persistence, row codec                   //
    // ===================================================================== //

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("litedb_rel_demo_");
        System.out.println("=== Phase 1: schema, DDL, key namespacing ===\n");

        // Run 1 — create tables
        LSMEngine e1 = new LSMEngine(dir.toString());
        RelationalEngine db1 = new RelationalEngine(e1);
        System.out.println(db1.execute("CREATE TABLE users (id INT, name TEXT, age INT)"));
        System.out.println(db1.execute("CREATE TABLE orders (id INT, user_id INT, total FLOAT)"));
        System.out.println(db1.execute("CREATE TABLE users (id INT)"));   // duplicate -> error
        System.out.print("Tables now: ");
        for (String t : db1.catalog().tableNames()) System.out.print(t + " ");
        System.out.println("\n");
        e1.close();

        // Run 2 — reopen: did the catalog persist?
        LSMEngine e2 = new LSMEngine(dir.toString());
        RelationalEngine db2 = new RelationalEngine(e2);
        System.out.print("After reopen, tables: ");
        for (String t : db2.catalog().tableNames()) System.out.print(t + " ");
        System.out.println();
        System.out.println("users schema: " + db2.catalog().getTable("users"));
        System.out.println(db2.execute("DROP TABLE orders"));
        System.out.print("After DROP orders: ");
        for (String t : db2.catalog().tableNames()) System.out.print(t + " ");
        System.out.println("\n");
        e2.close();

        // RowCodec round-trip (values with spaces / colons survive)
        List<String> row = Arrays.asList("42", "Alice: Smith", "30");
        String enc = RowCodec.encode(row);
        System.out.println("RowCodec: " + row + "  ->  " + enc + "  ->  " + RowCodec.decode(enc));

        // cleanup
        Files.walk(dir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
        System.out.println("\n[Phase 1 demo complete]");
    }
}
