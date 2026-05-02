package com.litedb.sql;

import java.util.*;

/**
 * SQLParser — hand-written recursive-descent SQL parser.
 *
 * CONCEPT:
 *   A SQL parser converts a text query into an Abstract Syntax Tree (AST).
 *   The AST is then executed by a query engine.
 *
 *   Supported statements:
 *     SELECT col,... FROM table [WHERE col op val] [ORDER BY col [ASC|DESC]] [LIMIT n]
 *     INSERT INTO table (col,...) VALUES (val,...)
 *     UPDATE table SET col=val,... [WHERE col op val]
 *     DELETE FROM table [WHERE col op val]
 *     CREATE TABLE table (col type,...)
 *     DROP TABLE table
 *
 *   Parsing strategy: recursive descent
 *     - Tokenize the input into tokens
 *     - Parse tokens top-down, one grammar rule per method
 *     - Build an AST node for each statement
 *
 *   This is how MySQL, PostgreSQL, and SQLite parse SQL.
 *   Production parsers use ANTLR or Bison-generated parsers for full SQL.
 */
public class SQLParser {

    // ------------------------------------------------------------------ //
    //  AST node types                                                     //
    // ------------------------------------------------------------------ //

    public static abstract class Statement {
        public abstract String type();
    }

    public static class SelectStatement extends Statement {
        public List<String> columns;   // ["*"] or ["col1","col2"]
        public String       table;
        public WhereClause  where;     // nullable
        public String       orderBy;   // nullable
        public boolean      orderDesc;
        public int          limit;     // -1 = no limit

        @Override public String type() { return "SELECT"; }

        @Override public String toString() {
            return "SELECT " + columns + " FROM " + table
                 + (where   != null ? " WHERE " + where   : "")
                 + (orderBy != null ? " ORDER BY " + orderBy + (orderDesc ? " DESC" : " ASC") : "")
                 + (limit   >= 0    ? " LIMIT " + limit : "");
        }
    }

    public static class InsertStatement extends Statement {
        public String              table;
        public List<String>        columns;
        public List<String>        values;

        @Override public String type() { return "INSERT"; }

        @Override public String toString() {
            return "INSERT INTO " + table + " " + columns + " VALUES " + values;
        }
    }

    public static class UpdateStatement extends Statement {
        public String              table;
        public Map<String,String>  assignments; // col → value
        public WhereClause         where;

        @Override public String type() { return "UPDATE"; }

        @Override public String toString() {
            return "UPDATE " + table + " SET " + assignments
                 + (where != null ? " WHERE " + where : "");
        }
    }

    public static class DeleteStatement extends Statement {
        public String      table;
        public WhereClause where;

        @Override public String type() { return "DELETE"; }

        @Override public String toString() {
            return "DELETE FROM " + table + (where != null ? " WHERE " + where : "");
        }
    }

    public static class CreateTableStatement extends Statement {
        public String              table;
        public List<ColumnDef>     columns;

        @Override public String type() { return "CREATE_TABLE"; }

        @Override public String toString() {
            return "CREATE TABLE " + table + " " + columns;
        }
    }

    public static class DropTableStatement extends Statement {
        public String table;

        @Override public String type() { return "DROP_TABLE"; }

        @Override public String toString() { return "DROP TABLE " + table; }
    }

    public static class WhereClause {
        public String column;
        public String operator; // =, !=, <, >, <=, >=, LIKE
        public String value;

        public WhereClause(String column, String operator, String value) {
            this.column   = column;
            this.operator = operator;
            this.value    = value;
        }

        @Override public String toString() {
            return column + " " + operator + " " + value;
        }
    }

    public static class ColumnDef {
        public String name;
        public String type; // INT, TEXT, FLOAT, BOOL

        public ColumnDef(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @Override public String toString() { return name + " " + type; }
    }

    // ------------------------------------------------------------------ //
    //  Tokenizer                                                          //
    // ------------------------------------------------------------------ //

    private List<String> tokens;
    private int          pos;

    private static List<String> tokenize(String sql) {
        List<String> tokens = new ArrayList<>();
        int i = 0;
        sql = sql.trim();
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            if (c == '\'') {
                // String literal
                int j = i + 1;
                while (j < sql.length() && sql.charAt(j) != '\'') j++;
                tokens.add("'" + sql.substring(i + 1, j) + "'");
                i = j + 1;
            } else if (c == '(' || c == ')' || c == ',' || c == ';') {
                tokens.add(String.valueOf(c));
                i++;
            } else if (c == '<' || c == '>' || c == '!' || c == '=') {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '=') {
                    tokens.add(sql.substring(i, i + 2));
                    i += 2;
                } else {
                    tokens.add(String.valueOf(c));
                    i++;
                }
            } else if (c == '*') {
                tokens.add("*");
                i++;
            } else {
                int j = i;
                while (j < sql.length() && !Character.isWhitespace(sql.charAt(j))
                       && sql.charAt(j) != '(' && sql.charAt(j) != ')'
                       && sql.charAt(j) != ',' && sql.charAt(j) != ';'
                       && sql.charAt(j) != '=') {
                    j++;
                }
                tokens.add(sql.substring(i, j));
                i = j;
            }
        }
        return tokens;
    }

    // ------------------------------------------------------------------ //
    //  Parser entry point                                                 //
    // ------------------------------------------------------------------ //

    public Statement parse(String sql) {
        this.tokens = tokenize(sql);
        this.pos    = 0;
        String keyword = peek().toUpperCase();
        switch (keyword) {
            case "SELECT": return parseSelect();
            case "INSERT": return parseInsert();
            case "UPDATE": return parseUpdate();
            case "DELETE": return parseDelete();
            case "CREATE": return parseCreate();
            case "DROP":   return parseDrop();
            default: throw new ParseException("Unknown statement: " + keyword);
        }
    }

    // ------------------------------------------------------------------ //
    //  Statement parsers                                                  //
    // ------------------------------------------------------------------ //

    private SelectStatement parseSelect() {
        SelectStatement stmt = new SelectStatement();
        stmt.limit = -1;
        consume("SELECT");
        stmt.columns = parseColumnList();
        consume("FROM");
        stmt.table = consume();
        if (peekIs("WHERE")) {
            consume("WHERE");
            stmt.where = parseWhere();
        }
        if (peekIs("ORDER")) {
            consume("ORDER");
            consume("BY");
            stmt.orderBy = consume();
            if (peekIs("DESC")) { consume("DESC"); stmt.orderDesc = true; }
            else if (peekIs("ASC")) { consume("ASC"); }
        }
        if (peekIs("LIMIT")) {
            consume("LIMIT");
            stmt.limit = Integer.parseInt(consume());
        }
        return stmt;
    }

    private InsertStatement parseInsert() {
        InsertStatement stmt = new InsertStatement();
        consume("INSERT");
        consume("INTO");
        stmt.table = consume();
        consume("(");
        stmt.columns = parseIdentifierList();
        consume(")");
        consume("VALUES");
        consume("(");
        stmt.values = parseValueList();
        consume(")");
        return stmt;
    }

    private UpdateStatement parseUpdate() {
        UpdateStatement stmt = new UpdateStatement();
        consume("UPDATE");
        stmt.table = consume();
        consume("SET");
        stmt.assignments = new LinkedHashMap<>();
        do {
            String col = consume();
            consume("=");
            String val = consumeValue();
            stmt.assignments.put(col, val);
        } while (peekIs(",") && consume(",") != null);
        if (peekIs("WHERE")) {
            consume("WHERE");
            stmt.where = parseWhere();
        }
        return stmt;
    }

    private DeleteStatement parseDelete() {
        DeleteStatement stmt = new DeleteStatement();
        consume("DELETE");
        consume("FROM");
        stmt.table = consume();
        if (peekIs("WHERE")) {
            consume("WHERE");
            stmt.where = parseWhere();
        }
        return stmt;
    }

    private CreateTableStatement parseCreate() {
        CreateTableStatement stmt = new CreateTableStatement();
        consume("CREATE");
        consume("TABLE");
        stmt.table = consume();
        consume("(");
        stmt.columns = new ArrayList<>();
        do {
            String name = consume();
            String type = consume().toUpperCase();
            stmt.columns.add(new ColumnDef(name, type));
        } while (peekIs(",") && consume(",") != null);
        consume(")");
        return stmt;
    }

    private DropTableStatement parseDrop() {
        DropTableStatement stmt = new DropTableStatement();
        consume("DROP");
        consume("TABLE");
        stmt.table = consume();
        return stmt;
    }

    // ------------------------------------------------------------------ //
    //  Clause parsers                                                     //
    // ------------------------------------------------------------------ //

    private WhereClause parseWhere() {
        String col = consume();
        String op  = consumeOperator();
        String val = consumeValue();
        return new WhereClause(col, op, val);
    }

    private List<String> parseColumnList() {
        List<String> cols = new ArrayList<>();
        if (peekIs("*")) { consume("*"); cols.add("*"); return cols; }
        cols.add(consume());
        while (peekIs(",")) { consume(","); cols.add(consume()); }
        return cols;
    }

    private List<String> parseIdentifierList() {
        List<String> ids = new ArrayList<>();
        ids.add(consume());
        while (peekIs(",")) { consume(","); ids.add(consume()); }
        return ids;
    }

    private List<String> parseValueList() {
        List<String> vals = new ArrayList<>();
        vals.add(consumeValue());
        while (peekIs(",")) { consume(","); vals.add(consumeValue()); }
        return vals;
    }

    // ------------------------------------------------------------------ //
    //  Token helpers                                                      //
    // ------------------------------------------------------------------ //

    private String peek() {
        if (pos >= tokens.size()) return "";
        return tokens.get(pos);
    }

    private boolean peekIs(String expected) {
        return peek().equalsIgnoreCase(expected);
    }

    private String consume() {
        if (pos >= tokens.size()) throw new ParseException("Unexpected end of input");
        return tokens.get(pos++);
    }

    private String consume(String expected) {
        String tok = consume();
        if (!tok.equalsIgnoreCase(expected))
            throw new ParseException("Expected '" + expected + "' but got '" + tok + "'");
        return tok;
    }

    private String consumeOperator() {
        String tok = consume();
        switch (tok) {
            case "=": case "!=": case "<": case ">": case "<=": case ">=": return tok;
            default:
                if (tok.equalsIgnoreCase("LIKE")) return "LIKE";
                throw new ParseException("Expected operator but got '" + tok + "'");
        }
    }

    private String consumeValue() {
        String tok = consume();
        if (tok.startsWith("'") && tok.endsWith("'")) {
            return tok.substring(1, tok.length() - 1); // strip quotes
        }
        return tok;
    }

    public static class ParseException extends RuntimeException {
        public ParseException(String msg) { super(msg); }
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) {
        System.out.println("============================================================");
        System.out.println("SQL PARSER DEMO");
        System.out.println("============================================================\n");

        SQLParser parser = new SQLParser();

        String[] queries = {
            "SELECT * FROM users",
            "SELECT id, name, email FROM users WHERE age > 25",
            "SELECT name FROM orders WHERE status = 'active' ORDER BY created_at DESC LIMIT 10",
            "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
            "UPDATE users SET name = 'Bob', age = 31 WHERE id = 1",
            "DELETE FROM users WHERE id = 1",
            "CREATE TABLE products (id INT, name TEXT, price FLOAT, active BOOL)",
            "DROP TABLE temp_data",
        };

        for (String sql : queries) {
            try {
                Statement stmt = parser.parse(sql);
                System.out.println("SQL:  " + sql);
                System.out.println("AST:  [" + stmt.type() + "] " + stmt);
                System.out.println();
            } catch (ParseException e) {
                System.out.println("SQL:  " + sql);
                System.out.println("ERR:  " + e.getMessage());
                System.out.println();
            }
        }

        // Error case
        try {
            parser.parse("BADQUERY foo bar");
        } catch (ParseException e) {
            System.out.println("Error handling: " + e.getMessage());
        }

        System.out.println("\n[Done] SQL parser demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. Tokenizer splits SQL text into tokens (keywords, identifiers, literals)");
        System.out.println("  2. Recursive descent: one method per grammar rule");
        System.out.println("  3. Output is an AST — a structured representation of the query");
        System.out.println("  4. The query executor walks the AST to produce results");
        System.out.println("  5. Production parsers handle 100s of grammar rules (ANTLR/Bison)");
    }
}