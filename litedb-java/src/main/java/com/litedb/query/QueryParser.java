package com.litedb.query;

import com.litedb.lsm.LSMEngine;

import java.io.IOException;
import java.util.*;

/**
 * QueryParser — parses and executes LiteDB text commands against an LSMEngine.
 *
 * Supported commands:
 *   SET <key> <value>       — insert or update
 *   GET <key>               — point lookup
 *   DELETE <key>            — delete (tombstone)
 *   SCAN <startKey> <endKey>— range scan
 *   STATS                   — engine statistics
 *   PING                    — health check
 *   QUIT / EXIT             — close connection
 *   HELP                    — list commands
 */
public class QueryParser {

    private final LSMEngine engine;

    public QueryParser(LSMEngine engine) {
        this.engine = engine;
    }

    // ------------------------------------------------------------------ //
    //  Entry point                                                        //
    // ------------------------------------------------------------------ //

    public QueryResult execute(String rawCommand) {
        if (rawCommand == null) return error("Null command");
        rawCommand = rawCommand.strip();
        if (rawCommand.isEmpty()) return error("Empty command");

        List<String> tokens;
        try {
            tokens = shellSplit(rawCommand);
        } catch (IllegalArgumentException e) {
            return error("Parse error: " + e.getMessage());
        }

        if (tokens.isEmpty()) return error("Empty command");

        String command = tokens.get(0).toUpperCase();
        List<String> args = tokens.subList(1, tokens.size());

        try {
            return dispatch(command, args);
        } catch (Exception e) {
            return error("Execution error: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------ //
    //  Dispatch                                                           //
    // ------------------------------------------------------------------ //

    private QueryResult dispatch(String command, List<String> args) throws IOException {
        switch (command) {
            case "SET":    return cmdSet(args);
            case "GET":    return cmdGet(args);
            case "DELETE":
            case "DEL":    return cmdDelete(args);
            case "SCAN":   return cmdScan(args);
            case "STATS":  return cmdStats();
            case "PING":   return new QueryResult(QueryResult.Status.PONG);
            case "QUIT":
            case "EXIT":   return new QueryResult(QueryResult.Status.QUIT);
            case "HELP":   return cmdHelp();
            default:       return error("Unknown command: '" + command + "'. Type HELP for commands.");
        }
    }

    // ------------------------------------------------------------------ //
    //  Command handlers                                                   //
    // ------------------------------------------------------------------ //

    private QueryResult cmdSet(List<String> args) throws IOException {
        if (args.size() < 2) return error("Usage: SET <key> <value>");
        String key   = args.get(0);
        if (!validKey(key)) return error("Invalid key: '" + key + "'. Keys cannot contain spaces.");
        String value = String.join(" ", args.subList(1, args.size()));
        engine.set(key, value);
        return new QueryResult(QueryResult.Status.OK);
    }

    private QueryResult cmdGet(List<String> args) throws IOException {
        if (args.size() != 1) return error("Usage: GET <key>");
        String value = engine.get(args.get(0));
        if (value == null) return new QueryResult(QueryResult.Status.NOT_FOUND);
        return new QueryResult(QueryResult.Status.VALUE, value);
    }

    private QueryResult cmdDelete(List<String> args) throws IOException {
        if (args.size() != 1) return error("Usage: DELETE <key>");
        engine.delete(args.get(0));
        return new QueryResult(QueryResult.Status.OK);
    }

    private QueryResult cmdScan(List<String> args) throws IOException {
        if (args.size() != 2) return error("Usage: SCAN <start_key> <end_key>");
        String startKey = args.get(0), endKey = args.get(1);
        if (startKey.compareTo(endKey) > 0) return error("start_key must be <= end_key");
        List<Map.Entry<String, String>> entries = engine.scan(startKey, endKey);
        List<String[]> rows = new ArrayList<>();
        for (Map.Entry<String, String> e : entries) rows.add(new String[]{e.getKey(), e.getValue()});
        return new QueryResult(QueryResult.Status.SCAN, null, rows);
    }

    private QueryResult cmdStats() {
        Map<String, Object> stats = engine.stats();
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> e : stats.entrySet()) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(e.getKey()).append("=").append(e.getValue());
        }
        return new QueryResult(QueryResult.Status.VALUE, sb.toString());
    }

    private QueryResult cmdHelp() {
        return new QueryResult(QueryResult.Status.VALUE,
            "Commands: SET <key> <value> | GET <key> | DELETE <key> | " +
            "SCAN <start> <end> | STATS | PING | QUIT");
    }

    private static QueryResult error(String msg) {
        return new QueryResult(QueryResult.Status.ERROR, msg);
    }

    private static boolean validKey(String key) {
        return key != null && !key.isEmpty() && !key.contains(" ");
    }

    // ------------------------------------------------------------------ //
    //  Shell-style tokenizer (handles quoted strings)                    //
    // ------------------------------------------------------------------ //

    /**
     * Split a command string respecting double-quoted tokens.
     * e.g.: SET name "Alice Smith" → ["SET", "name", "Alice Smith"]
     */
    static List<String> shellSplit(String s) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ' ' && !inQuotes) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (inQuotes) throw new IllegalArgumentException("Unclosed quote");
        if (current.length() > 0) tokens.add(current.toString());
        return tokens;
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) throws Exception {
        java.nio.file.Path tmpDir = java.nio.file.Files.createTempDirectory("litedb_qp_demo_");
        LSMEngine engine = new LSMEngine(tmpDir.toString());
        QueryParser parser = new QueryParser(engine);

        System.out.println("============================================================");
        System.out.println("QUERY PARSER DEMO");
        System.out.println("============================================================\n");

        String[] commands = {
            "PING",
            "SET name Alice",
            "SET age 30",
            "SET city \"New York\"",
            "SET country USA",
            "GET name",
            "GET age",
            "GET missing_key",
            "SCAN a z",
            "SCAN c d",
            "DELETE age",
            "GET age",
            "STATS",
            "HELP",
            "BADCOMMAND foo",
            "SET",
            "GET",
            "SCAN only_one_arg",
        };

        for (String cmd : commands) {
            QueryResult result = parser.execute(cmd);
            String wire = result.toWire().stripTrailing();
            System.out.printf("  > %-35s → %s%n", cmd, wire);
        }

        engine.close();
        deleteDir(tmpDir.toFile());
        System.out.println("\n[Done] Query parser demo complete.");
    }

    private static void deleteDir(java.io.File dir) {
        if (dir.isDirectory()) { for (java.io.File f : dir.listFiles()) deleteDir(f); }
        dir.delete();
    }
}