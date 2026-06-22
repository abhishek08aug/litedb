package com.litedb.wal;

/**
 * A single entry in the Write-Ahead Log.
 *
 * Each entry records one operation (SET or DELETE) with a monotonically
 * increasing sequence number for ordering and crash recovery.
 */
public class WALEntry {

    public static final String TOMBSTONE = "__DELETED__";

    public final int    sequence;   // monotonically increasing ID
    public final String operation;  // "SET" or "DELETE"
    public final String key;
    public final String value;      // null for DELETE

    public WALEntry(int sequence, String operation, String key, String value) {
        this.sequence  = sequence;
        this.operation = operation;
        this.key       = key;
        this.value     = value;
    }

    /** Serialize to a compact JSON-like string for storage. */
    public String toJson() {
        String escapedKey   = escapeJson(key);
        String escapedValue = value == null ? "null" : "\"" + escapeJson(value) + "\"";
        return "{\"seq\":" + sequence
             + ",\"op\":\"" + operation + "\""
             + ",\"key\":\"" + escapedKey + "\""
             + ",\"val\":" + escapedValue + "}";
    }

    /** Deserialize from the JSON string produced by toJson(). */
    public static WALEntry fromJson(String json) {
        int seq       = parseInt(extractField(json, "seq"));
        String op     = extractStringField(json, "op");
        String key    = extractStringField(json, "key");
        String valRaw = extractRawField(json, "val");
        String value  = valRaw.equals("null") ? null : unquote(valRaw);
        return new WALEntry(seq, op, key, value);
    }

    // ------------------------------------------------------------------ //
    //  Minimal JSON helpers (no external deps)                            //
    // ------------------------------------------------------------------ //

    private static String extractField(String json, String name) {
        String search = "\"" + name + "\":";
        int start = json.indexOf(search);
        if (start < 0) throw new IllegalArgumentException("Field not found: " + name);
        start += search.length();
        int end = json.indexOf(',', start);
        if (end < 0) end = json.indexOf('}', start);
        return json.substring(start, end).trim();
    }

    private static String extractRawField(String json, String name) {
        // "val" is always the LAST field, and its (escaped) string can itself contain commas or
        // braces — so read to the entry's final '}', not the first ',' (which extractField uses).
        String search = "\"" + name + "\":";
        int start = json.indexOf(search);
        if (start < 0) throw new IllegalArgumentException("Field not found: " + name);
        start += search.length();
        return json.substring(start, json.lastIndexOf('}')).trim();
    }

    private static String extractStringField(String json, String name) {
        return unquote(extractField(json, name));
    }

    private static String unquote(String s) {
        s = s.trim();
        if (s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        return s.replace("\\\"", "\"").replace("\\\\", "\\").replace("\\n", "\n");
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private static int parseInt(String s) {
        return Integer.parseInt(s.trim());
    }

    @Override
    public String toString() {
        return "WALEntry(seq=" + sequence + ", op=" + operation
             + ", key=" + key + ", val=" + value + ")";
    }
}