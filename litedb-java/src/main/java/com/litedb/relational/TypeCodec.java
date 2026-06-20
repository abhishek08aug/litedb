package com.litedb.relational;

/**
 * TypeCodec — order-preserving ("sortable") encoding of typed column values.
 *
 * The storage layer compares keys as plain strings (lexicographically). For INT/FLOAT
 * columns that is wrong: "10" &lt; "9" and "-5" &gt; "3" lexicographically. This codec maps a
 * typed value to a string whose LEXICOGRAPHIC order equals the value's NATURAL order, so
 * index range-scans and WHERE comparisons become correct without changing the storage engine.
 *
 * Techniques:
 *   INT   — take the 64-bit two's-complement long, flip the sign bit (x ^ MIN_VALUE) so the
 *           signed range maps monotonically onto the unsigned range, then format as a
 *           fixed-width 16-hex string (fixed width + hex digits sort correctly).
 *   FLOAT — IEEE-754 bits sort correctly for positives but reversed for negatives; the
 *           standard fix is: flip the sign bit for positives, flip all bits for negatives
 *           (bits ^= (bits>>63) | MIN_VALUE), then format as fixed-width hex.
 *   TEXT/BOOL/other — natural string order is already correct, so encode as-is.
 *
 * Rows are still stored human-readably; only index keys and comparisons use this encoding.
 * Malformed numerics fall back to the raw string (a teaching-scope limitation).
 */
public final class TypeCodec {

    public static String encode(String type, String value) {
        if (value == null) value = "";
        try {
            switch (type == null ? "" : type.toUpperCase()) {
                case "INT": {
                    long x = Long.parseLong(value.trim());
                    return String.format("%016x", x ^ Long.MIN_VALUE);
                }
                case "FLOAT":
                case "DOUBLE": {
                    long bits = Double.doubleToLongBits(Double.parseDouble(value.trim()));
                    bits ^= (bits >> 63) | Long.MIN_VALUE;
                    return String.format("%016x", bits);
                }
                default:
                    return value;   // TEXT / BOOL: natural lexicographic order
            }
        } catch (NumberFormatException e) {
            return value;            // malformed numeric -> raw fallback
        }
    }

    /** Compare two raw values of a column type, in natural (typed) order. */
    public static int compare(String type, String a, String b) {
        return encode(type, a).compareTo(encode(type, b));
    }
}
