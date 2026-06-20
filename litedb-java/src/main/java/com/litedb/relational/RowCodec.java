package com.litedb.relational;

import java.util.ArrayList;
import java.util.List;

/**
 * RowCodec — encodes a list of string fields into a single value, and back.
 *
 * Format: each field is written as "&lt;length&gt;:&lt;chars&gt;". Length-prefixing makes the
 * encoding collision-free for any field content (values may contain ':', spaces, etc.).
 *
 * Used both for table rows (column values in schema order) and for serializing schemas
 * into the catalog. Phase 1 keeps everything as strings; typed encoding is a later phase.
 */
public final class RowCodec {

    public static String encode(List<String> fields) {
        StringBuilder sb = new StringBuilder();
        for (String f : fields) {
            sb.append(f.length()).append(':').append(f);
        }
        return sb.toString();
    }

    public static List<String> decode(String s) {
        List<String> out = new ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            int colon = s.indexOf(':', i);
            int len = Integer.parseInt(s.substring(i, colon));
            int start = colon + 1;
            out.add(s.substring(start, start + len));
            i = start + len;
        }
        return out;
    }
}
