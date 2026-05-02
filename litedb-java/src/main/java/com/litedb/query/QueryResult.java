package com.litedb.query;

import java.util.ArrayList;
import java.util.List;

/**
 * Structured result from executing a LiteDB command.
 */
public class QueryResult {

    public enum Status { OK, VALUE, NOT_FOUND, ERROR, PONG, SCAN, QUIT }

    public final Status            status;
    public final String            value;   // for VALUE / ERROR
    public final List<String[]>    rows;    // for SCAN: each element is [key, value]

    public QueryResult(Status status) {
        this(status, null, null);
    }

    public QueryResult(Status status, String value) {
        this(status, value, null);
    }

    public QueryResult(Status status, String value, List<String[]> rows) {
        this.status = status;
        this.value  = value;
        this.rows   = rows != null ? rows : new ArrayList<>();
    }

    /** Serialize to wire protocol string. */
    public String toWire() {
        switch (status) {
            case VALUE:     return "VALUE " + value + "\n";
            case ERROR:     return "ERROR " + value + "\n";
            case SCAN: {
                StringBuilder sb = new StringBuilder("SCAN_START\n");
                for (String[] row : rows) sb.append("ROW ").append(row[0]).append(" ").append(row[1]).append("\n");
                sb.append("SCAN_END ").append(rows.size()).append("\n");
                return sb.toString();
            }
            default:        return status.name() + "\n";
        }
    }

    @Override
    public String toString() {
        if (status == Status.VALUE) return "QueryResult(VALUE, " + value + ")";
        if (status == Status.SCAN)  return "QueryResult(SCAN, " + rows.size() + " rows)";
        return "QueryResult(" + status + ")";
    }
}