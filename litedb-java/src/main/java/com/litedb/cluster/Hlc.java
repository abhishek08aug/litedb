package com.litedb.cluster;

import java.time.Instant;

/**
 * Hlc — Hybrid Logical Clock. A monotonic, globally-comparable timestamp anchored to the wall clock
 * (epoch nanoseconds, via {@link Instant}) so timestamps are ordered across processes, but never
 * goes backwards and bumps by one tick to break exact ties. On one machine all processes read the
 * same clock, so a begin() and a later write are strictly ordered — what snapshot isolation needs.
 * Epoch-nanos (~1.8e18) fit in 63 bits, so a timestamp slots straight into the MVCC version key.
 */
public final class Hlc {
    private long last = 0;

    private static long wallNanos() {
        Instant t = Instant.now();
        return t.getEpochSecond() * 1_000_000_000L + t.getNano();
    }

    public synchronized long now() {
        long wall = wallNanos();
        last = wall > last ? wall : last + 1;
        return last;
    }

    public synchronized long update(long remoteTs) {
        long wall = wallNanos();
        last = Math.max(Math.max(last + 1, remoteTs + 1), wall);
        return last;
    }
}
