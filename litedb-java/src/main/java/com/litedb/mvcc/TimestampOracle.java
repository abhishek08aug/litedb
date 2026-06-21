package com.litedb.mvcc;

import java.util.concurrent.atomic.AtomicLong;

/**
 * TimestampOracle — hands out monotonically increasing timestamps used to order transactions.
 *
 * A transaction takes a read timestamp (its snapshot) at begin and a commit timestamp at commit.
 * Commit timestamps are strictly increasing, so "happens-before" is just numeric comparison.
 */
public final class TimestampOracle {

    private final AtomicLong counter;

    public TimestampOracle(long start) {
        this.counter = new AtomicLong(start);
    }

    /** Next strictly-increasing timestamp (used as a commit timestamp). */
    public long next() {
        return counter.incrementAndGet();
    }

    /** The latest issued timestamp (a valid snapshot of all commits so far). */
    public long current() {
        return counter.get();
    }
}
