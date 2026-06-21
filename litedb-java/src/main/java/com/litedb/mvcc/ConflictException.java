package com.litedb.mvcc;

/**
 * ConflictException — thrown when a transaction's commit conflicts with a concurrent commit
 * (a write-write conflict under snapshot isolation). The transaction must abort and retry.
 */
public class ConflictException extends RuntimeException {
    public ConflictException(String key, long conflictTs, long readTs) {
        super("write-write conflict on key '" + key + "': committed at ts=" + conflictTs
                + " after this txn's snapshot ts=" + readTs + " (abort and retry)");
    }
}
