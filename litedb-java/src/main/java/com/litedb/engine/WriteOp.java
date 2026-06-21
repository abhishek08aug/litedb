package com.litedb.engine;

/**
 * WriteOp — one put or delete within an atomic write batch (see {@link StorageEngine#writeBatch}).
 */
public final class WriteOp {
    public final String key;
    public final String value;   // null for a delete
    public final boolean delete;

    private WriteOp(String key, String value, boolean delete) {
        this.key = key;
        this.value = value;
        this.delete = delete;
    }

    public static WriteOp put(String key, String value) {
        return new WriteOp(key, value, false);
    }

    public static WriteOp delete(String key) {
        return new WriteOp(key, null, true);
    }
}
