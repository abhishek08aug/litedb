package com.litedb.sstable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * SSTableWriter — writes a sorted sequence of (key, value) pairs to disk.
 *
 * SSTable File Format:
 * ┌──────────────────────────────────────────────────────┐
 * │  DATA BLOCK                                          │
 * │  Each entry: [2B key_len][key][4B val_len][value]    │
 * │                                                      │
 * │  INDEX BLOCK (sparse — every 16th entry)             │
 * │  [4B index_data_len][JSON: [[key,offset],...]]       │
 * │                                                      │
 * │  BLOOM FILTER BLOCK                                  │
 * │  [4B bloom_data_len][bloom bytes]                    │
 * │                                                      │
 * │  FOOTER (fixed 28 bytes from end)                    │
 * │  [8B index_offset][4B index_len]                     │
 * │  [4B entry_count][8B bloom_offset][8B magic]         │
 * └──────────────────────────────────────────────────────┘
 */
public class SSTableWriter {

    static final byte[] MAGIC         = "LITEDB01".getBytes(StandardCharsets.UTF_8);
    static final int    INDEX_INTERVAL = 16;
    static final int    FOOTER_SIZE    = 8 + 4 + 4 + 8 + 8; // 32 bytes

    private final Path path;

    public SSTableWriter(String filePath) throws IOException {
        this.path = Paths.get(filePath);
        Files.createDirectories(path.getParent() == null ? Paths.get(".") : path.getParent());
    }

    /**
     * Write sorted items to disk as an SSTable.
     * Returns an SSTableReader for the newly created file.
     */
    public SSTableReader write(List<Map.Entry<String, String>> sortedItems) throws IOException {
        BloomFilter bloom = new BloomFilter();
        List<long[]> index = new ArrayList<>(); // [key_bytes_offset, byte_offset]
        List<String> indexKeys = new ArrayList<>();
        int entryCount = 0;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(0); // truncate if exists

            for (int i = 0; i < sortedItems.size(); i++) {
                Map.Entry<String, String> entry = sortedItems.get(i);
                String key   = entry.getKey();
                String value = entry.getValue();

                // Record sparse index entry every INDEX_INTERVAL entries
                if (i % INDEX_INTERVAL == 0) {
                    indexKeys.add(key);
                    index.add(new long[]{ raf.getFilePointer() });
                }

                bloom.add(key);

                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

                // [2B key_len][key][4B val_len][value]
                raf.writeShort(keyBytes.length);
                raf.write(keyBytes);
                raf.writeInt(valBytes.length);
                raf.write(valBytes);
                entryCount++;
            }

            // Write index block
            long indexOffset = raf.getFilePointer();
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < indexKeys.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("[\"").append(escapeJson(indexKeys.get(i))).append("\",")
                  .append(index.get(i)[0]).append("]");
            }
            sb.append("]");
            byte[] indexData = sb.toString().getBytes(StandardCharsets.UTF_8);
            raf.writeInt(indexData.length);
            raf.write(indexData);

            // Write bloom filter block
            long bloomOffset = raf.getFilePointer();
            byte[] bloomData = bloom.toBytes();
            raf.writeInt(bloomData.length);
            raf.write(bloomData);

            // Write footer: [8B indexOffset][4B indexLen][4B entryCount][8B bloomOffset][8B magic]
            raf.writeLong(indexOffset);
            raf.writeInt(indexData.length);
            raf.writeInt(entryCount);
            raf.writeLong(bloomOffset);
            raf.write(MAGIC);
        }

        long size = Files.size(path);
        System.out.println("[SSTable] Written '" + path + "': " + entryCount + " entries, " + size + " bytes");
        return new SSTableReader(path.toString());
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}