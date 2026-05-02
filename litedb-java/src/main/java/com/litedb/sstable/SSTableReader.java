package com.litedb.sstable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * SSTableReader — reads from an immutable SSTable file.
 *
 * Supports:
 *   get(key)              — point lookup using Bloom filter + sparse index
 *   scan(startKey,endKey) — range scan
 *   iterAll()             — full sequential scan (used during compaction)
 */
public class SSTableReader {

    public final String path;
    public final int    entryCount;

    private final List<String> indexKeys    = new ArrayList<>();
    private final List<Long>   indexOffsets = new ArrayList<>();
    private       BloomFilter  bloom;
    private       long         indexOffset;
    private       long         bloomOffset;

    public SSTableReader(String filePath) throws IOException {
        this.path = filePath;
        this.entryCount = loadMetadata();
    }

    // ------------------------------------------------------------------ //
    //  Metadata loading                                                   //
    // ------------------------------------------------------------------ //

    private int loadMetadata() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            long fileLen = raf.length();

            // Read footer from end of file (32 bytes)
            raf.seek(fileLen - SSTableWriter.FOOTER_SIZE);
            long idxOffset  = raf.readLong();
            int  idxLen     = raf.readInt();
            int  count      = raf.readInt();
            long blmOffset  = raf.readLong();
            byte[] magic    = new byte[8];
            raf.readFully(magic);

            if (!Arrays.equals(magic, SSTableWriter.MAGIC)) {
                throw new IOException("Not a valid SSTable file: " + path);
            }

            this.indexOffset = idxOffset;
            this.bloomOffset = blmOffset;

            // Read index block
            raf.seek(idxOffset);
            int idxDataLen = raf.readInt();
            byte[] idxData = new byte[idxDataLen];
            raf.readFully(idxData);
            parseIndex(new String(idxData, StandardCharsets.UTF_8));

            // Read bloom filter block
            raf.seek(blmOffset);
            int blmLen = raf.readInt();
            byte[] blmData = new byte[blmLen];
            raf.readFully(blmData);
            this.bloom = BloomFilter.fromBytes(blmData);

            return count;
        }
    }

    /** Parse JSON index: [[key,offset],...] */
    private void parseIndex(String json) {
        // Simple parser for our own format: [["key",offset],["key2",offset2],...]
        json = json.trim();
        if (json.equals("[]")) return;
        // Remove outer brackets
        json = json.substring(1, json.length() - 1);
        // Split on ],[
        String[] pairs = json.split("\\],\\[");
        for (String pair : pairs) {
            pair = pair.replace("[", "").replace("]", "").trim();
            // pair is: "key",offset
            int lastComma = pair.lastIndexOf(',');
            String keyPart    = pair.substring(0, lastComma).trim();
            String offsetPart = pair.substring(lastComma + 1).trim();
            // Remove quotes from key
            if (keyPart.startsWith("\"") && keyPart.endsWith("\"")) {
                keyPart = keyPart.substring(1, keyPart.length() - 1);
            }
            keyPart = keyPart.replace("\\\"", "\"").replace("\\\\", "\\");
            indexKeys.add(keyPart);
            indexOffsets.add(Long.parseLong(offsetPart));
        }
    }

    // ------------------------------------------------------------------ //
    //  Point lookup                                                       //
    // ------------------------------------------------------------------ //

    /**
     * Look up a key.
     * 1. Bloom filter check — if definitely absent, return null immediately
     * 2. Use sparse index to find approximate position
     * 3. Scan forward from that position
     */
    public String get(String key) throws IOException {
        if (!bloom.mightContain(key)) return null; // definitely not here

        long startOffset = findStartOffset(key);

        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            raf.seek(startOffset);
            while (raf.getFilePointer() < indexOffset) {
                String[] entry = readEntry(raf);
                if (entry == null) break;
                int cmp = entry[0].compareTo(key);
                if (cmp == 0) return entry[1];
                if (cmp > 0)  break; // passed it
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ //
    //  Range scan                                                         //
    // ------------------------------------------------------------------ //

    public List<Map.Entry<String, String>> scan(String startKey, String endKey) throws IOException {
        List<Map.Entry<String, String>> results = new ArrayList<>();
        long startOffset = findStartOffset(startKey);

        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            raf.seek(startOffset);
            while (raf.getFilePointer() < indexOffset) {
                String[] entry = readEntry(raf);
                if (entry == null) break;
                if (entry[0].compareTo(endKey) > 0) break;
                if (entry[0].compareTo(startKey) >= 0) {
                    results.add(new AbstractMap.SimpleImmutableEntry<>(entry[0], entry[1]));
                }
            }
        }
        return results;
    }

    // ------------------------------------------------------------------ //
    //  Full iteration (used during compaction)                            //
    // ------------------------------------------------------------------ //

    public List<Map.Entry<String, String>> iterAll() throws IOException {
        List<Map.Entry<String, String>> results = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            raf.seek(0);
            while (raf.getFilePointer() < indexOffset) {
                String[] entry = readEntry(raf);
                if (entry == null) break;
                results.add(new AbstractMap.SimpleImmutableEntry<>(entry[0], entry[1]));
            }
        }
        return results;
    }

    // ------------------------------------------------------------------ //
    //  Helpers                                                            //
    // ------------------------------------------------------------------ //

    private long findStartOffset(String key) {
        long offset = 0;
        for (int i = 0; i < indexKeys.size(); i++) {
            if (indexKeys.get(i).compareTo(key) <= 0) {
                offset = indexOffsets.get(i);
            } else {
                break;
            }
        }
        return offset;
    }

    /** Read one entry from the current RAF position. Returns [key, value] or null on EOF. */
    private String[] readEntry(RandomAccessFile raf) throws IOException {
        try {
            int keyLen = raf.readShort() & 0xFFFF;
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            int valLen = raf.readInt();
            byte[] valBytes = new byte[valLen];
            raf.readFully(valBytes);
            return new String[]{ new String(keyBytes, StandardCharsets.UTF_8),
                                 new String(valBytes, StandardCharsets.UTF_8) };
        } catch (EOFException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "SSTableReader(path=" + path + ", entries=" + entryCount + ")";
    }
}