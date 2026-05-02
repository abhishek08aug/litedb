package com.litedb.wal;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * WriteAheadLog — Append-only Write-Ahead Log (WAL)
 *
 * CONCEPT:
 *   Before writing data anywhere, write it to the WAL first.
 *   The WAL is an append-only file on disk.
 *   If the process crashes mid-write, on restart we replay the WAL
 *   to recover all committed writes — nothing is lost.
 *
 *   This is how PostgreSQL, MySQL, RocksDB, and Cassandra all guarantee
 *   durability (the 'D' in ACID).
 *
 * WAL Entry Format (binary):
 *   [4 bytes: payload_length] [N bytes: UTF-8 JSON] [4 bytes: CRC32 checksum]
 *
 *   CRC32 detects corruption — if the last entry is truncated (crash during write),
 *   the checksum won't match and we skip that entry.
 */
public class WriteAheadLog implements Closeable {

    private static final int HEADER_SIZE   = 4;  // payload length prefix (big-endian int)
    private static final int CHECKSUM_SIZE = 4;  // CRC32 suffix

    private final Path          path;
    private final ReentrantLock lock = new ReentrantLock();
    private       int           nextSequence = 0;
    private       FileOutputStream fos;
    private       FileChannel      channel;

    /**
     * Open (or create) a WAL at the given path.
     * Scans existing entries to recover the next sequence number.
     */
    public WriteAheadLog(String filePath) throws IOException {
        this.path = Paths.get(filePath);
        Files.createDirectories(this.path.getParent() == null
                ? Paths.get(".") : this.path.getParent());

        // Recover sequence from existing WAL
        for (WALEntry e : readAll()) {
            if (e.sequence >= nextSequence) {
                nextSequence = e.sequence + 1;
            }
        }

        // Open in append mode
        this.fos     = new FileOutputStream(path.toFile(), true);
        this.channel = fos.getChannel();

        System.out.println("[WAL] Opened '" + path + "', next sequence=" + nextSequence);
    }

    // ------------------------------------------------------------------ //
    //  Write path                                                         //
    // ------------------------------------------------------------------ //

    /**
     * Append a SET entry to the WAL.
     * fsync() ensures the entry is on disk before returning.
     */
    public WALEntry appendSet(String key, String value) throws IOException {
        return append("SET", key, value);
    }

    /**
     * Append a DELETE entry to the WAL.
     */
    public WALEntry appendDelete(String key) throws IOException {
        return append("DELETE", key, null);
    }

    private WALEntry append(String operation, String key, String value) throws IOException {
        lock.lock();
        try {
            WALEntry entry = new WALEntry(nextSequence++, operation, key, value);
            writeEntry(entry);
            return entry;
        } finally {
            lock.unlock();
        }
    }

    private void writeEntry(WALEntry entry) throws IOException {
        byte[] payload  = entry.toJson().getBytes("UTF-8");
        CRC32  crc32    = new CRC32();
        crc32.update(payload);
        long   checksum = crc32.getValue();

        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + payload.length + CHECKSUM_SIZE);
        buf.putInt(payload.length);          // 4-byte big-endian length
        buf.put(payload);                    // JSON payload
        buf.putInt((int)(checksum & 0xFFFFFFFFL)); // 4-byte CRC32

        buf.flip();
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        channel.force(false);  // fsync — durability guarantee
    }

    // ------------------------------------------------------------------ //
    //  Read / Recovery path                                               //
    // ------------------------------------------------------------------ //

    /**
     * Read all valid entries from the WAL file.
     * Skips corrupted/truncated entries at the end (crash during write).
     * Used during crash recovery to rebuild the MemTable.
     */
    public List<WALEntry> readAll() throws IOException {
        List<WALEntry> entries = new ArrayList<>();
        if (!Files.exists(path)) return entries;

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(path.toFile())))) {

            while (true) {
                // Read length prefix
                int payloadLength;
                try {
                    payloadLength = dis.readInt();
                } catch (EOFException e) {
                    break; // clean EOF
                }

                // Read payload
                byte[] payload = new byte[payloadLength];
                int read = dis.read(payload, 0, payloadLength);
                if (read < payloadLength) {
                    System.out.println("[WAL] Truncated entry — skipping (crash during write)");
                    break;
                }

                // Read checksum
                int storedCrc;
                try {
                    storedCrc = dis.readInt();
                } catch (EOFException e) {
                    System.out.println("[WAL] Missing checksum — skipping");
                    break;
                }

                // Verify checksum
                CRC32 crc32 = new CRC32();
                crc32.update(payload);
                int actualCrc = (int)(crc32.getValue() & 0xFFFFFFFFL);
                if (storedCrc != actualCrc) {
                    System.out.println("[WAL] CRC mismatch — entry corrupted, skipping");
                    break;
                }

                try {
                    String json = new String(payload, "UTF-8");
                    entries.add(WALEntry.fromJson(json));
                } catch (Exception e) {
                    System.out.println("[WAL] Malformed entry: " + e.getMessage() + " — skipping");
                    break;
                }
            }
        }
        return entries;
    }

    /**
     * Truncate the WAL after a successful MemTable flush to SSTable.
     * Once data is safely on disk as an SSTable, the WAL is no longer needed.
     */
    public void truncate() throws IOException {
        lock.lock();
        try {
            channel.close();
            fos.close();
            Files.deleteIfExists(path);
            fos     = new FileOutputStream(path.toFile(), true);
            channel = fos.getChannel();
            System.out.println("[WAL] Truncated (data safely flushed to SSTable)");
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            channel.force(true);
            channel.close();
            fos.close();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "WriteAheadLog(path=" + path + ", nextSeq=" + nextSequence + ")";
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) throws Exception {
        java.nio.file.Path tmpDir = Files.createTempDirectory("litedb_wal_demo_");
        String walPath = tmpDir.resolve("wal.log").toString();

        System.out.println("============================================================");
        System.out.println("WAL DEMO");
        System.out.println("============================================================");

        // Step 1: Write entries
        System.out.println("\n[Step 1] Writing entries to WAL...");
        WriteAheadLog wal = new WriteAheadLog(walPath);
        WALEntry e1 = wal.appendSet("name", "Alice");
        WALEntry e2 = wal.appendSet("age", "30");
        WALEntry e3 = wal.appendSet("city", "New York");
        WALEntry e4 = wal.appendDelete("age");
        System.out.println("  Written: " + e1);
        System.out.println("  Written: " + e2);
        System.out.println("  Written: " + e3);
        System.out.println("  Written: " + e4);
        wal.close();

        // Step 2: Simulate crash & recovery
        System.out.println("\n[Step 2] Simulating crash... reopening WAL for recovery");
        WriteAheadLog wal2 = new WriteAheadLog(walPath);

        System.out.println("\n[Step 3] Replaying WAL entries (crash recovery):");
        for (WALEntry entry : wal2.readAll()) {
            System.out.println("  Replaying: " + entry);
        }

        // Step 3: Show file size
        long size = Files.size(Paths.get(walPath));
        System.out.println("\n[Step 4] WAL file size on disk: " + size + " bytes");
        System.out.println("         Location: " + walPath);

        wal2.close();

        // Cleanup
        deleteDir(tmpDir.toFile());
        System.out.println("\n[Done] WAL demo complete.");
        System.out.println("\nKey insight: Even if the process crashed after Step 1,");
        System.out.println("we can replay the WAL to recover all 4 operations.");
    }

    private static void deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File f : dir.listFiles()) deleteDir(f);
        }
        dir.delete();
    }
}