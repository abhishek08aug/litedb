package com.litedb.sstable;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * BloomFilter — Probabilistic key membership test.
 *
 * A Bloom filter uses multiple hash functions and a bit array.
 *   add(key)          : set bits at hash positions
 *   mightContain(key) : if ANY bit is 0 → definitely not present
 *                       if ALL bits are 1 → probably present (false positive possible)
 *
 * False positive rate with m=10000 bits, k=3 hashes, n=1000 keys ≈ 1%
 * This means: 1% of "not found" lookups will still read the SSTable.
 *             99% of "not found" lookups skip the SSTable entirely.
 */
public class BloomFilter {

    private final byte[] bits;
    private final int    sizeBits;
    private final int    numHashes;

    public BloomFilter() {
        this(10000, 3);
    }

    public BloomFilter(int sizeBits, int numHashes) {
        this.sizeBits  = sizeBits;
        this.numHashes = numHashes;
        this.bits      = new byte[sizeBits / 8 + 1];
    }

    /** Internal constructor used by fromBytes(). */
    private BloomFilter(int sizeBits, int numHashes, byte[] bits) {
        this.sizeBits  = sizeBits;
        this.numHashes = numHashes;
        this.bits      = bits;
    }

    // ------------------------------------------------------------------ //
    //  Hash positions                                                     //
    // ------------------------------------------------------------------ //

    private int[] hashPositions(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int[]  positions = new int[numHashes];
        for (int seed = 0; seed < numHashes; seed++) {
            CRC32 crc = new CRC32();
            // Mix seed into the key bytes to get k independent hashes
            crc.update(keyBytes);
            crc.update(seed * 0x9e3779b9);
            long h = crc.getValue() & 0xFFFFFFFFL;
            positions[seed] = (int)(h % sizeBits);
        }
        return positions;
    }

    // ------------------------------------------------------------------ //
    //  Public API                                                         //
    // ------------------------------------------------------------------ //

    public void add(String key) {
        for (int pos : hashPositions(key)) {
            int byteIdx = pos / 8;
            int bitIdx  = pos % 8;
            bits[byteIdx] |= (byte)(1 << bitIdx);
        }
    }

    public boolean mightContain(String key) {
        for (int pos : hashPositions(key)) {
            int byteIdx = pos / 8;
            int bitIdx  = pos % 8;
            if ((bits[byteIdx] & (1 << bitIdx)) == 0) {
                return false; // definitely not present
            }
        }
        return true; // probably present
    }

    // ------------------------------------------------------------------ //
    //  Serialization                                                      //
    // ------------------------------------------------------------------ //

    /**
     * Serialize to bytes: [4B sizeBits][4B numHashes][bits...]
     */
    public byte[] toBytes() {
        byte[] header = new byte[8];
        header[0] = (byte)(sizeBits  >> 24);
        header[1] = (byte)(sizeBits  >> 16);
        header[2] = (byte)(sizeBits  >>  8);
        header[3] = (byte)(sizeBits);
        header[4] = (byte)(numHashes >> 24);
        header[5] = (byte)(numHashes >> 16);
        header[6] = (byte)(numHashes >>  8);
        header[7] = (byte)(numHashes);
        byte[] result = new byte[8 + bits.length];
        System.arraycopy(header, 0, result, 0, 8);
        System.arraycopy(bits,   0, result, 8, bits.length);
        return result;
    }

    public static BloomFilter fromBytes(byte[] data) {
        int sz  = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16)
                | ((data[2] & 0xFF) <<  8) |  (data[3] & 0xFF);
        int nh  = ((data[4] & 0xFF) << 24) | ((data[5] & 0xFF) << 16)
                | ((data[6] & 0xFF) <<  8) |  (data[7] & 0xFF);
        byte[] bitsArr = new byte[data.length - 8];
        System.arraycopy(data, 8, bitsArr, 0, bitsArr.length);
        return new BloomFilter(sz, nh, bitsArr);
    }
}