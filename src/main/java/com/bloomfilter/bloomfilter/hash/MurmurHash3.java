package com.bloomfilter.bloomfilter.hash;

/**
 * Pure Java implementation of MurmurHash3 (32-bit variant).
 *
 * <p>Used for deterministic shard routing — distributing items uniformly across
 * N bloom filter shards. This is NOT the hash used inside the bloom filter itself
 * (that's handled by RedisBloom). This only determines WHICH shard an item maps to.</p>
 *
 * <h3>Why MurmurHash3?</h3>
 * <ul>
 *   <li>Excellent distribution uniformity (critical for shard balance)</li>
 *   <li>Very fast (~3 GB/s on modern CPUs)</li>
 *   <li>Deterministic — same input always maps to same shard</li>
 *   <li>Well-tested, widely used (Guava, Apache Commons, Redis itself)</li>
 * </ul>
 *
 * <p>Port of the public domain C reference implementation by Austin Appleby.</p>
 */
public final class MurmurHash3 {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final int DEFAULT_SEED = 0x9747b28c;

    private MurmurHash3() {
        // Utility class
    }

    /**
     * Computes a 32-bit MurmurHash3 hash of the given string using the default seed.
     *
     * @param data the input string
     * @return 32-bit hash value
     */
    public static int hash32(String data) {
        return hash32(data.getBytes(java.nio.charset.StandardCharsets.UTF_8), DEFAULT_SEED);
    }

    /**
     * Computes a 32-bit MurmurHash3 hash of the given string with a custom seed.
     *
     * @param data the input string
     * @param seed hash seed for different hash families
     * @return 32-bit hash value
     */
    public static int hash32(String data, int seed) {
        return hash32(data.getBytes(java.nio.charset.StandardCharsets.UTF_8), seed);
    }

    /**
     * Computes a 32-bit MurmurHash3 hash of the given byte array.
     *
     * @param data byte array to hash
     * @param seed hash seed
     * @return 32-bit hash value
     */
    public static int hash32(byte[] data, int seed) {
        int h1 = seed;
        int length = data.length;
        int nblocks = length / 4;

        // Body — process 4-byte blocks
        for (int i = 0; i < nblocks; i++) {
            int offset = i * 4;
            int k1 = getBlock32(data, offset);

            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // Tail — process remaining bytes
        int offset = nblocks * 4;
        int k1 = 0;
        switch (length & 3) {
            case 3:
                k1 ^= (data[offset + 2] & 0xFF) << 16;
                // fall through
            case 2:
                k1 ^= (data[offset + 1] & 0xFF) << 8;
                // fall through
            case 1:
                k1 ^= (data[offset] & 0xFF);
                k1 *= C1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= C2;
                h1 ^= k1;
        }

        // Finalization — avalanche
        h1 ^= length;
        h1 = fmix32(h1);

        return h1;
    }

    /**
     * Maps the hash to a positive integer in [0, shardCount).
     * Uses absolute value with special handling for Integer.MIN_VALUE.
     *
     * @param data       the input string
     * @param shardCount number of shards
     * @return shard index in [0, shardCount)
     */
    public static int shardIndex(String data, int shardCount) {
        int hash = hash32(data);
        // Math.abs(Integer.MIN_VALUE) is negative, so use bitwise AND
        return (hash & 0x7FFFFFFF) % shardCount;
    }

    private static int getBlock32(byte[] data, int offset) {
        return (data[offset] & 0xFF)
                | ((data[offset + 1] & 0xFF) << 8)
                | ((data[offset + 2] & 0xFF) << 16)
                | ((data[offset + 3] & 0xFF) << 24);
    }

    private static int fmix32(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }
}
