package com.bloomfilter.bloomfilter.redis;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

/**
 * Defines all RedisBloom module commands as Lettuce ProtocolKeywords.
 *
 * <p>Lettuce does not natively support RedisBloom commands. This enum bridges
 * the gap by implementing {@link ProtocolKeyword}, allowing us to dispatch
 * arbitrary commands through Lettuce's {@code dispatch()} mechanism.</p>
 *
 * <p>Each command maps 1:1 to its Redis counterpart:
 * <ul>
 *   <li>{@link #BF_RESERVE} → {@code BF.RESERVE key error_rate capacity [EXPANSION n] [NONSCALING]}</li>
 *   <li>{@link #BF_ADD} → {@code BF.ADD key item}</li>
 *   <li>{@link #BF_MADD} → {@code BF.MADD key item [item ...]}</li>
 *   <li>{@link #BF_EXISTS} → {@code BF.EXISTS key item}</li>
 *   <li>{@link #BF_MEXISTS} → {@code BF.MEXISTS key item [item ...]}</li>
 *   <li>{@link #BF_INFO} → {@code BF.INFO key}</li>
 *   <li>{@link #BF_CARD} → {@code BF.CARD key}</li>
 * </ul>
 *
 * @see <a href="https://redis.io/docs/data-types/probabilistic/bloom-filter/">RedisBloom Documentation</a>
 */
public enum BloomFilterCommand implements ProtocolKeyword {

    /**
     * Creates a new Bloom Filter with specified error rate and capacity.
     * Must be called before adding items if you need non-default parameters.
     */
    BF_RESERVE("BF.RESERVE"),

    /**
     * Adds a single item to the Bloom Filter.
     * Returns 1 if the item was newly added, 0 if it may have existed.
     */
    BF_ADD("BF.ADD"),

    /**
     * Adds multiple items to the Bloom Filter in a single round-trip.
     * Critical for bulk loading billions of items efficiently.
     */
    BF_MADD("BF.MADD"),

    /**
     * Checks if a single item may exist in the Bloom Filter.
     * Returns 1 if the item may exist, 0 if it definitely does not.
     */
    BF_EXISTS("BF.EXISTS"),

    /**
     * Checks if multiple items may exist in the Bloom Filter.
     * Returns a list of 0/1 values corresponding to each item.
     */
    BF_MEXISTS("BF.MEXISTS"),

    /**
     * Returns information about a Bloom Filter (capacity, size, number of filters, etc.).
     */
    BF_INFO("BF.INFO"),

    /**
     * Returns the cardinality (number of items added) of a Bloom Filter.
     */
    BF_CARD("BF.CARD");

    private final byte[] bytes;

    BloomFilterCommand(String command) {
        this.bytes = command.getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
