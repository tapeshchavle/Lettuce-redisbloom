package com.bloomfilter.bloomfilter.service;


import com.bloomfilter.bloomfilter.dto.BloomFilterResponse;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * High-level service interface for Bloom Filter operations.
 *
 * <p>This interface abstracts away the underlying sharding, batching, and Redis
 * communication details. Clients interact with logical filter names and items
 * without needing to know about shard keys or batch sizes.</p>
 *
 * <h3>Consistency Guarantees</h3>
 * <ul>
 *   <li><strong>Zero false negatives</strong>: If {@code mightContain} returns false,
 *       the item was NEVER added.</li>
 *   <li><strong>Configurable false positives</strong>: {@code mightContain} may return
 *       true for items not added, at the configured error rate.</li>
 * </ul>
 *
 * <h3>Thread Safety</h3>
 * <p>All implementations must be thread-safe. Multiple threads can invoke
 * any method concurrently without external synchronization.</p>
 */
public interface BloomFilterService {

    // ─── Single Item Operations ────────────────────────────────────

    /**
     * Adds a single item to the named bloom filter.
     *
     * @param filterName logical filter name (e.g., "users")
     * @param item       the item to add
     * @return true if the item was newly added, false if it may have already existed
     */
    boolean add(String filterName, String item);

    /**
     * Checks if an item might exist in the named bloom filter.
     *
     * @param filterName logical filter name
     * @param item       the item to check
     * @return true if the item MIGHT exist (possible false positive),
     *         false if it DEFINITELY does not exist
     */
    boolean mightContain(String filterName, String item);

    // ─── Batch Operations ──────────────────────────────────────────

    /**
     * Adds multiple items to the named bloom filter.
     * Items are automatically grouped by shard and sent via BF.MADD.
     *
     * @param filterName logical filter name
     * @param items      items to add
     * @return list of booleans — true if newly added, false if may have existed
     */
    List<Boolean> multiAdd(String filterName, List<String> items);

    /**
     * Checks if multiple items might exist in the named bloom filter.
     * Items are automatically grouped by shard and sent via BF.MEXISTS.
     *
     * @param filterName logical filter name
     * @param items      items to check
     * @return list of booleans — true if might exist, false if definitely absent
     */
    List<Boolean> multiMightContain(String filterName, List<String> items);

    // ─── Async Operations (for high-throughput paths) ──────────────

    /**
     * Async variant of {@link #add(String, String)}.
     */
    CompletableFuture<Boolean> addAsync(String filterName, String item);

    /**
     * Async variant of {@link #mightContain(String, String)}.
     */
    CompletableFuture<Boolean> mightContainAsync(String filterName, String item);

    // ─── Bulk Operations (for data loading) ────────────────────────

    /**
     * Bulk-adds a large collection of items, processing in configurable batches.
     * Designed for initial data loading (millions/billions of items).
     *
     * @param filterName logical filter name
     * @param items      collection of items to add
     * @param batchSize  number of items per BF.MADD call
     * @return total number of items processed
     */
    long bulkAdd(String filterName, Collection<String> items, int batchSize);

    // ─── Management Operations ─────────────────────────────────────

    /**
     * Creates a new bloom filter with the specified parameters.
     * This calls BF.RESERVE on all shards.
     *
     * @param filterName logical filter name
     * @param capacity   total expected number of items
     * @param errorRate  desired false positive rate (e.g., 0.001 for 0.1%)
     * @param shardCount number of shards to create
     */
    void createFilter(String filterName, long capacity, double errorRate, int shardCount);

    /**
     * Gets aggregated information about a bloom filter across all shards.
     *
     * @param filterName logical filter name
     * @return aggregated info
     */
    BloomFilterResponse.AggregatedInfo getFilterInfo(String filterName);

    /**
     * Deletes a bloom filter and all its shards from Redis.
     *
     * @param filterName logical filter name
     */
    void deleteFilter(String filterName);
}
