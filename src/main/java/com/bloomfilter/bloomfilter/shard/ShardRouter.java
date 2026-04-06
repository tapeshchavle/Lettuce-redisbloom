package com.bloomfilter.bloomfilter.shard;

import com.bloomfilter.bloomfilter.config.BloomFilterProperties;
import com.bloomfilter.bloomfilter.hash.MurmurHash3;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Routes items to their appropriate bloom filter shard using MurmurHash3.
 *
 * <h3>Sharding Strategy</h3>
 * <p>For billions of items, a single Redis key becomes a bottleneck due to:
 * <ul>
 *   <li>Memory: Single key holding the entire bit array</li>
 *   <li>CPU: All hash computations on one Redis thread</li>
 *   <li>Network: All clients contending for the same key</li>
 * </ul>
 *
 * <p>By distributing items across N shards, we get:
 * <ul>
 *   <li>Each shard holds 1/N of the capacity → smaller bit arrays</li>
 *   <li>In Redis Cluster, shards can be distributed across nodes</li>
 *   <li>Parallel BF.MADD/BF.MEXISTS across shards</li>
 * </ul>
 *
 * <h3>Key Format</h3>
 * <pre>
 * {keyPrefix}:{filterName}:shard:{shardIndex}
 * Example: bf:users:shard:7
 * </pre>
 *
 * <p>The filter name is NOT wrapped in Redis hash tags by default.
 * If using Redis Cluster with co-location requirements, configure hash tags
 * externally.</p>
 */
@Slf4j
@Component
public class ShardRouter {

    private final BloomFilterProperties properties;

    public ShardRouter(BloomFilterProperties properties) {
        this.properties = properties;
    }

    /**
     * Resolves the Redis key for a single item.
     *
     * @param filterName logical filter name (e.g., "users")
     * @param item       the item to route
     * @return the Redis key for the shard this item belongs to
     */
    public String resolveShardKey(String filterName, String item) {
        int shardCount = getShardCount(filterName);
        int shardIndex = MurmurHash3.shardIndex(item, shardCount);
        return buildShardKey(filterName, shardIndex);
    }

    /**
     * Returns all shard keys for a given filter.
     * Used for operations that span all shards (e.g., BF.RESERVE on startup, BF.INFO).
     *
     * @param filterName logical filter name
     * @return list of all shard keys
     */
    public List<String> getAllShardKeys(String filterName) {
        int shardCount = getShardCount(filterName);
        return IntStream.range(0, shardCount)
                .mapToObj(i -> buildShardKey(filterName, i))
                .toList();
    }

    /**
     * Groups items by their target shard key for efficient batch operations.
     *
     * <p>Instead of sending N individual BF.ADD commands, we group items by shard
     * and send one BF.MADD per shard — reducing N commands to at most shardCount commands.</p>
     *
     * @param filterName logical filter name
     * @param items      items to group
     * @return map of shard key → list of items belonging to that shard
     */
    public Map<String, List<String>> groupByShard(String filterName, List<String> items) {
        int shardCount = getShardCount(filterName);
        Map<String, List<String>> shardGroups = new HashMap<>();

        for (String item : items) {
            int shardIndex = MurmurHash3.shardIndex(item, shardCount);
            String shardKey = buildShardKey(filterName, shardIndex);
            shardGroups.computeIfAbsent(shardKey, k -> new ArrayList<>()).add(item);
        }

        if (log.isDebugEnabled()) {
            log.debug("Grouped {} items across {} shards for filter '{}'",
                    items.size(), shardGroups.size(), filterName);
        }

        return shardGroups;
    }

    /**
     * Returns the shard count for a filter, using filter-specific config or global default.
     */
    public int getShardCount(String filterName) {
        BloomFilterProperties.FilterConfig config = properties.getFilterConfig(filterName);
        return config.resolveShardCount(properties.getDefaultShardCount());
    }

    /**
     * Returns the per-shard capacity for a filter.
     * Total capacity is split evenly across shards.
     */
    public long getPerShardCapacity(String filterName) {
        BloomFilterProperties.FilterConfig config = properties.getFilterConfig(filterName);
        long totalCapacity = config.resolveCapacity(properties.getDefaultCapacity());
        int shardCount = config.resolveShardCount(properties.getDefaultShardCount());
        return totalCapacity / shardCount;
    }

    /**
     * Returns the error rate for a filter, using filter-specific config or global default.
     */
    public double getErrorRate(String filterName) {
        BloomFilterProperties.FilterConfig config = properties.getFilterConfig(filterName);
        return config.resolveErrorRate(properties.getDefaultErrorRate());
    }

    private String buildShardKey(String filterName, int shardIndex) {
        return String.format("%s:%s:shard:%d", properties.getKeyPrefix(), filterName, shardIndex);
    }
}
