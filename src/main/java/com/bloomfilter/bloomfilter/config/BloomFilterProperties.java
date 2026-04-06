package com.bloomfilter.bloomfilter.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration properties for the distributed Bloom Filter system.
 *
 * <p>Supports named filter presets for different use cases (e.g., user dedup, post dedup).
 * Each filter can have independent capacity, error rate, and shard count.</p>
 *
 * <p>Example YAML:
 * <pre>
 * bloom-filter:
 *   key-prefix: "bf"
 *   default-capacity: 1000000000
 *   default-error-rate: 0.001
 *   default-shard-count: 16
 *   filters:
 *     users:
 *       capacity: 1000000000
 *       error-rate: 0.001
 *       shard-count: 16
 * </pre>
 */
@Data
@Validated
@ConfigurationProperties(prefix = "bloom-filter")
public class BloomFilterProperties {

    /**
     * Redis key prefix for all bloom filter keys.
     * Final key format: {keyPrefix}:{filterName}:shard:{N}
     */
    @NotBlank
    private String keyPrefix = "bf";

    /**
     * Whether to auto-create configured filters on application startup via BF.RESERVE.
     */
    private boolean autoReserve = true;

    /**
     * Default expected number of items to be inserted.
     * Used when a named filter does not specify its own capacity.
     */
    @Min(1)
    private long defaultCapacity = 1_000_000_000L;

    /**
     * Default desired false positive rate (0.0 to 1.0).
     * Lower values use more memory. 0.001 = 0.1% false positive rate.
     */
    @Min(0)
    @Max(1)
    private double defaultErrorRate = 0.001;

    /**
     * Default number of shards to distribute items across.
     * More shards = better distribution but more Redis keys.
     * Recommended: 16 for ~1B items, 32 for ~5B items.
     */
    @Min(1)
    private int defaultShardCount = 16;

    /**
     * RedisBloom expansion factor when sub-filters are created.
     * Only relevant when nonScaling is false.
     */
    @Min(1)
    private int expansion = 2;

    /**
     * If true, the bloom filter will NOT create sub-filters when capacity is exceeded.
     * Use this when you know your exact upper bound.
     */
    private boolean nonScaling = false;

    /**
     * Number of items per BF.MADD batch during bulk operations.
     * Tuned for network efficiency vs. Redis CPU usage.
     */
    @Min(100)
    private int batchSize = 10_000;

    /**
     * Named filter configurations. Each entry defines a filter with its own
     * capacity, error rate, and shard count. Filters listed here will be
     * auto-created on startup if autoReserve is true.
     */
    private Map<String, FilterConfig> filters = new HashMap<>();

    /**
     * Configuration for a single named Bloom Filter.
     */
    @Data
    public static class FilterConfig {

        /** Expected number of items for this filter. Falls back to defaultCapacity. */
        private Long capacity;

        /** Desired false positive rate for this filter. Falls back to defaultErrorRate. */
        private Double errorRate;

        /** Number of shards for this filter. Falls back to defaultShardCount. */
        private Integer shardCount;

        /**
         * Resolve capacity, falling back to the global default.
         */
        public long resolveCapacity(long fallback) {
            return capacity != null ? capacity : fallback;
        }

        /**
         * Resolve error rate, falling back to the global default.
         */
        public double resolveErrorRate(double fallback) {
            return errorRate != null ? errorRate : fallback;
        }

        /**
         * Resolve shard count, falling back to the global default.
         */
        public int resolveShardCount(int fallback) {
            return shardCount != null ? shardCount : fallback;
        }
    }

    /**
     * Get a filter config by name, creating defaults if not explicitly configured.
     */
    public FilterConfig getFilterConfig(String filterName) {
        return filters.getOrDefault(filterName, new FilterConfig());
    }
}
