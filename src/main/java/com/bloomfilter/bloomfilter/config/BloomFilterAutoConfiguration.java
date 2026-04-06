package com.bloomfilter.bloomfilter.config;

import com.bloomfilter.bloomfilter.redis.RedisBloomCommandExecutor;
import com.bloomfilter.bloomfilter.shard.ShardRouter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Auto-configuration that initializes the Bloom Filter system on application startup.
 *
 * <h3>Startup Sequence</h3>
 * <ol>
 *   <li>Validates that the RedisBloom module is loaded</li>
 *   <li>For each configured filter in {@code bloom-filter.filters}:</li>
 *   <li>  Creates all shards via {@code BF.RESERVE} (idempotent — skips existing)</li>
 *   <li>Logs summary of initialized filters</li>
 * </ol>
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(BloomFilterProperties.class)
public class BloomFilterAutoConfiguration {

    /**
     * Initializes configured bloom filters on application startup.
     *
     * <p>This runs after all beans are created and the Redis connection is established.
     * Filter creation is idempotent — if a filter (shard) already exists in Redis,
     * the BF.RESERVE call is silently skipped.</p>
     */
    @Bean
    public CommandLineRunner bloomFilterInitializer(
            BloomFilterProperties properties,
            RedisBloomCommandExecutor commandExecutor,
            ShardRouter shardRouter) {

        return args -> {
            if (!properties.isAutoReserve()) {
                log.info("Auto-reserve is disabled. Skipping bloom filter initialization.");
                return;
            }

            log.info("═══════════════════════════════════════════════════════════════");
            log.info("  Initializing Redis Bloom Filters");
            log.info("═══════════════════════════════════════════════════════════════");

            // Validate RedisBloom module
            boolean moduleAvailable = commandExecutor.isRedisBloomAvailable();
            if (!moduleAvailable) {
                log.error("╔═══════════════════════════════════════════════════════════╗");
                log.error("║  WARNING: RedisBloom module is NOT loaded!               ║");
                log.error("║  Bloom filter operations will FAIL at runtime.           ║");
                log.error("║                                                          ║");
                log.error("║  Solutions:                                               ║");
                log.error("║  1. Use Redis Stack: docker run -p 6379:6379             ║");
                log.error("║     redis/redis-stack-server:latest                      ║");
                log.error("║  2. Load module: --loadmodule /path/to/redisbloom.so     ║");
                log.error("╚═══════════════════════════════════════════════════════════╝");
                return;
            }

            log.info("  ✓ RedisBloom module detected");

            // Initialize each configured filter
            Map<String, BloomFilterProperties.FilterConfig> filters = properties.getFilters();
            if (filters.isEmpty()) {
                log.info("  No filters configured for auto-initialization.");
                log.info("═══════════════════════════════════════════════════════════════");
                return;
            }

            for (Map.Entry<String, BloomFilterProperties.FilterConfig> entry : filters.entrySet()) {
                String filterName = entry.getKey();
                BloomFilterProperties.FilterConfig config = entry.getValue();

                long capacity = config.resolveCapacity(properties.getDefaultCapacity());
                double errorRate = config.resolveErrorRate(properties.getDefaultErrorRate());
                int shardCount = config.resolveShardCount(properties.getDefaultShardCount());
                long perShardCapacity = capacity / shardCount;

                log.info("  Initializing filter: '{}'", filterName);
                log.info("    Capacity:     {} total ({} per shard)", formatNumber(capacity), formatNumber(perShardCapacity));
                log.info("    Error Rate:   {}%", errorRate * 100);
                log.info("    Shards:       {}", shardCount);
                log.info("    Expansion:    {}", properties.getExpansion());
                log.info("    Non-Scaling:  {}", properties.isNonScaling());

                int created = 0;
                int existing = 0;
                for (int i = 0; i < shardCount; i++) {
                    String shardKey = String.format("%s:%s:shard:%d",
                            properties.getKeyPrefix(), filterName, i);
                    try {
                        if (commandExecutor.keyExists(shardKey)) {
                            existing++;
                        } else {
                            commandExecutor.reserve(shardKey, errorRate, perShardCapacity,
                                    properties.getExpansion(), properties.isNonScaling());
                            created++;
                        }
                    } catch (Exception e) {
                        log.warn("    ⚠ Failed to initialize shard {}: {}", shardKey, e.getMessage());
                    }
                }
                log.info("    ✓ Result: {} created, {} already existed", created, existing);
            }

            log.info("═══════════════════════════════════════════════════════════════");
            log.info("  Bloom Filter system ready. {} filters initialized.", filters.size());
            log.info("═══════════════════════════════════════════════════════════════");
        };
    }

    private String formatNumber(long number) {
        if (number >= 1_000_000_000) return String.format("%.1fB", number / 1_000_000_000.0);
        if (number >= 1_000_000) return String.format("%.1fM", number / 1_000_000.0);
        if (number >= 1_000) return String.format("%.1fK", number / 1_000.0);
        return String.valueOf(number);
    }
}
