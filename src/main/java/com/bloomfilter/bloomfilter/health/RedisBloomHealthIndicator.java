package com.bloomfilter.bloomfilter.health;

import com.bloomfilter.bloomfilter.config.BloomFilterProperties;
import com.bloomfilter.bloomfilter.redis.RedisBloomCommandExecutor;
import com.bloomfilter.bloomfilter.shard.ShardRouter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom health indicator that reports the status of the Bloom Filter system.
 *
 * <p>Checks performed:
 * <ul>
 *   <li>Redis connectivity</li>
 *   <li>RedisBloom module availability</li>
 *   <li>Configured filters exist in Redis</li>
 * </ul>
 *
 * <p>Accessible via {@code GET /actuator/health}</p>
 */
@Slf4j
@Component("bloomFilter")
public class RedisBloomHealthIndicator implements HealthIndicator {

    private final RedisBloomCommandExecutor commandExecutor;
    private final BloomFilterProperties properties;
    private final ShardRouter shardRouter;

    public RedisBloomHealthIndicator(RedisBloomCommandExecutor commandExecutor,
                                     BloomFilterProperties properties,
                                     ShardRouter shardRouter) {
        this.commandExecutor = commandExecutor;
        this.properties = properties;
        this.shardRouter = shardRouter;
    }

    @Override
    public Health health() {
        try {
            // Check 1: RedisBloom module availability
            boolean bloomAvailable = commandExecutor.isRedisBloomAvailable();
            if (!bloomAvailable) {
                return Health.down()
                        .withDetail("redisBloom", "Module not loaded")
                        .withDetail("suggestion", "Use Redis Stack or load the RedisBloom module")
                        .build();
            }

            // Check 2: Configured filters health
            Map<String, Object> filterStatus = new HashMap<>();
            Map<String, BloomFilterProperties.FilterConfig> filters = properties.getFilters();

            for (Map.Entry<String, BloomFilterProperties.FilterConfig> entry : filters.entrySet()) {
                String filterName = entry.getKey();
                List<String> shardKeys = shardRouter.getAllShardKeys(filterName);
                long existingShards = shardKeys.stream()
                        .filter(commandExecutor::keyExists)
                        .count();
                filterStatus.put(filterName, Map.of(
                        "totalShards", shardKeys.size(),
                        "activeShards", existingShards,
                        "healthy", existingShards == shardKeys.size()
                ));
            }

            return Health.up()
                    .withDetail("redisBloom", "Available")
                    .withDetail("configuredFilters", filters.size())
                    .withDetail("filters", filterStatus)
                    .build();

        } catch (Exception e) {
            log.error("Health check failed", e);
            return Health.down()
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
}
