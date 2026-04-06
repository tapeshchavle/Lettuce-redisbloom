package com.bloomfilter.bloomfilter.shard;

import com.bloomfilter.bloomfilter.config.BloomFilterProperties;
import com.bloomfilter.bloomfilter.hash.MurmurHash3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link ShardRouter} — validates shard distribution uniformity,
 * key formatting, and determinism.
 */
@DisplayName("ShardRouter")
class ShardRouterTest {

    private ShardRouter shardRouter;
    private BloomFilterProperties properties;

    @BeforeEach
    void setUp() {
        properties = new BloomFilterProperties();
        properties.setKeyPrefix("bf");
        properties.setDefaultShardCount(16);
        properties.setDefaultCapacity(1_000_000_000);
        properties.setDefaultErrorRate(0.001);
        shardRouter = new ShardRouter(properties);
    }

    @Test
    @DisplayName("should generate correct shard key format")
    void shardKeyFormat() {
        String key = shardRouter.resolveShardKey("users", "test-item");
        assertThat(key).matches("bf:users:shard:\\d+");
    }

    @Test
    @DisplayName("same item should always map to same shard (deterministic)")
    void deterministic() {
        String key1 = shardRouter.resolveShardKey("users", "consistent-item");
        String key2 = shardRouter.resolveShardKey("users", "consistent-item");
        String key3 = shardRouter.resolveShardKey("users", "consistent-item");

        assertThat(key1).isEqualTo(key2).isEqualTo(key3);
    }

    @Test
    @DisplayName("different items should distribute across shards")
    void distribution() {
        Set<String> shardKeys = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            shardKeys.add(shardRouter.resolveShardKey("users", "item-" + i));
        }
        // With 16 shards and 1000 items, all shards should be hit
        assertThat(shardKeys).hasSizeGreaterThan(14); // Allow some variance
    }

    @Test
    @DisplayName("shard distribution should be roughly uniform")
    void uniformDistribution() {
        int shardCount = 16;
        int totalItems = 100_000;
        int[] distribution = new int[shardCount];

        for (int i = 0; i < totalItems; i++) {
            int shard = MurmurHash3.shardIndex("user-" + i, shardCount);
            distribution[shard]++;
        }

        double expected = (double) totalItems / shardCount;
        double maxDeviation = expected * 0.15; // Allow 15% deviation

        for (int count : distribution) {
            assertThat((double) count)
                    .as("Shard count should be within 15%% of expected (%f)", expected)
                    .isBetween(expected - maxDeviation, expected + maxDeviation);
        }
    }

    @Test
    @DisplayName("getAllShardKeys should return all shard keys in order")
    void getAllShardKeys() {
        List<String> keys = shardRouter.getAllShardKeys("users");

        assertThat(keys).hasSize(16);
        assertThat(keys.get(0)).isEqualTo("bf:users:shard:0");
        assertThat(keys.get(15)).isEqualTo("bf:users:shard:15");
    }

    @Test
    @DisplayName("groupByShard should correctly partition items")
    void groupByShard() {
        List<String> items = List.of("alpha", "beta", "gamma", "delta", "epsilon");

        Map<String, List<String>> groups = shardRouter.groupByShard("users", items);

        // All items should be accounted for
        int totalGrouped = groups.values().stream().mapToInt(List::size).sum();
        assertThat(totalGrouped).isEqualTo(items.size());

        // Each group key should have correct format
        groups.keySet().forEach(key ->
                assertThat(key).matches("bf:users:shard:\\d+")
        );
    }

    @Test
    @DisplayName("per-shard capacity should be total / shardCount")
    void perShardCapacity() {
        long perShard = shardRouter.getPerShardCapacity("users");
        assertThat(perShard).isEqualTo(1_000_000_000L / 16);
    }

    @Test
    @DisplayName("should use filter-specific config when available")
    void filterSpecificConfig() {
        BloomFilterProperties.FilterConfig config = new BloomFilterProperties.FilterConfig();
        config.setShardCount(32);
        config.setCapacity(5_000_000_000L);
        properties.getFilters().put("posts", config);

        List<String> keys = shardRouter.getAllShardKeys("posts");
        assertThat(keys).hasSize(32);

        long perShard = shardRouter.getPerShardCapacity("posts");
        assertThat(perShard).isEqualTo(5_000_000_000L / 32);
    }

    @Test
    @DisplayName("MurmurHash3 should handle edge cases")
    void hashEdgeCases() {
        // Empty string
        assertThat(MurmurHash3.shardIndex("", 16)).isBetween(0, 15);

        // Very long string
        String longStr = "a".repeat(100_000);
        assertThat(MurmurHash3.shardIndex(longStr, 16)).isBetween(0, 15);

        // Unicode
        assertThat(MurmurHash3.shardIndex("日本語テスト", 16)).isBetween(0, 15);

        // Special characters
        assertThat(MurmurHash3.shardIndex("!@#$%^&*()", 16)).isBetween(0, 15);
    }
}
