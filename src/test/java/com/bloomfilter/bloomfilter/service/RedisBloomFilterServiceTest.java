package com.bloomfilter.bloomfilter.service;

import com.bloomfilter.bloomfilter.config.BloomFilterProperties;
import com.bloomfilter.bloomfilter.dto.BloomFilterInfo;
import com.bloomfilter.bloomfilter.dto.BloomFilterResponse;
import com.bloomfilter.bloomfilter.metrics.BloomFilterMetrics;
import com.bloomfilter.bloomfilter.redis.RedisBloomCommandExecutor;
import com.bloomfilter.bloomfilter.shard.ShardRouter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RedisBloomFilterService}.
 *
 * <p>Tests shard routing, batch processing, async operations, and error handling
 * with mocked dependencies.</p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RedisBloomFilterService")
class RedisBloomFilterServiceTest {

    @Mock private RedisBloomCommandExecutor commandExecutor;
    @Mock private ShardRouter shardRouter;
    @Mock private BloomFilterProperties properties;
    @Mock private BloomFilterMetrics metrics;

    private RedisBloomFilterService service;

    @BeforeEach
    void setUp() {
        service = new RedisBloomFilterService(commandExecutor, shardRouter, properties, metrics);
    }

    @Nested
    @DisplayName("Single Add")
    class SingleAddTests {

        @Test
        @DisplayName("should route item to correct shard and add")
        void addRoutesToShard() {
            when(shardRouter.resolveShardKey("users", "user123"))
                    .thenReturn("bf:users:shard:5");
            when(commandExecutor.add("bf:users:shard:5", "user123"))
                    .thenReturn(true);

            boolean result = service.add("users", "user123");

            assertThat(result).isTrue();
            verify(shardRouter).resolveShardKey("users", "user123");
            verify(commandExecutor).add("bf:users:shard:5", "user123");
            verify(metrics).recordAdd(eq("users"), anyLong());
        }

        @Test
        @DisplayName("should record error metric on failure")
        void addRecordsErrorOnFailure() {
            when(shardRouter.resolveShardKey("users", "user123"))
                    .thenReturn("bf:users:shard:5");
            when(commandExecutor.add("bf:users:shard:5", "user123"))
                    .thenThrow(new RuntimeException("Redis down"));

            assertThatThrownBy(() -> service.add("users", "user123"));

            verify(metrics).recordError("users", "add");
        }
    }

    @Nested
    @DisplayName("Single Exists")
    class SingleExistsTests {

        @Test
        @DisplayName("should route item to correct shard and check")
        void existsRoutesToShard() {
            when(shardRouter.resolveShardKey("users", "user123"))
                    .thenReturn("bf:users:shard:3");
            when(commandExecutor.exists("bf:users:shard:3", "user123"))
                    .thenReturn(true);

            boolean result = service.mightContain("users", "user123");

            assertThat(result).isTrue();
            verify(metrics).recordExists(eq("users"), eq(true), anyLong());
        }
    }

    @Nested
    @DisplayName("Batch Add (multiAdd)")
    class BatchAddTests {

        @Test
        @DisplayName("should group items by shard and batch add")
        void multiAddGroupsByShard() {
            List<String> items = List.of("a", "b", "c", "d");
            Map<String, List<String>> shardGroups = Map.of(
                    "bf:users:shard:0", List.of("a", "c"),
                    "bf:users:shard:1", List.of("b", "d")
            );

            when(shardRouter.groupByShard("users", items)).thenReturn(shardGroups);
            when(properties.getBatchSize()).thenReturn(10000);
            when(commandExecutor.multiAdd("bf:users:shard:0", "a", "c"))
                    .thenReturn(List.of(true, true));
            when(commandExecutor.multiAdd("bf:users:shard:1", "b", "d"))
                    .thenReturn(List.of(false, true));

            List<Boolean> results = service.multiAdd("users", items);

            // Results should be in original item order: a=true, b=false, c=true, d=true
            assertThat(results).hasSize(4);
            verify(metrics).recordBatchSize("users", "add", 4);
        }

        @Test
        @DisplayName("should return empty list for null input")
        void multiAddNullInput() {
            List<Boolean> results = service.multiAdd("users", null);
            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("should return empty list for empty input")
        void multiAddEmptyInput() {
            List<Boolean> results = service.multiAdd("users", List.of());
            assertThat(results).isEmpty();
        }
    }

    @Nested
    @DisplayName("Batch Exists (multiMightContain)")
    class BatchExistsTests {

        @Test
        @DisplayName("should group items by shard and batch check")
        void multiExistsGroupsByShard() {
            List<String> items = List.of("x", "y");
            Map<String, List<String>> shardGroups = Map.of(
                    "bf:users:shard:2", List.of("x", "y")
            );

            when(shardRouter.groupByShard("users", items)).thenReturn(shardGroups);
            when(properties.getBatchSize()).thenReturn(10000);
            when(commandExecutor.multiExists("bf:users:shard:2", "x", "y"))
                    .thenReturn(List.of(true, false));

            List<Boolean> results = service.multiMightContain("users", items);

            assertThat(results).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Async Operations")
    class AsyncTests {

        @Test
        @DisplayName("should add asynchronously with correct shard routing")
        void addAsyncRoutes() {
            when(shardRouter.resolveShardKey("users", "item1"))
                    .thenReturn("bf:users:shard:7");
            when(commandExecutor.addAsync("bf:users:shard:7", "item1"))
                    .thenReturn(CompletableFuture.completedFuture(true));

            CompletableFuture<Boolean> future = service.addAsync("users", "item1");

            assertThat(future.join()).isTrue();
        }

        @Test
        @DisplayName("should check asynchronously with correct shard routing")
        void existsAsyncRoutes() {
            when(shardRouter.resolveShardKey("users", "item1"))
                    .thenReturn("bf:users:shard:7");
            when(commandExecutor.existsAsync("bf:users:shard:7", "item1"))
                    .thenReturn(CompletableFuture.completedFuture(false));

            CompletableFuture<Boolean> future = service.mightContainAsync("users", "item1");

            assertThat(future.join()).isFalse();
        }
    }

    @Nested
    @DisplayName("Management Operations")
    class ManagementTests {

        @Test
        @DisplayName("should create filter with correct per-shard capacity")
        void createFilterDistributesCapacity() {
            when(properties.getKeyPrefix()).thenReturn("bf");
            when(properties.getExpansion()).thenReturn(2);
            when(properties.isNonScaling()).thenReturn(false);

            service.createFilter("users", 1_000_000, 0.001, 4);

            // Should call reserve 4 times with 250,000 capacity each
            verify(commandExecutor, times(4)).reserve(
                    argThat(key -> key.startsWith("bf:users:shard:")),
                    eq(0.001),
                    eq(250_000L),
                    eq(2),
                    eq(false)
            );
        }

        @Test
        @DisplayName("should delete all shards when deleting a filter")
        void deleteRemovesAllShards() {
            when(shardRouter.getAllShardKeys("users"))
                    .thenReturn(List.of("bf:users:shard:0", "bf:users:shard:1"));
            when(commandExecutor.delete(anyString())).thenReturn(true);

            service.deleteFilter("users");

            verify(commandExecutor).delete("bf:users:shard:0");
            verify(commandExecutor).delete("bf:users:shard:1");
        }

        @Test
        @DisplayName("should aggregate info across all shards")
        void getFilterInfoAggregates() {
            when(shardRouter.getAllShardKeys("users"))
                    .thenReturn(List.of("bf:users:shard:0", "bf:users:shard:1"));
            when(commandExecutor.keyExists(anyString())).thenReturn(true);
            when(commandExecutor.info("bf:users:shard:0"))
                    .thenReturn(BloomFilterInfo.builder()
                            .key("bf:users:shard:0")
                            .capacity(500_000)
                            .memoryUsageBytes(1_000_000)
                            .numberOfItemsInserted(100_000)
                            .build());
            when(commandExecutor.info("bf:users:shard:1"))
                    .thenReturn(BloomFilterInfo.builder()
                            .key("bf:users:shard:1")
                            .capacity(500_000)
                            .memoryUsageBytes(1_000_000)
                            .numberOfItemsInserted(150_000)
                            .build());

            BloomFilterResponse.AggregatedInfo info = service.getFilterInfo("users");

            assertThat(info.getTotalCapacity()).isEqualTo(1_000_000);
            assertThat(info.getTotalMemoryUsageBytes()).isEqualTo(2_000_000);
            assertThat(info.getTotalItemsInserted()).isEqualTo(250_000);
            assertThat(info.getShardCount()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Bulk Add")
    class BulkAddTests {

        @Test
        @DisplayName("should return 0 for null input")
        void bulkAddNull() {
            long result = service.bulkAdd("users", null, 1000);
            assertThat(result).isEqualTo(0);
        }

        @Test
        @DisplayName("should return 0 for empty input")
        void bulkAddEmpty() {
            long result = service.bulkAdd("users", List.of(), 1000);
            assertThat(result).isEqualTo(0);
        }
    }
}
