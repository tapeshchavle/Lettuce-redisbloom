package com.bloomfilter.bloomfilter.redis;

import com.bloomfilter.bloomfilter.exception.BloomFilterException;
import com.bloomfilter.bloomfilter.exception.RedisBloomModuleException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RedisBloomCommandExecutor}.
 *
 * <p>Tests all RedisBloom command dispatches with mocked Lettuce connections
 * to verify correct argument construction and response parsing.</p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RedisBloomCommandExecutor")
@SuppressWarnings("unchecked")
class RedisBloomCommandExecutorTest {

    @Mock private LettuceConnectionFactory connectionFactory;
    @Mock private RedisConnection redisConnection;
    @Mock private StatefulRedisConnection<String, String> nativeConnection;
    @Mock private RedisCommands<String, String> syncCommands;
    @Mock private RedisAsyncCommands<String, String> asyncCommands;

    private RedisBloomCommandExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new RedisBloomCommandExecutor(connectionFactory);
        lenient().when(connectionFactory.getConnection()).thenReturn(redisConnection);
        lenient().when(redisConnection.getNativeConnection()).thenReturn(nativeConnection);
        lenient().when(nativeConnection.sync()).thenReturn(syncCommands);
        lenient().when(nativeConnection.async()).thenReturn(asyncCommands);
    }

    @Nested
    @DisplayName("BF.ADD")
    class AddTests {

        @Test
        @DisplayName("should return true when item is newly added")
        void addNewItem() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(IntegerOutput.class), any(CommandArgs.class)))
                    .thenReturn(1L);

            boolean result = executor.add("bf:users:shard:0", "user123");

            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("should return false when item may already exist")
        void addExistingItem() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(IntegerOutput.class), any(CommandArgs.class)))
                    .thenReturn(0L);

            boolean result = executor.add("bf:users:shard:0", "user123");

            assertThat(result).isFalse();
        }

        @Test
        @DisplayName("should throw BloomFilterException on Redis error")
        void addRedisError() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(IntegerOutput.class), any(CommandArgs.class)))
                    .thenThrow(new RuntimeException("Connection refused"));

            assertThatThrownBy(() -> executor.add("bf:users:shard:0", "user123"))
                    .isInstanceOf(BloomFilterException.class)
                    .hasMessageContaining("BF.ADD failed");
        }

        @Test
        @DisplayName("should throw RedisBloomModuleException when module is missing")
        void addModuleMissing() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(IntegerOutput.class), any(CommandArgs.class)))
                    .thenThrow(new RuntimeException("ERR unknown command 'BF.ADD'"));

            assertThatThrownBy(() -> executor.add("bf:users:shard:0", "user123"))
                    .isInstanceOf(RedisBloomModuleException.class)
                    .hasMessageContaining("RedisBloom module is not loaded");
        }
    }

    @Nested
    @DisplayName("BF.EXISTS")
    class ExistsTests {

        @Test
        @DisplayName("should return true when item might exist")
        void existsFound() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(IntegerOutput.class), any(CommandArgs.class)))
                    .thenReturn(1L);

            boolean result = executor.exists("bf:users:shard:0", "user123");

            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("should return false when item definitely does not exist")
        void existsNotFound() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(IntegerOutput.class), any(CommandArgs.class)))
                    .thenReturn(0L);

            boolean result = executor.exists("bf:users:shard:0", "user123");

            assertThat(result).isFalse();
        }
    }

    @Nested
    @DisplayName("BF.MADD")
    class MultiAddTests {

        @Test
        @DisplayName("should return list of results for multiple items")
        void multiAddSuccess() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(CommandOutput.class), any(CommandArgs.class)))
                    .thenReturn(List.of(1L, 0L, 1L));

            List<Boolean> results = executor.multiAdd("bf:users:shard:0", "a", "b", "c");

            assertThat(results).containsExactly(true, false, true);
        }

        @Test
        @DisplayName("should return empty list for null items")
        void multiAddNullItems() {
            List<Boolean> results = executor.multiAdd("bf:users:shard:0", (String[]) null);
            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("should return empty list for empty items")
        void multiAddEmptyItems() {
            List<Boolean> results = executor.multiAdd("bf:users:shard:0");
            assertThat(results).isEmpty();
        }
    }

    @Nested
    @DisplayName("BF.MEXISTS")
    class MultiExistsTests {

        @Test
        @DisplayName("should return list of existence results")
        void multiExistsSuccess() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(CommandOutput.class), any(CommandArgs.class)))
                    .thenReturn(List.of(1L, 0L, 1L));

            List<Boolean> results = executor.multiExists("bf:users:shard:0", "a", "b", "c");

            assertThat(results).containsExactly(true, false, true);
        }
    }

    @Nested
    @DisplayName("BF.RESERVE")
    class ReserveTests {

        @Test
        @DisplayName("should create filter with all parameters")
        void reserveWithExpansion() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(StatusOutput.class), any(CommandArgs.class)))
                    .thenReturn("OK");

            assertThatCode(() -> executor.reserve("bf:users:shard:0", 0.001, 1_000_000, 2, false))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("should skip silently when filter already exists")
        void reserveAlreadyExists() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(StatusOutput.class), any(CommandArgs.class)))
                    .thenThrow(new RuntimeException("item exists"));

            assertThatCode(() -> executor.reserve("bf:users:shard:0", 0.001, 1_000_000, 2, false))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("BF.CARD")
    class CardinalityTests {

        @Test
        @DisplayName("should return cardinality count")
        void cardinalitySuccess() {
            when(syncCommands.dispatch(any(ProtocolKeyword.class), any(IntegerOutput.class), any(CommandArgs.class)))
                    .thenReturn(42L);

            long result = executor.cardinality("bf:users:shard:0");

            assertThat(result).isEqualTo(42L);
        }
    }

    @Nested
    @DisplayName("DELETE")
    class DeleteTests {

        @Test
        @DisplayName("should delete existing key")
        void deleteExisting() {
            when(syncCommands.del("bf:users:shard:0")).thenReturn(1L);

            boolean deleted = executor.delete("bf:users:shard:0");

            assertThat(deleted).isTrue();
        }

        @Test
        @DisplayName("should return false for non-existing key")
        void deleteNonExisting() {
            when(syncCommands.del("bf:users:shard:0")).thenReturn(0L);

            boolean deleted = executor.delete("bf:users:shard:0");

            assertThat(deleted).isFalse();
        }
    }

    @Nested
    @DisplayName("Async Operations")
    class AsyncTests {

        @Test
        @DisplayName("should add item asynchronously")
        void addAsyncSuccess() {
            RedisFuture<Long> mockFuture = mock(RedisFuture.class);
            when(mockFuture.toCompletableFuture())
                    .thenReturn(CompletableFuture.completedFuture(1L));
            when(asyncCommands.dispatch(any(ProtocolKeyword.class), any(IntegerOutput.class), any(CommandArgs.class)))
                    .thenReturn(mockFuture);

            CompletableFuture<Boolean> future = executor.addAsync("bf:users:shard:0", "user123");

            assertThat(future.join()).isTrue();
        }

        @Test
        @DisplayName("should check existence asynchronously")
        void existsAsyncSuccess() {
            RedisFuture<Long> mockFuture = mock(RedisFuture.class);
            when(mockFuture.toCompletableFuture())
                    .thenReturn(CompletableFuture.completedFuture(0L));
            when(asyncCommands.dispatch(any(ProtocolKeyword.class), any(IntegerOutput.class), any(CommandArgs.class)))
                    .thenReturn(mockFuture);

            CompletableFuture<Boolean> future = executor.existsAsync("bf:users:shard:0", "user123");

            assertThat(future.join()).isFalse();
        }
    }
}
