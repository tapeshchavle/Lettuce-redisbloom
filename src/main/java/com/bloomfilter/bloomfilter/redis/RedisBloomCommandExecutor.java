package com.bloomfilter.bloomfilter.redis;

import com.bloomfilter.bloomfilter.dto.BloomFilterInfo;
import com.bloomfilter.bloomfilter.exception.BloomFilterException;
import com.bloomfilter.bloomfilter.exception.RedisBloomModuleException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.CommandArgs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.stereotype.Component;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Low-level command executor for RedisBloom operations using Lettuce's dispatch mechanism.
 *
 * <p>This class provides both synchronous and asynchronous variants of all RedisBloom
 * commands. It uses Lettuce's {@code dispatch()} method to send custom commands that
 * are not part of the standard Redis command set.</p>
 *
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe. Lettuce connections are multiplexed — a single connection
 * handles multiple concurrent commands. The connection factory manages pooling.</p>
 *
 * <h3>Performance Characteristics</h3>
 * <ul>
 *   <li>Single operations: O(k) where k = number of hash functions</li>
 *   <li>Batch operations (MADD/MEXISTS): O(n*k) but single round-trip</li>
 *   <li>Network: 1 RTT per call (use batch methods to amortize)</li>
 * </ul>
 */
@Slf4j
@Component
public class RedisBloomCommandExecutor {

    private final LettuceConnectionFactory connectionFactory;

    public RedisBloomCommandExecutor(LettuceConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    // ─── BF.RESERVE ────────────────────────────────────────────────────

    /**
     * Creates a new Bloom Filter with specified parameters.
     *
     * @param key        Redis key for the bloom filter
     * @param errorRate  desired false positive rate (e.g., 0.001 for 0.1%)
     * @param capacity   expected number of items (per shard)
     * @param expansion  sub-filter expansion factor (typically 2)
     * @param nonScaling if true, filter will not auto-expand when full
     * @throws BloomFilterException if the filter already exists or Redis error occurs
     */
    public void reserve(String key, double errorRate, long capacity, int expansion, boolean nonScaling) {
        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .addKey(key)
                    .add(errorRate)
                    .add(capacity);

            if (nonScaling) {
                args.add("NONSCALING");
            } else {
                args.add("EXPANSION");
                args.add(expansion);
            }

            sync.dispatch(BloomFilterCommand.BF_RESERVE, new StatusOutput<>(StringCodec.UTF8), args);
            log.info("Reserved bloom filter: key={}, errorRate={}, capacity={}, expansion={}, nonScaling={}",
                    key, errorRate, capacity, expansion, nonScaling);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("item exists")) {
                log.debug("Bloom filter already exists: key={}", key);
                return; // Idempotent — filter already exists
            }
            throw new BloomFilterException("Failed to reserve bloom filter: " + key, e);
        }
    }

    // ─── BF.ADD ────────────────────────────────────────────────────────

    /**
     * Adds a single item to the bloom filter.
     *
     * @return true if the item was newly added, false if it may have already existed
     */
    public boolean add(String key, String item) {
        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .addKey(key)
                    .addValue(item);

            Long result = sync.dispatch(BloomFilterCommand.BF_ADD, new IntegerOutput<>(StringCodec.UTF8), args);
            return result != null && result == 1L;
        } catch (Exception e) {
            throw wrapException("BF.ADD", key, e);
        }
    }

    /**
     * Async variant of {@link #add(String, String)}.
     */
    public CompletableFuture<Boolean> addAsync(String key, String item) {
        StatefulRedisConnection<String, String> conn = getNativeConnection();
        RedisAsyncCommands<String, String> async = conn.async();
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .addKey(key)
                .addValue(item);

        RedisFuture<Long> future = async.dispatch(BloomFilterCommand.BF_ADD,
                new IntegerOutput<>(StringCodec.UTF8), args);

        return future.toCompletableFuture()
                .thenApply(result -> result != null && result == 1L)
                .whenComplete((result, ex) -> conn.close());
    }

    // ─── BF.MADD ───────────────────────────────────────────────────────

    /**
     * Adds multiple items to the bloom filter in a single round-trip.
     * This is the primary method for bulk ingestion at scale.
     *
     * @param key   Redis key
     * @param items items to add
     * @return list of booleans — true if newly added, false if may have existed
     */
    public List<Boolean> multiAdd(String key, String... items) {
        if (items == null || items.length == 0) {
            return List.of();
        }
        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .addKey(key);
            for (String item : items) {
                args.addValue(item);
            }

            @SuppressWarnings("unchecked")
            List<Long> results = (List<Long>) sync.dispatch(BloomFilterCommand.BF_MADD,
                    (CommandOutput) new ArrayOutput<>(StringCodec.UTF8), args);

            return toLongBooleanList(results);
        } catch (Exception e) {
            throw wrapException("BF.MADD", key, e);
        }
    }

    /**
     * Async variant of {@link #multiAdd(String, String...)}.
     */
    public CompletableFuture<List<Boolean>> multiAddAsync(String key, String... items) {
        if (items == null || items.length == 0) {
            return CompletableFuture.completedFuture(List.of());
        }
        StatefulRedisConnection<String, String> conn = getNativeConnection();
        RedisAsyncCommands<String, String> async = conn.async();
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .addKey(key);
        for (String item : items) {
            args.addValue(item);
        }

        @SuppressWarnings("unchecked")
        RedisFuture<List<Long>> future = (RedisFuture<List<Long>>) (RedisFuture<?>) async.dispatch(BloomFilterCommand.BF_MADD,
                (CommandOutput) new ArrayOutput<>(StringCodec.UTF8), args);

        return future.toCompletableFuture()
                .thenApply(this::toLongBooleanList)
                .whenComplete((result, ex) -> conn.close());
    }

    // ─── BF.EXISTS ─────────────────────────────────────────────────────

    /**
     * Checks if an item may exist in the bloom filter.
     *
     * @return true if the item MIGHT exist (possible false positive),
     *         false if the item DEFINITELY does not exist (zero false negatives)
     */
    public boolean exists(String key, String item) {
        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .addKey(key)
                    .addValue(item);

            Long result = sync.dispatch(BloomFilterCommand.BF_EXISTS,
                    new IntegerOutput<>(StringCodec.UTF8), args);
            return result != null && result == 1L;
        } catch (Exception e) {
            throw wrapException("BF.EXISTS", key, e);
        }
    }

    /**
     * Async variant of {@link #exists(String, String)}.
     */
    public CompletableFuture<Boolean> existsAsync(String key, String item) {
        StatefulRedisConnection<String, String> conn = getNativeConnection();
        RedisAsyncCommands<String, String> async = conn.async();
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .addKey(key)
                .addValue(item);

        RedisFuture<Long> future = async.dispatch(BloomFilterCommand.BF_EXISTS,
                new IntegerOutput<>(StringCodec.UTF8), args);

        return future.toCompletableFuture()
                .thenApply(result -> result != null && result == 1L)
                .whenComplete((result, ex) -> conn.close());
    }

    // ─── BF.MEXISTS ────────────────────────────────────────────────────

    /**
     * Checks if multiple items may exist in the bloom filter in a single round-trip.
     *
     * @return list of booleans — true if might exist, false if definitely absent
     */
    public List<Boolean> multiExists(String key, String... items) {
        if (items == null || items.length == 0) {
            return List.of();
        }
        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .addKey(key);
            for (String item : items) {
                args.addValue(item);
            }

            @SuppressWarnings("unchecked")
            List<Long> results = (List<Long>) sync.dispatch(BloomFilterCommand.BF_MEXISTS,
                    (CommandOutput) new ArrayOutput<>(StringCodec.UTF8), args);

            return toLongBooleanList(results);
        } catch (Exception e) {
            throw wrapException("BF.MEXISTS", key, e);
        }
    }

    /**
     * Async variant of {@link #multiExists(String, String...)}.
     */
    public CompletableFuture<List<Boolean>> multiExistsAsync(String key, String... items) {
        if (items == null || items.length == 0) {
            return CompletableFuture.completedFuture(List.of());
        }
        StatefulRedisConnection<String, String> conn = getNativeConnection();
        RedisAsyncCommands<String, String> async = conn.async();
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .addKey(key);
        for (String item : items) {
            args.addValue(item);
        }

        @SuppressWarnings("unchecked")
        RedisFuture<List<Long>> future = (RedisFuture<List<Long>>) (RedisFuture<?>) async.dispatch(BloomFilterCommand.BF_MEXISTS,
                (CommandOutput) new ArrayOutput<>(StringCodec.UTF8), args);

        return future.toCompletableFuture()
                .thenApply(this::toLongBooleanList)
                .whenComplete((result, ex) -> conn.close());
    }

    // ─── BF.INFO ───────────────────────────────────────────────────────

    /**
     * Retrieves information about a bloom filter.
     *
     * @return {@link BloomFilterInfo} with capacity, size, number of filters, etc.
     */
    public BloomFilterInfo info(String key) {
        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .addKey(key);

            @SuppressWarnings("unchecked")
            List<Object> results = (List<Object>) sync.dispatch(BloomFilterCommand.BF_INFO,
                    (CommandOutput) new ArrayOutput<>(StringCodec.UTF8), args);

            return parseInfoResponse(key, results);
        } catch (Exception e) {
            throw wrapException("BF.INFO", key, e);
        }
    }

    // ─── BF.CARD ───────────────────────────────────────────────────────

    /**
     * Returns the number of items added to the bloom filter.
     */
    public long cardinality(String key) {
        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .addKey(key);

            Long result = sync.dispatch(BloomFilterCommand.BF_CARD,
                    new IntegerOutput<>(StringCodec.UTF8), args);
            return result != null ? result : 0L;
        } catch (Exception e) {
            throw wrapException("BF.CARD", key, e);
        }
    }

    // ─── DELETE (standard Redis DEL) ───────────────────────────────────

    /**
     * Deletes a bloom filter key from Redis.
     */
    public boolean delete(String key) {
        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            Long deleted = sync.del(key);
            return deleted != null && deleted > 0;
        } catch (Exception e) {
            throw wrapException("DEL", key, e);
        }
    }

    /**
     * Checks if the RedisBloom module is loaded on the server.
     *
     * @return true if RedisBloom is available
     */
    public boolean isRedisBloomAvailable() {
        // Custom ProtocolKeyword for MODULE command (not in Lettuce's CommandType enum)
        ProtocolKeyword moduleCmd = new ProtocolKeyword() {
            @Override
            public byte[] getBytes() {
                return "MODULE".getBytes(StandardCharsets.US_ASCII);
            }
        };

        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            @SuppressWarnings("unchecked")
            List<Object> modules = (List<Object>) sync.dispatch(
                    moduleCmd,
                    (CommandOutput) new ArrayOutput<>(StringCodec.UTF8),
                    new CommandArgs<>(StringCodec.UTF8).add("LIST"));

            if (modules == null) return false;

            // MODULE LIST returns nested arrays; look for "bf" in the response
            return modules.toString().toLowerCase().contains("bf");
        } catch (Exception e) {
            log.warn("Could not check RedisBloom module availability", e);
            return false;
        }
    }

    /**
     * Checks if a bloom filter key exists in Redis.
     */
    public boolean keyExists(String key) {
        try (var conn = getNativeConnection()) {
            RedisCommands<String, String> sync = conn.sync();
            Long exists = sync.exists(key);
            return exists != null && exists > 0;
        } catch (Exception e) {
            throw wrapException("EXISTS", key, e);
        }
    }

    // ─── Internal Helpers ──────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private StatefulRedisConnection<String, String> getNativeConnection() {
        try {
            return (StatefulRedisConnection<String, String>)
                    connectionFactory.getConnection().getNativeConnection();
        } catch (Exception e) {
            throw new BloomFilterException("Failed to obtain Redis connection", e);
        }
    }

    private List<Boolean> toLongBooleanList(List<Long> results) {
        if (results == null) return List.of();
        List<Boolean> booleans = new ArrayList<>(results.size());
        for (Long val : results) {
            booleans.add(val != null && val == 1L);
        }
        return booleans;
    }

    private BloomFilterInfo parseInfoResponse(String key, List<Object> results) {
        if (results == null || results.isEmpty()) {
            return BloomFilterInfo.empty(key);
        }

        // BF.INFO returns alternating key-value pairs:
        // [Capacity, 1000000, Size, 7794184, Number of filters, 1, Number of items inserted, 0, Expansion rate, 2]
        long capacity = 0;
        long size = 0;
        int numberOfFilters = 0;
        long numberOfItemsInserted = 0;
        int expansionRate = 0;

        for (int i = 0; i < results.size() - 1; i += 2) {
            String field = String.valueOf(results.get(i));
            long value = toLong(results.get(i + 1));

            switch (field) {
                case "Capacity" -> capacity = value;
                case "Size" -> size = value;
                case "Number of filters" -> numberOfFilters = (int) value;
                case "Number of items inserted" -> numberOfItemsInserted = value;
                case "Expansion rate" -> expansionRate = (int) value;
            }
        }

        return BloomFilterInfo.builder()
                .key(key)
                .capacity(capacity)
                .memoryUsageBytes(size)
                .numberOfSubFilters(numberOfFilters)
                .numberOfItemsInserted(numberOfItemsInserted)
                .expansionRate(expansionRate)
                .build();
    }

    private long toLong(Object obj) {
        if (obj instanceof Number num) {
            return num.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(obj));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private BloomFilterException wrapException(String command, String key, Exception e) {
        if (e instanceof BloomFilterException bfe) {
            return bfe;
        }
        String message = e.getMessage();
        if (message != null && message.contains("unknown command")) {
            throw new RedisBloomModuleException(
                    "RedisBloom module is not loaded. Command '" + command + "' is unavailable. " +
                    "Ensure Redis Stack or the RedisBloom module is installed.", e);
        }
        return new BloomFilterException(
                String.format("Redis command %s failed for key '%s': %s", command, key, message), e);
    }
}
