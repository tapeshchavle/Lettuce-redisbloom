package com.bloomfilter.bloomfilter.service;

import com.bloomfilter.bloomfilter.config.BloomFilterProperties;
import com.bloomfilter.bloomfilter.dto.BloomFilterInfo;
import com.bloomfilter.bloomfilter.dto.BloomFilterResponse;
import com.bloomfilter.bloomfilter.metrics.BloomFilterMetrics;
import com.bloomfilter.bloomfilter.redis.RedisBloomCommandExecutor;
import com.bloomfilter.bloomfilter.shard.ShardRouter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Production implementation of {@link BloomFilterService} backed by Redis (RedisBloom via Lettuce).
 *
 * <h3>Architecture</h3>
 * <pre>
 * Client → BloomFilterService → ShardRouter → RedisBloomCommandExecutor → Redis
 *                                    ↓
 *                              MurmurHash3
 *                           (item → shard index)
 * </pre>
 *
 * <h3>Shard-Aware Operations</h3>
 * <p>Every operation routes items through the {@link ShardRouter} to determine which
 * Redis key (shard) to target. Batch operations group items by shard and send
 * one BF.MADD/BF.MEXISTS per shard, minimizing round trips.</p>
 *
 * <h3>Metrics</h3>
 * <p>All operations are instrumented via {@link BloomFilterMetrics}:
 * <ul>
 *   <li>Operation counts (add, exists, batch)</li>
 *   <li>Latency histograms</li>
 *   <li>Error counters</li>
 *   <li>Batch size distributions</li>
 * </ul>
 */
@Slf4j
@Service
public class RedisBloomFilterService implements BloomFilterService {

    private final RedisBloomCommandExecutor commandExecutor;
    private final ShardRouter shardRouter;
    private final BloomFilterProperties properties;
    private final BloomFilterMetrics metrics;

    public RedisBloomFilterService(RedisBloomCommandExecutor commandExecutor,
                                   ShardRouter shardRouter,
                                   BloomFilterProperties properties,
                                   BloomFilterMetrics metrics) {
        this.commandExecutor = commandExecutor;
        this.shardRouter = shardRouter;
        this.properties = properties;
        this.metrics = metrics;
    }

    // ─── Single Item Operations ────────────────────────────────────

    @Override
    public boolean add(String filterName, String item) {
        long start = System.nanoTime();
        try {
            String shardKey = shardRouter.resolveShardKey(filterName, item);
            boolean result = commandExecutor.add(shardKey, item);
            metrics.recordAdd(filterName, System.nanoTime() - start);
            log.debug("BF.ADD filter={}, shard={}, item={}, wasNew={}", filterName, shardKey, item, result);
            return result;
        } catch (Exception e) {
            metrics.recordError(filterName, "add");
            throw e;
        }
    }

    @Override
    public boolean mightContain(String filterName, String item) {
        long start = System.nanoTime();
        try {
            String shardKey = shardRouter.resolveShardKey(filterName, item);
            boolean result = commandExecutor.exists(shardKey, item);
            metrics.recordExists(filterName, result, System.nanoTime() - start);
            log.debug("BF.EXISTS filter={}, shard={}, item={}, mightExist={}", filterName, shardKey, item, result);
            return result;
        } catch (Exception e) {
            metrics.recordError(filterName, "exists");
            throw e;
        }
    }

    // ─── Batch Operations ──────────────────────────────────────────

    @Override
    public List<Boolean> multiAdd(String filterName, List<String> items) {
        if (items == null || items.isEmpty()) return List.of();

        long start = System.nanoTime();
        try {
            metrics.recordBatchSize(filterName, "add", items.size());

            // Group items by shard for efficient batch dispatch
            Map<String, List<String>> shardGroups = shardRouter.groupByShard(filterName, items);

            // Track results in original order
            Map<String, Boolean> resultMap = new LinkedHashMap<>();

            for (Map.Entry<String, List<String>> entry : shardGroups.entrySet()) {
                String shardKey = entry.getKey();
                List<String> shardItems = entry.getValue();

                // Process in sub-batches to avoid exceeding Redis command size limits
                List<Boolean> shardResults = processInBatches(shardKey, shardItems, true);

                for (int i = 0; i < shardItems.size(); i++) {
                    resultMap.put(shardItems.get(i), shardResults.get(i));
                }
            }

            // Reconstruct results in original item order
            List<Boolean> orderedResults = items.stream()
                    .map(resultMap::get)
                    .collect(Collectors.toList());

            metrics.recordBatchOperation(filterName, "add", items.size(), System.nanoTime() - start);
            log.info("BF.MADD filter={}, totalItems={}, shards={}, durationMs={}",
                    filterName, items.size(), shardGroups.size(),
                    (System.nanoTime() - start) / 1_000_000);

            return orderedResults;
        } catch (Exception e) {
            metrics.recordError(filterName, "multiAdd");
            throw e;
        }
    }

    @Override
    public List<Boolean> multiMightContain(String filterName, List<String> items) {
        if (items == null || items.isEmpty()) return List.of();

        long start = System.nanoTime();
        try {
            metrics.recordBatchSize(filterName, "exists", items.size());

            Map<String, List<String>> shardGroups = shardRouter.groupByShard(filterName, items);
            Map<String, Boolean> resultMap = new LinkedHashMap<>();

            for (Map.Entry<String, List<String>> entry : shardGroups.entrySet()) {
                String shardKey = entry.getKey();
                List<String> shardItems = entry.getValue();

                List<Boolean> shardResults = processInBatches(shardKey, shardItems, false);

                for (int i = 0; i < shardItems.size(); i++) {
                    resultMap.put(shardItems.get(i), shardResults.get(i));
                }
            }

            List<Boolean> orderedResults = items.stream()
                    .map(resultMap::get)
                    .collect(Collectors.toList());

            metrics.recordBatchOperation(filterName, "exists", items.size(), System.nanoTime() - start);
            log.info("BF.MEXISTS filter={}, totalItems={}, shards={}, durationMs={}",
                    filterName, items.size(), shardGroups.size(),
                    (System.nanoTime() - start) / 1_000_000);

            return orderedResults;
        } catch (Exception e) {
            metrics.recordError(filterName, "multiExists");
            throw e;
        }
    }

    // ─── Async Operations ──────────────────────────────────────────

    @Override
    public CompletableFuture<Boolean> addAsync(String filterName, String item) {
        String shardKey = shardRouter.resolveShardKey(filterName, item);
        long start = System.nanoTime();
        return commandExecutor.addAsync(shardKey, item)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        metrics.recordError(filterName, "addAsync");
                    } else {
                        metrics.recordAdd(filterName, System.nanoTime() - start);
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> mightContainAsync(String filterName, String item) {
        String shardKey = shardRouter.resolveShardKey(filterName, item);
        long start = System.nanoTime();
        return commandExecutor.existsAsync(shardKey, item)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        metrics.recordError(filterName, "existsAsync");
                    } else {
                        metrics.recordExists(filterName, result, System.nanoTime() - start);
                    }
                });
    }

    // ─── Bulk Operations ───────────────────────────────────────────

    @Override
    public long bulkAdd(String filterName, Collection<String> items, int batchSize) {
        if (items == null || items.isEmpty()) return 0;

        long start = System.nanoTime();
        AtomicLong totalProcessed = new AtomicLong(0);

        log.info("Starting bulk add: filter={}, totalItems={}, batchSize={}", filterName, items.size(), batchSize);

        // Convert to list for efficient subList access
        List<String> itemList = items instanceof List ? (List<String>) items : new ArrayList<>(items);

        // Process in chunks to avoid memory pressure
        for (int i = 0; i < itemList.size(); i += batchSize) {
            int end = Math.min(i + batchSize, itemList.size());
            List<String> batch = itemList.subList(i, end);
            multiAdd(filterName, batch);
            totalProcessed.addAndGet(batch.size());

            // Log progress every 10 batches
            if ((i / batchSize) % 10 == 0) {
                log.info("Bulk add progress: filter={}, processed={}/{} ({}%)",
                        filterName, totalProcessed.get(), itemList.size(),
                        String.format("%.1f", (totalProcessed.get() * 100.0) / itemList.size()));
            }
        }

        long durationMs = (System.nanoTime() - start) / 1_000_000;
        long throughput = durationMs > 0 ? (totalProcessed.get() * 1000 / durationMs) : totalProcessed.get();
        log.info("Bulk add completed: filter={}, totalItems={}, durationMs={}, throughput={} items/sec",
                filterName, totalProcessed.get(), durationMs, throughput);

        return totalProcessed.get();
    }

    // ─── Management Operations ─────────────────────────────────────

    @Override
    public void createFilter(String filterName, long capacity, double errorRate, int shardCount) {
        log.info("Creating bloom filter: name={}, capacity={}, errorRate={}, shards={}",
                filterName, capacity, errorRate, shardCount);

        long perShardCapacity = capacity / shardCount;
        for (int i = 0; i < shardCount; i++) {
            String shardKey = String.format("%s:%s:shard:%d", properties.getKeyPrefix(), filterName, i);
            commandExecutor.reserve(shardKey, errorRate, perShardCapacity,
                    properties.getExpansion(), properties.isNonScaling());
        }

        log.info("Created bloom filter '{}' with {} shards, {} capacity per shard",
                filterName, shardCount, perShardCapacity);
    }

    @Override
    public BloomFilterResponse.AggregatedInfo getFilterInfo(String filterName) {
        List<String> shardKeys = shardRouter.getAllShardKeys(filterName);
        List<BloomFilterInfo> shardInfos = new ArrayList<>();

        long totalCapacity = 0;
        long totalMemory = 0;
        long totalItems = 0;

        for (String shardKey : shardKeys) {
            try {
                if (commandExecutor.keyExists(shardKey)) {
                    BloomFilterInfo info = commandExecutor.info(shardKey);
                    shardInfos.add(info);
                    totalCapacity += info.getCapacity();
                    totalMemory += info.getMemoryUsageBytes();
                    totalItems += info.getNumberOfItemsInserted();
                }
            } catch (Exception e) {
                log.warn("Could not get info for shard: {}", shardKey, e);
            }
        }

        double avgFillRatio = totalCapacity > 0 ? (double) totalItems / totalCapacity : 0.0;

        // Format memory
        String memoryHuman;
        if (totalMemory < 1024) memoryHuman = totalMemory + " B";
        else if (totalMemory < 1024 * 1024) memoryHuman = String.format("%.1f KB", totalMemory / 1024.0);
        else if (totalMemory < 1024L * 1024 * 1024) memoryHuman = String.format("%.1f MB", totalMemory / (1024.0 * 1024));
        else memoryHuman = String.format("%.2f GB", totalMemory / (1024.0 * 1024 * 1024));

        return BloomFilterResponse.AggregatedInfo.builder()
                .filterName(filterName)
                .shardCount(shardInfos.size())
                .totalCapacity(totalCapacity)
                .totalMemoryUsageBytes(totalMemory)
                .totalMemoryUsageHuman(memoryHuman)
                .totalItemsInserted(totalItems)
                .averageFillRatio(avgFillRatio)
                .shardDetails(shardInfos)
                .build();
    }

    @Override
    public void deleteFilter(String filterName) {
        List<String> shardKeys = shardRouter.getAllShardKeys(filterName);
        int deleted = 0;
        for (String shardKey : shardKeys) {
            if (commandExecutor.delete(shardKey)) {
                deleted++;
            }
        }
        log.info("Deleted bloom filter '{}': removed {}/{} shards", filterName, deleted, shardKeys.size());
    }

    // ─── Internal Helpers ──────────────────────────────────────────

    /**
     * Processes items in sub-batches via BF.MADD or BF.MEXISTS.
     * This prevents exceeding Redis's max command argument limit and
     * keeps memory pressure manageable for very large batches.
     */
    private List<Boolean> processInBatches(String shardKey, List<String> items, boolean isAdd) {
        int batchSize = properties.getBatchSize();
        if (items.size() <= batchSize) {
            String[] itemArray = items.toArray(new String[0]);
            return isAdd
                    ? commandExecutor.multiAdd(shardKey, itemArray)
                    : commandExecutor.multiExists(shardKey, itemArray);
        }

        List<Boolean> allResults = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            String[] batch = items.subList(i, end).toArray(new String[0]);
            List<Boolean> batchResults = isAdd
                    ? commandExecutor.multiAdd(shardKey, batch)
                    : commandExecutor.multiExists(shardKey, batch);
            allResults.addAll(batchResults);
        }
        return allResults;
    }
}
