package com.bloomfilter.bloomfilter.controller;

import com.bloomfilter.bloomfilter.config.BloomFilterProperties;
import com.bloomfilter.bloomfilter.dto.BloomFilterRequest;
import com.bloomfilter.bloomfilter.dto.BloomFilterResponse;
import com.bloomfilter.bloomfilter.service.BloomFilterService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST API controller for Bloom Filter operations.
 *
 * <h3>Endpoints</h3>
 * <ul>
 *   <li>{@code POST /api/v1/bloom/{filterName}/add} — Add single item</li>
 *   <li>{@code POST /api/v1/bloom/{filterName}/add/batch} — Batch add items</li>
 *   <li>{@code GET  /api/v1/bloom/{filterName}/exists/{item}} — Check single item</li>
 *   <li>{@code POST /api/v1/bloom/{filterName}/exists/batch} — Batch check items</li>
 *   <li>{@code POST /api/v1/bloom/{filterName}} — Create filter</li>
 *   <li>{@code GET  /api/v1/bloom/{filterName}/info} — Get filter info</li>
 *   <li>{@code DELETE /api/v1/bloom/{filterName}} — Delete filter</li>
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/bloom")
public class BloomFilterController {

    private final BloomFilterService bloomFilterService;
    private final BloomFilterProperties properties;

    public BloomFilterController(BloomFilterService bloomFilterService,
                                 BloomFilterProperties properties) {
        this.bloomFilterService = bloomFilterService;
        this.properties = properties;
    }

    // ─── Add Operations ────────────────────────────────────────────

    /**
     * Add a single item to the bloom filter.
     *
     * @param filterName logical filter name (e.g., "users", "posts")
     * @param request    contains the item to add
     * @return whether the item was newly added or may have already existed
     */
    @PostMapping("/{filterName}/add")
    public ResponseEntity<BloomFilterResponse> addItem(
            @PathVariable String filterName,
            @Valid @RequestBody BloomFilterRequest.AddItem request) {

        long start = System.currentTimeMillis();
        boolean wasNew = bloomFilterService.add(filterName, request.getItem());
        long duration = System.currentTimeMillis() - start;

        return ResponseEntity.ok(BloomFilterResponse.addResult(filterName, wasNew, duration));
    }

    /**
     * Add multiple items to the bloom filter in a single request.
     * Items are automatically distributed across shards and batched.
     *
     * @param filterName logical filter name
     * @param request    contains the list of items to add (max 100,000)
     */
    @PostMapping("/{filterName}/add/batch")
    public ResponseEntity<BloomFilterResponse> batchAdd(
            @PathVariable String filterName,
            @Valid @RequestBody BloomFilterRequest.BatchAdd request) {

        long start = System.currentTimeMillis();
        List<Boolean> results = bloomFilterService.multiAdd(filterName, request.getItems());
        long duration = System.currentTimeMillis() - start;

        return ResponseEntity.ok(
                BloomFilterResponse.batchResult(filterName, results, request.getItems().size(), duration));
    }

    // ─── Exists Operations ─────────────────────────────────────────

    /**
     * Check if a single item might exist in the bloom filter.
     *
     * <p><strong>Important</strong>: A {@code true} result means the item MIGHT exist
     * (possible false positive at the configured error rate). A {@code false} result
     * means the item DEFINITELY does not exist (zero false negatives).</p>
     *
     * @param filterName logical filter name
     * @param item       the item to check
     */
    @GetMapping("/{filterName}/exists/{item}")
    public ResponseEntity<BloomFilterResponse> checkExists(
            @PathVariable String filterName,
            @PathVariable String item) {

        long start = System.currentTimeMillis();
        boolean mightExist = bloomFilterService.mightContain(filterName, item);
        long duration = System.currentTimeMillis() - start;

        return ResponseEntity.ok(
                BloomFilterResponse.existsResult(filterName, item, mightExist, duration));
    }

    /**
     * Check if multiple items might exist in the bloom filter.
     *
     * @param filterName logical filter name
     * @param request    contains the list of items to check (max 100,000)
     */
    @PostMapping("/{filterName}/exists/batch")
    public ResponseEntity<BloomFilterResponse> batchCheck(
            @PathVariable String filterName,
            @Valid @RequestBody BloomFilterRequest.BatchCheck request) {

        long start = System.currentTimeMillis();
        List<Boolean> results = bloomFilterService.multiMightContain(filterName, request.getItems());
        long duration = System.currentTimeMillis() - start;

        return ResponseEntity.ok(
                BloomFilterResponse.batchResult(filterName, results, request.getItems().size(), duration));
    }

    // ─── Management Operations ─────────────────────────────────────

    /**
     * Create a new bloom filter with custom parameters.
     * If no parameters are provided, defaults from application.yml are used.
     *
     * @param filterName logical filter name
     * @param request    optional capacity, error rate, and shard count
     */
    @PostMapping("/{filterName}")
    public ResponseEntity<BloomFilterResponse> createFilter(
            @PathVariable String filterName,
            @Valid @RequestBody(required = false) BloomFilterRequest.CreateFilter request) {

        long capacity = (request != null && request.getCapacity() != null)
                ? request.getCapacity() : properties.getDefaultCapacity();
        double errorRate = (request != null && request.getErrorRate() != null)
                ? request.getErrorRate() : properties.getDefaultErrorRate();
        int shardCount = (request != null && request.getShardCount() != null)
                ? request.getShardCount() : properties.getDefaultShardCount();

        bloomFilterService.createFilter(filterName, capacity, errorRate, shardCount);

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(BloomFilterResponse.created(filterName));
    }

    /**
     * Get aggregated information about a bloom filter across all shards.
     *
     * @param filterName logical filter name
     */
    @GetMapping("/{filterName}/info")
    public ResponseEntity<BloomFilterResponse> getFilterInfo(@PathVariable String filterName) {
        BloomFilterResponse.AggregatedInfo info = bloomFilterService.getFilterInfo(filterName);

        return ResponseEntity.ok(BloomFilterResponse.builder()
                .filterName(filterName)
                .info(info)
                .build());
    }

    /**
     * Delete a bloom filter and all its shards.
     *
     * @param filterName logical filter name
     */
    @DeleteMapping("/{filterName}")
    public ResponseEntity<BloomFilterResponse> deleteFilter(@PathVariable String filterName) {
        bloomFilterService.deleteFilter(filterName);
        return ResponseEntity.ok(BloomFilterResponse.deleted(filterName));
    }
}
