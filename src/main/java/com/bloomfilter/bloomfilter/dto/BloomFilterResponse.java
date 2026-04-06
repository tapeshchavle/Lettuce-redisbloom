package com.bloomfilter.bloomfilter.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Response DTOs for Bloom Filter API operations.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BloomFilterResponse {

    /** Filter name */
    private String filterName;

    /** Operation result for single-item operations */
    private Boolean result;

    /** Operation results for batch operations */
    private List<Boolean> results;

    /** Whether the item might exist (true) or definitely does not (false) */
    private Boolean mightExist;

    /** Human-readable message */
    private String message;

    /** Number of items processed */
    private Integer itemsProcessed;

    /** Processing time in milliseconds */
    private Long processingTimeMs;

    /** Timestamp of the operation */
    @Builder.Default
    private Instant timestamp = Instant.now();

    /** Aggregated filter info across all shards */
    private AggregatedInfo info;

    /**
     * Aggregated information across all shards of a bloom filter.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggregatedInfo {
        private String filterName;
        private int shardCount;
        private long totalCapacity;
        private long totalMemoryUsageBytes;
        private String totalMemoryUsageHuman;
        private long totalItemsInserted;
        private double averageFillRatio;
        private List<BloomFilterInfo> shardDetails;
    }

    // ─── Static Factory Methods ────────────────────────────────────

    public static BloomFilterResponse addResult(String filterName, boolean wasNew, long processingTimeMs) {
        return BloomFilterResponse.builder()
                .filterName(filterName)
                .result(wasNew)
                .message(wasNew ? "Item added successfully" : "Item may already exist")
                .processingTimeMs(processingTimeMs)
                .build();
    }

    public static BloomFilterResponse existsResult(String filterName, String item, boolean mightExist, long processingTimeMs) {
        return BloomFilterResponse.builder()
                .filterName(filterName)
                .mightExist(mightExist)
                .message(mightExist
                        ? "Item MIGHT exist in filter (possible false positive)"
                        : "Item DEFINITELY does not exist in filter")
                .processingTimeMs(processingTimeMs)
                .build();
    }

    public static BloomFilterResponse batchResult(String filterName, List<Boolean> results,
                                                   int itemsProcessed, long processingTimeMs) {
        return BloomFilterResponse.builder()
                .filterName(filterName)
                .results(results)
                .itemsProcessed(itemsProcessed)
                .processingTimeMs(processingTimeMs)
                .build();
    }

    public static BloomFilterResponse created(String filterName) {
        return BloomFilterResponse.builder()
                .filterName(filterName)
                .message("Bloom filter created successfully")
                .build();
    }

    public static BloomFilterResponse deleted(String filterName) {
        return BloomFilterResponse.builder()
                .filterName(filterName)
                .message("Bloom filter deleted successfully")
                .build();
    }
}
