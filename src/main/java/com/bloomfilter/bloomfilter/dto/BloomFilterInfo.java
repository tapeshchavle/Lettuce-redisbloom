package com.bloomfilter.bloomfilter.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents metadata and statistics about a Bloom Filter from BF.INFO.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BloomFilterInfo {

    /** Redis key or logical filter name */
    private String key;

    /** Total capacity (number of items the filter was created for) */
    private long capacity;

    /** Memory usage in bytes */
    private long memoryUsageBytes;

    /** Number of sub-filters (grows when filter expands beyond initial capacity) */
    private int numberOfSubFilters;

    /** Number of items that have been inserted */
    private long numberOfItemsInserted;

    /** Expansion rate when creating sub-filters */
    private int expansionRate;

    /** Human-readable memory usage */
    public String getMemoryUsageHuman() {
        if (memoryUsageBytes < 1024) return memoryUsageBytes + " B";
        if (memoryUsageBytes < 1024 * 1024) return String.format("%.1f KB", memoryUsageBytes / 1024.0);
        if (memoryUsageBytes < 1024 * 1024 * 1024) return String.format("%.1f MB", memoryUsageBytes / (1024.0 * 1024));
        return String.format("%.2f GB", memoryUsageBytes / (1024.0 * 1024 * 1024));
    }

    /** Fill ratio (items inserted / capacity) */
    public double getFillRatio() {
        return capacity > 0 ? (double) numberOfItemsInserted / capacity : 0.0;
    }

    public static BloomFilterInfo empty(String key) {
        return BloomFilterInfo.builder().key(key).build();
    }
}
