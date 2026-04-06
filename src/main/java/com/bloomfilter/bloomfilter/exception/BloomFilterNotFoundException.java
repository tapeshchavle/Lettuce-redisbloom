package com.bloomfilter.bloomfilter.exception;

/**
 * Thrown when a referenced bloom filter does not exist in Redis.
 */
public class BloomFilterNotFoundException extends BloomFilterException {

    private final String filterName;

    public BloomFilterNotFoundException(String filterName) {
        super("Bloom filter not found: " + filterName);
        this.filterName = filterName;
    }

    public String getFilterName() {
        return filterName;
    }
}
