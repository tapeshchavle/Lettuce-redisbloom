package com.bloomfilter.bloomfilter.exception;

/**
 * Base exception for all Bloom Filter operations.
 */
public class BloomFilterException extends RuntimeException {

    public BloomFilterException(String message) {
        super(message);
    }

    public BloomFilterException(String message, Throwable cause) {
        super(message, cause);
    }
}
