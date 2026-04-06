package com.bloomfilter.bloomfilter.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Request DTOs for Bloom Filter API operations.
 */
public final class BloomFilterRequest {

    private BloomFilterRequest() {}

    /**
     * Request to add a single item to a bloom filter.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AddItem {
        @NotBlank(message = "Item must not be blank")
        @Size(max = 10_000, message = "Item must not exceed 10,000 characters")
        private String item;
    }

    /**
     * Request to add multiple items to a bloom filter (batch operation).
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchAdd {
        @NotEmpty(message = "Items list must not be empty")
        @Size(max = 100_000, message = "Batch size must not exceed 100,000 items")
        private List<@NotBlank String> items;
    }

    /**
     * Request to check multiple items against a bloom filter (batch operation).
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchCheck {
        @NotEmpty(message = "Items list must not be empty")
        @Size(max = 100_000, message = "Batch size must not exceed 100,000 items")
        private List<@NotBlank String> items;
    }

    /**
     * Request to create a new bloom filter with custom parameters.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateFilter {
        private Long capacity;
        private Double errorRate;
        private Integer shardCount;
    }
}
