package com.bloomfilter.bloomfilter.controller;

import com.bloomfilter.bloomfilter.config.BloomFilterProperties;
import com.bloomfilter.bloomfilter.dto.BloomFilterResponse;
import com.bloomfilter.bloomfilter.service.BloomFilterService;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Web layer tests for {@link BloomFilterController}.
 * Tests HTTP request/response mapping, validation, and error handling.
 */
@WebMvcTest(BloomFilterController.class)
@DisplayName("BloomFilterController")
class BloomFilterControllerTest {

    @Autowired private MockMvc mockMvc;
    @MockitoBean private BloomFilterService bloomFilterService;
    @MockitoBean private BloomFilterProperties properties;

    @Nested
    @DisplayName("POST /api/v1/bloom/{filterName}/add")
    class AddEndpointTests {

        @Test
        @DisplayName("should add item and return 200")
        void addItemSuccess() throws Exception {
            when(bloomFilterService.add("users", "user123")).thenReturn(true);

            mockMvc.perform(post("/api/v1/bloom/users/add")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"item\": \"user123\"}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.filterName").value("users"))
                    .andExpect(jsonPath("$.result").value(true))
                    .andExpect(jsonPath("$.message").value("Item added successfully"));
        }

        @Test
        @DisplayName("should return 400 for blank item")
        void addItemBlankValidation() throws Exception {
            mockMvc.perform(post("/api/v1/bloom/users/add")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"item\": \"\"}"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("should return 400 for missing item")
        void addItemMissing() throws Exception {
            mockMvc.perform(post("/api/v1/bloom/users/add")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{}"))
                    .andExpect(status().isBadRequest());
        }
    }

    @Nested
    @DisplayName("POST /api/v1/bloom/{filterName}/add/batch")
    class BatchAddEndpointTests {

        @Test
        @DisplayName("should batch add items and return results")
        void batchAddSuccess() throws Exception {
            when(bloomFilterService.multiAdd("users", List.of("a", "b", "c")))
                    .thenReturn(List.of(true, false, true));

            mockMvc.perform(post("/api/v1/bloom/users/add/batch")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"items\": [\"a\", \"b\", \"c\"]}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.results[0]").value(true))
                    .andExpect(jsonPath("$.results[1]").value(false))
                    .andExpect(jsonPath("$.results[2]").value(true))
                    .andExpect(jsonPath("$.itemsProcessed").value(3));
        }

        @Test
        @DisplayName("should return 400 for empty items list")
        void batchAddEmptyList() throws Exception {
            mockMvc.perform(post("/api/v1/bloom/users/add/batch")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"items\": []}"))
                    .andExpect(status().isBadRequest());
        }
    }

    @Nested
    @DisplayName("GET /api/v1/bloom/{filterName}/exists/{item}")
    class ExistsEndpointTests {

        @Test
        @DisplayName("should return 'might exist' when item found")
        void existsFound() throws Exception {
            when(bloomFilterService.mightContain("users", "user123")).thenReturn(true);

            mockMvc.perform(get("/api/v1/bloom/users/exists/user123"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.mightExist").value(true))
                    .andExpect(jsonPath("$.message").value(
                            "Item MIGHT exist in filter (possible false positive)"));
        }

        @Test
        @DisplayName("should return 'definitely not found' when item absent")
        void existsNotFound() throws Exception {
            when(bloomFilterService.mightContain("users", "unknown")).thenReturn(false);

            mockMvc.perform(get("/api/v1/bloom/users/exists/unknown"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.mightExist").value(false))
                    .andExpect(jsonPath("$.message").value(
                            "Item DEFINITELY does not exist in filter"));
        }
    }

    @Nested
    @DisplayName("POST /api/v1/bloom/{filterName}")
    class CreateFilterEndpointTests {

        @Test
        @DisplayName("should create filter with custom parameters")
        void createWithParams() throws Exception {
            when(properties.getDefaultCapacity()).thenReturn(1_000_000_000L);
            when(properties.getDefaultErrorRate()).thenReturn(0.001);
            when(properties.getDefaultShardCount()).thenReturn(16);

            mockMvc.perform(post("/api/v1/bloom/users")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"capacity\": 500000, \"errorRate\": 0.01, \"shardCount\": 4}"))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.filterName").value("users"))
                    .andExpect(jsonPath("$.message").value("Bloom filter created successfully"));

            verify(bloomFilterService).createFilter("users", 500000, 0.01, 4);
        }
    }

    @Nested
    @DisplayName("GET /api/v1/bloom/{filterName}/info")
    class InfoEndpointTests {

        @Test
        @DisplayName("should return aggregated filter info")
        void getInfoSuccess() throws Exception {
            BloomFilterResponse.AggregatedInfo info = BloomFilterResponse.AggregatedInfo.builder()
                    .filterName("users")
                    .shardCount(16)
                    .totalCapacity(1_000_000)
                    .totalMemoryUsageBytes(2_000_000)
                    .totalMemoryUsageHuman("1.9 MB")
                    .totalItemsInserted(500_000)
                    .averageFillRatio(0.5)
                    .build();

            when(bloomFilterService.getFilterInfo("users")).thenReturn(info);

            mockMvc.perform(get("/api/v1/bloom/users/info"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.info.shardCount").value(16))
                    .andExpect(jsonPath("$.info.totalCapacity").value(1_000_000));
        }
    }

    @Nested
    @DisplayName("DELETE /api/v1/bloom/{filterName}")
    class DeleteEndpointTests {

        @Test
        @DisplayName("should delete filter and return success")
        void deleteSuccess() throws Exception {
            mockMvc.perform(delete("/api/v1/bloom/users"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.message").value("Bloom filter deleted successfully"));

            verify(bloomFilterService).deleteFilter("users");
        }
    }
}
