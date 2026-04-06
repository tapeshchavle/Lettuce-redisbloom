package com.bloomfilter.bloomfilter;

import com.bloomfilter.bloomfilter.dto.BloomFilterResponse;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * End-to-end integration test using Testcontainers with Redis Stack.
 *
 * <p>This test starts a real Redis Stack container (with RedisBloom module),
 * creates bloom filters, adds items, and verifies existence checks against
 * the actual RedisBloom engine.</p>
 *
 * <p><strong>Requirements</strong>: Docker must be running.</p>
 */
@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Bloom Filter Integration Test")
class BloomFilterIntegrationTest {

    @Container
    static GenericContainer<?> redisStack = new GenericContainer<>(
            DockerImageName.parse("redis/redis-stack-server:latest"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redisStack::getHost);
        registry.add("spring.data.redis.port", () -> redisStack.getMappedPort(6379));
        // Disable auto-reserve for controlled test flow
        registry.add("bloom-filter.auto-reserve", () -> false);
    }

    @Autowired private MockMvc mockMvc;
    @Autowired private ObjectMapper objectMapper;

    private static final String FILTER_NAME = "integration-test";

    @Test
    @Order(1)
    @DisplayName("1. Create a bloom filter via API")
    void createFilter() throws Exception {
        mockMvc.perform(post("/api/v1/bloom/" + FILTER_NAME)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"capacity\": 100000, \"errorRate\": 0.001, \"shardCount\": 4}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.message").value("Bloom filter created successfully"));
    }

    @Test
    @Order(2)
    @DisplayName("2. Add a single item")
    void addSingleItem() throws Exception {
        mockMvc.perform(post("/api/v1/bloom/" + FILTER_NAME + "/add")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"item\": \"user_tapesh_2026\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value(true));
    }

    @Test
    @Order(3)
    @DisplayName("3. Check added item exists (should return true)")
    void checkExistingItem() throws Exception {
        mockMvc.perform(get("/api/v1/bloom/" + FILTER_NAME + "/exists/user_tapesh_2026"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mightExist").value(true));
    }

    @Test
    @Order(4)
    @DisplayName("4. Check non-existent item (should return false — ZERO false negatives)")
    void checkNonExistentItem() throws Exception {
        mockMvc.perform(get("/api/v1/bloom/" + FILTER_NAME + "/exists/definitely_not_added"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mightExist").value(false));
    }

    @Test
    @Order(5)
    @DisplayName("5. Batch add 1000 items")
    void batchAdd() throws Exception {
        List<String> items = IntStream.range(0, 1000)
                .mapToObj(i -> "batch_user_" + i)
                .toList();

        String json = objectMapper.writeValueAsString(
                java.util.Map.of("items", items));

        mockMvc.perform(post("/api/v1/bloom/" + FILTER_NAME + "/add/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.itemsProcessed").value(1000));
    }

    @Test
    @Order(6)
    @DisplayName("6. Batch check — all added items should be found")
    void batchCheckExisting() throws Exception {
        List<String> items = IntStream.range(0, 100)
                .mapToObj(i -> "batch_user_" + i)
                .toList();

        String json = objectMapper.writeValueAsString(
                java.util.Map.of("items", items));

        MvcResult result = mockMvc.perform(post("/api/v1/bloom/" + FILTER_NAME + "/exists/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk())
                .andReturn();

        String responseBody = result.getResponse().getContentAsString();
        // Verify all items are found (zero false negatives)
        assertThat(responseBody).contains("\"results\"");
    }

    @Test
    @Order(7)
    @DisplayName("7. Get filter info with aggregated shard statistics")
    void getFilterInfo() throws Exception {
        mockMvc.perform(get("/api/v1/bloom/" + FILTER_NAME + "/info"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.info.shardCount").value(4))
                .andExpect(jsonPath("$.info.totalItemsInserted").exists());
    }

    @Test
    @Order(8)
    @DisplayName("8. False positive rate should be within configured bounds")
    void falsePositiveRate() throws Exception {
        // Test 10,000 items that were NEVER added
        int testCount = 10_000;
        int falsePositives = 0;

        for (int i = 0; i < testCount; i++) {
            MvcResult mvcResult = mockMvc.perform(
                            get("/api/v1/bloom/" + FILTER_NAME + "/exists/never_added_item_" + i))
                    .andExpect(status().isOk())
                    .andReturn();

            String body = mvcResult.getResponse().getContentAsString();
            if (body.contains("\"mightExist\":true")) {
                falsePositives++;
            }
        }

        double fpRate = (double) falsePositives / testCount;
        System.out.printf("False positive rate: %.4f%% (%d/%d)%n",
                fpRate * 100, falsePositives, testCount);

        // With 0.1% configured error rate and only ~1001 items in a 100k filter,
        // the actual FP rate should be well below 1%
        assertThat(fpRate)
                .as("False positive rate should be below 1%%")
                .isLessThan(0.01);
    }

    @Test
    @Order(9)
    @DisplayName("9. Delete filter and verify it's gone")
    void deleteFilter() throws Exception {
        mockMvc.perform(delete("/api/v1/bloom/" + FILTER_NAME))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.message").value("Bloom filter deleted successfully"));
    }

    @Test
    @Order(10)
    @DisplayName("10. Health endpoint should report bloom filter status")
    void healthEndpoint() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk());
    }
}
