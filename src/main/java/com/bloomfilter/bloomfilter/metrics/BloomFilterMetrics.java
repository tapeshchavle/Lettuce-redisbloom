package com.bloomfilter.bloomfilter.metrics;

import io.micrometer.core.instrument.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Micrometer metrics instrumentation for Bloom Filter operations.
 *
 * <h3>Exported Metrics</h3>
 * <table>
 *   <tr><th>Name</th><th>Type</th><th>Tags</th><th>Description</th></tr>
 *   <tr><td>bloom.filter.add.total</td><td>Counter</td><td>filter</td><td>Total add operations</td></tr>
 *   <tr><td>bloom.filter.exists.total</td><td>Counter</td><td>filter, result</td><td>Total exists checks (found/not_found)</td></tr>
 *   <tr><td>bloom.filter.operation.duration</td><td>Timer</td><td>filter, operation</td><td>Latency histogram</td></tr>
 *   <tr><td>bloom.filter.batch.size</td><td>DistributionSummary</td><td>filter, operation</td><td>Batch size distribution</td></tr>
 *   <tr><td>bloom.filter.error.total</td><td>Counter</td><td>filter, operation</td><td>Error count</td></tr>
 * </table>
 *
 * <p>These metrics are exported via the Prometheus endpoint at {@code /actuator/prometheus}.</p>
 */
@Slf4j
@Component
public class BloomFilterMetrics {

    private final MeterRegistry meterRegistry;

    public BloomFilterMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Records a successful add operation with latency.
     */
    public void recordAdd(String filterName, long durationNanos) {
        Counter.builder("bloom.filter.add.total")
                .description("Total bloom filter add operations")
                .tag("filter", filterName)
                .register(meterRegistry)
                .increment();

        Timer.builder("bloom.filter.operation.duration")
                .description("Bloom filter operation latency")
                .tag("filter", filterName)
                .tag("operation", "add")
                .register(meterRegistry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records a successful exists check with result and latency.
     */
    public void recordExists(String filterName, boolean found, long durationNanos) {
        Counter.builder("bloom.filter.exists.total")
                .description("Total bloom filter exists checks")
                .tag("filter", filterName)
                .tag("result", found ? "found" : "not_found")
                .register(meterRegistry)
                .increment();

        Timer.builder("bloom.filter.operation.duration")
                .description("Bloom filter operation latency")
                .tag("filter", filterName)
                .tag("operation", "exists")
                .register(meterRegistry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records a batch operation with item count and latency.
     */
    public void recordBatchOperation(String filterName, String operation, int itemCount, long durationNanos) {
        Timer.builder("bloom.filter.operation.duration")
                .description("Bloom filter operation latency")
                .tag("filter", filterName)
                .tag("operation", "batch_" + operation)
                .register(meterRegistry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records the size of a batch operation for distribution analysis.
     */
    public void recordBatchSize(String filterName, String operation, int size) {
        DistributionSummary.builder("bloom.filter.batch.size")
                .description("Bloom filter batch operation sizes")
                .tag("filter", filterName)
                .tag("operation", operation)
                .register(meterRegistry)
                .record(size);
    }

    /**
     * Records an error during a bloom filter operation.
     */
    public void recordError(String filterName, String operation) {
        Counter.builder("bloom.filter.error.total")
                .description("Total bloom filter operation errors")
                .tag("filter", filterName)
                .tag("operation", operation)
                .register(meterRegistry)
                .increment();
    }
}
