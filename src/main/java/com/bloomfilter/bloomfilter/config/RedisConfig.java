package com.bloomfilter.bloomfilter.config;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;

import java.time.Duration;

/**
 * Production-grade Redis (Lettuce) configuration.
 *
 * <p>Key features:
 * <ul>
 *   <li>Connection pooling via commons-pool2</li>
 *   <li>Auto-reconnect with exponential backoff</li>
 *   <li>Ping-before-activate to validate connections</li>
 *   <li>Configurable timeouts for connect/read/write</li>
 *   <li>Shared ClientResources for thread efficiency</li>
 * </ul>
 */
@Configuration
public class RedisConfig {

    @Value("${spring.data.redis.host:localhost}")
    private String host;

    @Value("${spring.data.redis.port:6379}")
    private int port;

    @Value("${spring.data.redis.password:}")
    private String password;

    @Value("${spring.data.redis.timeout:2s}")
    private Duration timeout;

    @Value("${spring.data.redis.connect-timeout:3s}")
    private Duration connectTimeout;

    @Value("${spring.data.redis.lettuce.pool.max-active:16}")
    private int maxActive;

    @Value("${spring.data.redis.lettuce.pool.max-idle:8}")
    private int maxIdle;

    @Value("${spring.data.redis.lettuce.pool.min-idle:4}")
    private int minIdle;

    @Value("${spring.data.redis.lettuce.pool.max-wait:2s}")
    private Duration maxWait;

    /**
     * Shared Lettuce client resources — manages Netty event loops and computation threads.
     * Singleton per application to avoid thread explosion under high concurrency.
     */
    @Bean(destroyMethod = "shutdown")
    public ClientResources clientResources() {
        return DefaultClientResources.builder()
                .ioThreadPoolSize(Runtime.getRuntime().availableProcessors())
                .computationThreadPoolSize(Runtime.getRuntime().availableProcessors())
                .build();
    }

    /**
     * Connection pool configuration tuned for high-throughput bloom filter operations.
     *
     * <p>Pool sizing rationale:
     * <ul>
     *   <li>max-active=16: Limits concurrent Redis connections to prevent saturation</li>
     *   <li>min-idle=4: Keeps warm connections ready for burst traffic</li>
     *   <li>max-wait=2s: Fails fast if pool exhausted (better than blocking indefinitely)</li>
     * </ul>
     */
    @Bean
    public GenericObjectPoolConfig<?> redisPoolConfig() {
        GenericObjectPoolConfig<?> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(maxActive);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);
        poolConfig.setMaxWait(maxWait);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
        return poolConfig;
    }

    /**
     * Lettuce client options with production-grade resilience settings.
     */
    @Bean
    public ClientOptions clientOptions() {
        return ClientOptions.builder()
                .autoReconnect(true)
                .pingBeforeActivateConnection(true)
                .timeoutOptions(TimeoutOptions.enabled(timeout))
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(connectTimeout)
                        .keepAlive(true)
                        .build())
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build();
    }

    /**
     * Lettuce connection factory with pooling and production client options.
     */
    @Bean
    public LettuceConnectionFactory redisConnectionFactory(
            ClientResources clientResources,
            GenericObjectPoolConfig<?> redisPoolConfig,
            ClientOptions clientOptions) {

        RedisStandaloneConfiguration serverConfig = new RedisStandaloneConfiguration(host, port);
        if (password != null && !password.isBlank()) {
            serverConfig.setPassword(password);
        }

        @SuppressWarnings("unchecked")
        LettucePoolingClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
                .poolConfig((GenericObjectPoolConfig) redisPoolConfig)
                .clientResources(clientResources)
                .clientOptions(clientOptions)
                .commandTimeout(timeout)
                .build();

        LettuceConnectionFactory factory = new LettuceConnectionFactory(serverConfig, clientConfig);
        factory.setShareNativeConnection(false); // Each operation gets its own connection from pool
        return factory;
    }
}
