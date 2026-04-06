package com.bloomfilter.bloomfilter.exception;

/**
 * Thrown when the RedisBloom module is not loaded on the Redis server.
 *
 * <p>This typically means the Redis server is a standard Redis instance
 * without the RedisBloom module, or Redis Stack is not being used.</p>
 *
 * <p>Solution: Use Redis Stack (includes RedisBloom) or load the module manually:
 * <pre>
 * # Docker
 * docker run -p 6379:6379 redis/redis-stack-server:latest
 *
 * # Manual module load
 * redis-server --loadmodule /path/to/redisbloom.so
 * </pre>
 */
public class RedisBloomModuleException extends BloomFilterException {

    public RedisBloomModuleException(String message) {
        super(message);
    }

    public RedisBloomModuleException(String message, Throwable cause) {
        super(message, cause);
    }
}
