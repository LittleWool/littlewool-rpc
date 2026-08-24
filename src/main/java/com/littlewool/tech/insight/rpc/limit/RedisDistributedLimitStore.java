package com.littlewool.tech.insight.rpc.limit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: RedisDistributedLimitStore
 * @Description: 基于 Redis Lua 的分布式限流中心存储
 * @Author: LittleWool
 * @Date: 2026/8/24 22:15
 * @Version: 1.0
 **/
public class RedisDistributedLimitStore implements DistributedLimitStore, Closeable {

    private static final String FIXED_WINDOW_SCRIPT =
        "local key = KEYS[1]\n"
            + "local maxPermits = tonumber(ARGV[1])\n"
            + "local windowMs = tonumber(ARGV[2])\n"
            + "local nowMs = tonumber(ARGV[3])\n"
            + "local state = redis.call('HMGET', key, 'start', 'count')\n"
            + "local start = tonumber(state[1]) or nowMs\n"
            + "local count = tonumber(state[2]) or 0\n"
            + "if nowMs - start >= windowMs then\n"
            + "  start = nowMs\n"
            + "  count = 0\n"
            + "end\n"
            + "if count >= maxPermits then\n"
            + "  redis.call('HSET', key, 'start', start, 'count', count)\n"
            + "  redis.call('PEXPIRE', key, windowMs * 2)\n"
            + "  return 0\n"
            + "end\n"
            + "count = count + 1\n"
            + "redis.call('HSET', key, 'start', start, 'count', count)\n"
            + "redis.call('PEXPIRE', key, windowMs * 2)\n"
            + "return 1";

    private static final String TOKEN_BUCKET_SCRIPT =
        "local key = KEYS[1]\n"
            + "local rate = tonumber(ARGV[1])\n"
            + "local capacity = tonumber(ARGV[2])\n"
            + "local requested = tonumber(ARGV[3])\n"
            + "local nowMs = tonumber(ARGV[4])\n"
            + "local state = redis.call('HMGET', key, 'tokens', 'updated')\n"
            + "local tokens = tonumber(state[1])\n"
            + "local updated = tonumber(state[2])\n"
            + "if tokens == nil then\n"
            + "  tokens = capacity\n"
            + "  updated = nowMs\n"
            + "end\n"
            + "if nowMs > updated then\n"
            + "  tokens = math.min(capacity, tokens + (nowMs - updated) * rate / 1000)\n"
            + "  updated = nowMs\n"
            + "end\n"
            + "local acquired = math.min(requested, math.floor(tokens))\n"
            + "tokens = tokens - acquired\n"
            + "redis.call('HSET', key, 'tokens', tokens, 'updated', updated)\n"
            + "redis.call('PEXPIRE', key, math.ceil(capacity * 1000 / rate) * 2)\n"
            + "return acquired";

    private final JedisPool jedisPool;

    public RedisDistributedLimitStore(String host, int port) {
        this(new JedisPool(host, port));
    }

    public RedisDistributedLimitStore(JedisPool jedisPool) {
        if (jedisPool == null) {
            throw new IllegalArgumentException("jedisPool must not be null");
        }
        this.jedisPool = jedisPool;
    }

    @Override
    public boolean tryAcquire(String key, int maxPermits, long window, TimeUnit unit) {
        if (maxPermits <= 0) {
            throw new IllegalArgumentException("maxPermits must be positive");
        }
        if (window <= 0) {
            throw new IllegalArgumentException("window must be positive");
        }
        long windowMs = Math.max(1L, unit.toMillis(window));
        try (Jedis jedis = jedisPool.getResource()) {
            Object result = jedis.eval(FIXED_WINDOW_SCRIPT, Collections.singletonList(key),
                Arrays.asList(String.valueOf(maxPermits), String.valueOf(windowMs),
                    String.valueOf(System.currentTimeMillis())));
            return toLong(result) == 1L;
        }
    }

    @Override
    public int tryAcquireTokens(String key, int permitsPerSecond, int capacity, int requestPermits) {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        if (requestPermits <= 0) {
            throw new IllegalArgumentException("requestPermits must be positive");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            Object result = jedis.eval(TOKEN_BUCKET_SCRIPT, Collections.singletonList(key),
                Arrays.asList(String.valueOf(permitsPerSecond), String.valueOf(capacity),
                    String.valueOf(requestPermits), String.valueOf(System.currentTimeMillis())));
            return (int)toLong(result);
        }
    }

    @Override
    public void close() {
        jedisPool.close();
    }

    private static long toLong(Object result) {
        if (result instanceof Number) {
            return ((Number)result).longValue();
        }
        return Long.parseLong(String.valueOf(result));
    }
}
