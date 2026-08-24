package com.littlewool.tech.insight.rpc.limit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: InMemoryDistributedLimitStore
 * @Description: 本地内存版分布式限流存储，便于单机验证；生产环境可替换为 Redis/Lua 实现
 * @Author: LittleWool
 * @Date: 2026/8/24 21:45
 * @Version: 1.0
 **/
public class InMemoryDistributedLimitStore implements DistributedLimitStore {

    private final Map<String, FixedWindowState> fixedWindowStates = new ConcurrentHashMap<>();

    private final Map<String, TokenBucketState> tokenBucketStates = new ConcurrentHashMap<>();

    @Override
    public boolean tryAcquire(String key, int maxPermits, long window, TimeUnit unit) {
        if (maxPermits <= 0) {
            throw new IllegalArgumentException("maxPermits must be positive");
        }
        if (window <= 0) {
            throw new IllegalArgumentException("window must be positive");
        }
        long windowNs = unit.toNanos(window);
        return fixedWindowStates.computeIfAbsent(key, k -> new FixedWindowState()).tryAcquire(maxPermits, windowNs);
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
        return tokenBucketStates.computeIfAbsent(key, k -> new TokenBucketState())
            .tryAcquire(permitsPerSecond, capacity, requestPermits);
    }

    private static class FixedWindowState {

        private long windowStartNs = System.nanoTime();

        private int requestCount;

        synchronized boolean tryAcquire(int maxPermits, long windowNs) {
            long now = System.nanoTime();
            if (now - windowStartNs >= windowNs) {
                windowStartNs = now;
                requestCount = 0;
            }
            if (requestCount >= maxPermits) {
                return false;
            }
            requestCount++;
            return true;
        }
    }

    private static class TokenBucketState {

        private int capacity;

        private double refillTokensPerNs;

        private double tokens;

        private long lastRefillNs = System.nanoTime();

        synchronized int tryAcquire(int permitsPerSecond, int newCapacity, int requestPermits) {
            if (refillTokensPerNs == 0) {
                capacity = newCapacity;
                refillTokensPerNs = (double) permitsPerSecond / TimeUnit.SECONDS.toNanos(1);
                tokens = capacity;
            } else if (capacity != newCapacity) {
                capacity = newCapacity;
                tokens = Math.min(tokens, capacity);
            }
            refill();
            int acquired = Math.min(requestPermits, (int) tokens);
            tokens -= acquired;
            return acquired;
        }

        private void refill() {
            long now = System.nanoTime();
            if (now <= lastRefillNs) {
                return;
            }
            tokens = Math.min(capacity, tokens + (now - lastRefillNs) * refillTokensPerNs);
            lastRefillNs = now;
        }
    }
}
