package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName: TokenBucketLimiter
 * @Description: 令牌桶限流
 * @Author: LittleWool
 * @Date: 2026/8/24 21:30
 * @Version: 1.0
 **/
public class TokenBucketLimiter implements Limiter {

    private final int capacity;

    private final double refillTokensPerNs;

    private double tokens;

    private long lastRefillNs;

    public TokenBucketLimiter(int permitsPerSecond) {
        this(permitsPerSecond, permitsPerSecond);
    }

    public TokenBucketLimiter(int permitsPerSecond, int capacity) {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.tokens = capacity;
        this.refillTokensPerNs = (double) permitsPerSecond / TimeUnit.SECONDS.toNanos(1);
        this.lastRefillNs = System.nanoTime();
    }

    @Override
    public synchronized boolean tryAcquire() {
        refill();
        if (tokens < 1) {
            return false;
        }
        tokens -= 1;
        return true;
    }

    private void refill() {
        long now = System.nanoTime();
        if (now <= lastRefillNs) {
            return;
        }
        tokens = Math.min(capacity, tokens + (now - lastRefillNs) * refillTokensPerNs);
        lastRefillNs = now;
    }

    @Override
    public void release(int permits) {
        //令牌桶按时间补充令牌，不需要释放令牌
    }
}
