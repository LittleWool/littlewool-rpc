package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ClassName: LeakyBucketLimiter
 * @Description: 漏桶限流
 * @Author: LittleWool
 * @Date: 2026/8/24 21:30
 * @Version: 1.0
 **/
public class LeakyBucketLimiter implements Limiter {

    private final long leakIntervalNs;

    private final long maxQueuedNs;

    private final AtomicLong nextLeakNs = new AtomicLong();

    public LeakyBucketLimiter(int permitsPerSecond) {
        this(permitsPerSecond, permitsPerSecond);
    }

    public LeakyBucketLimiter(int permitsPerSecond, int bucketCapacity) {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        if (bucketCapacity <= 0) {
            throw new IllegalArgumentException("bucketCapacity must be positive");
        }
        this.leakIntervalNs = TimeUnit.SECONDS.toNanos(1) / permitsPerSecond;
        this.maxQueuedNs = leakIntervalNs * bucketCapacity;
    }

    @Override
    public boolean tryAcquire() {
        long now = System.nanoTime();
        while (true) {
            long nextLeak = nextLeakNs.get();
            long nextSlot = Math.max(nextLeak, now) + leakIntervalNs;
            if (nextSlot - now > maxQueuedNs) {
                return false;
            }
            if (nextLeakNs.compareAndSet(nextLeak, nextSlot)) {
                return true;
            }
        }
    }

    @Override
    public void release(int permits) {
        //漏桶按固定速率漏出，不需要释放令牌
    }
}
