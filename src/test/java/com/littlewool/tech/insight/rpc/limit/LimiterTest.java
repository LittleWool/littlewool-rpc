package com.littlewool.tech.insight.rpc.limit;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LimiterTest {

    @Test
    public void fixedWindowRejectsAfterLimit() {
        Limiter limiter = new FixedWindowLimiter(2, 1, TimeUnit.SECONDS);

        assertTrue(limiter.tryAcquire());
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire());
    }

    @Test
    public void slidingWindowRejectsAfterLimit() {
        Limiter limiter = new SlidingWindowLimiter(2, 1, TimeUnit.SECONDS);

        assertTrue(limiter.tryAcquire());
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire());
    }

    @Test
    public void tokenBucketAllowsBurstUpToCapacity() {
        Limiter limiter = new TokenBucketLimiter(1, 2);

        assertTrue(limiter.tryAcquire());
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire());
    }

    @Test
    public void leakyBucketRejectsWhenQueueIsFull() {
        Limiter limiter = new LeakyBucketLimiter(1, 1);

        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire());
    }

    @Test
    public void distributedLimiterSharesGlobalWindowByKey() {
        DistributedLimitStore store = new InMemoryDistributedLimitStore();
        Limiter limiterA = new DistributedLimiter(store, "user:1", 2);
        Limiter limiterB = new DistributedLimiter(store, "user:1", 2);

        assertTrue(limiterA.tryAcquire());
        assertTrue(limiterB.tryAcquire());
        assertFalse(limiterA.tryAcquire());
    }

    @Test
    public void highQpsDistributedLimiterUsesLocalBatchTokens() {
        DistributedLimitStore store = new InMemoryDistributedLimitStore();
        Limiter limiter = new HighQpsDistributedLimiter(store, "hot:1", 64, 64, 4, 4, 16);

        assertTrue(limiter.tryAcquire());
        assertTrue(limiter.tryAcquire());
    }
}
