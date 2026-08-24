package com.littlewool.tech.insight.rpc.limit;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DistributedLimiterOptimizationTest {

    @Test
    public void basicDistributedLimiterHitsStoreForEveryRequest() {
        CountingDistributedLimitStore store = new CountingDistributedLimitStore(new InMemoryDistributedLimitStore());
        Limiter limiter = new DistributedLimiter(store, "basic", 1_000);

        ExerciseResult result = exercise(limiter, 1_000);

        assertEquals(1_000, result.allowed);
        assertEquals(1_000, store.fixedWindowCalls());
        assertEquals(1_000, store.maxCallsForOneKey());
    }

    @Test
    public void shardedLimiterSplitsHotKeyAcrossCenterKeys() {
        CountingDistributedLimitStore store = new CountingDistributedLimitStore(new InMemoryDistributedLimitStore());
        Limiter limiter = new ShardedDistributedLimiter(store, "sharded", 1_024, 1, TimeUnit.SECONDS, 16);

        ExerciseResult result = exercise(limiter, 1_024);

        assertEquals(1_024, result.allowed);
        assertEquals(1_024, store.fixedWindowCalls());
        assertEquals(64, store.maxCallsForOneKey());
    }

    @Test
    public void batchTokenLimiterReducesCenterRoundTrips() {
        CountingDistributedLimitStore store = new CountingDistributedLimitStore(new InMemoryDistributedLimitStore());
        Limiter limiter = new BatchTokenDistributedLimiter(store, "batch", 1_024, 1_024, 64);

        ExerciseResult result = exercise(limiter, 1_024);

        assertEquals(1_024, result.allowed);
        assertEquals(16, store.tokenBucketCalls());
    }

    @Test
    public void dynamicStepGrowsBatchSizeWhenCenterHasEnoughTokens() {
        CountingDistributedLimitStore store = new CountingDistributedLimitStore(new InMemoryDistributedLimitStore());
        HighQpsDistributedLimiter limiter = new HighQpsDistributedLimiter(store, "dynamic", 1_024, 1_024, 1, 4, 128);

        ExerciseResult result = exercise(limiter, 512);

        assertEquals(512, result.allowed);
        assertEquals(128, limiter.currentBatchSize());
        assertTrue(store.tokenBucketCalls() < 16);
    }

    @Test
    public void shardedRedisStoreRoutesShardKeysEvenly() {
        RecordingDistributedLimitStore nodeA = new RecordingDistributedLimitStore();
        RecordingDistributedLimitStore nodeB = new RecordingDistributedLimitStore();
        DistributedLimitStore store = new ShardedRedisDistributedLimitStore(nodeA, nodeB);

        for (int i = 0; i < 8; i++) {
            store.tryAcquire("rpc:limit:shard:" + i, 100, 1, TimeUnit.SECONDS);
        }

        assertEquals(4, nodeA.fixedWindowCalls());
        assertEquals(4, nodeB.fixedWindowCalls());
    }

    private static ExerciseResult exercise(Limiter limiter, int attempts) {
        int allowed = 0;
        int rejected = 0;
        for (int i = 0; i < attempts; i++) {
            if (limiter.tryAcquire()) {
                allowed++;
            } else {
                rejected++;
            }
        }
        return new ExerciseResult(allowed, rejected);
    }

    private static class ExerciseResult {

        private final int allowed;

        private final int rejected;

        private ExerciseResult(int allowed, int rejected) {
            this.allowed = allowed;
            this.rejected = rejected;
        }
    }

    private static class CountingDistributedLimitStore implements DistributedLimitStore {

        private final DistributedLimitStore delegate;

        private final AtomicInteger fixedWindowCalls = new AtomicInteger();

        private final AtomicInteger tokenBucketCalls = new AtomicInteger();

        private final Map<String, AtomicInteger> callsByKey = new HashMap<>();

        private CountingDistributedLimitStore(DistributedLimitStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean tryAcquire(String key, int maxPermits, long window, TimeUnit unit) {
            fixedWindowCalls.incrementAndGet();
            recordKey(key);
            return delegate.tryAcquire(key, maxPermits, window, unit);
        }

        @Override
        public int tryAcquireTokens(String key, int permitsPerSecond, int capacity, int requestPermits) {
            tokenBucketCalls.incrementAndGet();
            recordKey(key);
            return delegate.tryAcquireTokens(key, permitsPerSecond, capacity, requestPermits);
        }

        private void recordKey(String key) {
            synchronized (callsByKey) {
                callsByKey.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
            }
        }

        private int fixedWindowCalls() {
            return fixedWindowCalls.get();
        }

        private int tokenBucketCalls() {
            return tokenBucketCalls.get();
        }

        private int maxCallsForOneKey() {
            synchronized (callsByKey) {
                return Collections.max(callsByKey.values(), (left, right) -> Integer.compare(left.get(), right.get()))
                    .get();
            }
        }
    }

    private static class RecordingDistributedLimitStore implements DistributedLimitStore {

        private final AtomicInteger fixedWindowCalls = new AtomicInteger();

        @Override
        public boolean tryAcquire(String key, int maxPermits, long window, TimeUnit unit) {
            fixedWindowCalls.incrementAndGet();
            return true;
        }

        @Override
        public int tryAcquireTokens(String key, int permitsPerSecond, int capacity, int requestPermits) {
            return requestPermits;
        }

        private int fixedWindowCalls() {
            return fixedWindowCalls.get();
        }
    }
}
