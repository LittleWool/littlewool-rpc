package com.littlewool.tech.insight.rpc.limit;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributedLimiterBenchmarkTest {

    private static final int ATTEMPTS = Integer.getInteger("limit.benchmark.attempts", 100_000);

    @Test
    public void benchmarkLimiterOptimizationStages() {
        benchmark("basic-fixed-window", newLimiterStore(),
            store -> new DistributedLimiter(store, "bench:basic", ATTEMPTS));
        benchmark("sharded-fixed-window", newLimiterStore(),
            store -> new ShardedDistributedLimiter(store, "bench:sharded", ATTEMPTS, 1, TimeUnit.SECONDS, 16));
        benchmark("batch-token-pool", newLimiterStore(),
            store -> new BatchTokenDistributedLimiter(store, "bench:batch", ATTEMPTS, ATTEMPTS, 128));
        benchmark("dynamic-step-sharded", newLimiterStore(),
            store -> new HighQpsDistributedLimiter(store, "bench:dynamic", ATTEMPTS, ATTEMPTS, 16, 8, 512));
    }

    private static CountingStore newLimiterStore() {
        return new CountingStore(new InMemoryDistributedLimitStore());
    }

    private static void benchmark(String name, CountingStore store, LimiterFactory factory) {
        Limiter limiter = factory.create(store);
        long startNs = System.nanoTime();
        int allowed = 0;
        int rejected = 0;
        for (int i = 0; i < ATTEMPTS; i++) {
            if (limiter.tryAcquire()) {
                allowed++;
            } else {
                rejected++;
            }
        }
        long costMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        System.out.println("limiter-benchmark name=" + name + " attempts=" + ATTEMPTS + " allowed=" + allowed
            + " rejected=" + rejected + " fixedCalls=" + store.fixedWindowCalls() + " tokenCalls="
            + store.tokenBucketCalls() + " centerCalls=" + store.centerCalls() + " maxCallsForOneKey="
            + store.maxCallsForOneKey() + " finalBatchSize=" + finalBatchSize(limiter) + " costMs=" + costMs);
    }

    private static String finalBatchSize(Limiter limiter) {
        if (limiter instanceof HighQpsDistributedLimiter) {
            return String.valueOf(((HighQpsDistributedLimiter)limiter).currentBatchSize());
        }
        return "-";
    }

    private interface LimiterFactory {

        Limiter create(DistributedLimitStore store);
    }

    private static class CountingStore implements DistributedLimitStore {

        private final DistributedLimitStore delegate;

        private final AtomicInteger fixedWindowCalls = new AtomicInteger();

        private final AtomicInteger tokenBucketCalls = new AtomicInteger();

        private final Map<String, AtomicInteger> callsByKey = new HashMap<>();

        private CountingStore(DistributedLimitStore delegate) {
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

        private int centerCalls() {
            return fixedWindowCalls.get() + tokenBucketCalls.get();
        }

        private int maxCallsForOneKey() {
            synchronized (callsByKey) {
                if (callsByKey.isEmpty()) {
                    return 0;
                }
                return Collections.max(callsByKey.values(), (left, right) -> Integer.compare(left.get(), right.get()))
                    .get();
            }
        }
    }
}
