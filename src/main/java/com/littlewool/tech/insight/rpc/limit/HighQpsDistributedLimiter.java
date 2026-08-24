package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName: HighQpsDistributedLimiter
 * @Description: 高 QPS 分布式限流，本地 token 池批量取号并按分片 key 降低中心压力
 * @Author: LittleWool
 * @Date: 2026/8/24 21:45
 * @Version: 1.0
 **/
public class HighQpsDistributedLimiter implements Limiter {

    private final DistributedLimitStore store;

    private final String key;

    private final int shardCount;

    private final int permitsPerShard;

    private final int capacityPerShard;

    private final int minBatchSize;

    private final int maxBatchSize;

    private final AtomicInteger localTokens = new AtomicInteger();

    private final AtomicInteger batchSize;

    private final AtomicInteger shardCursor = new AtomicInteger();

    public HighQpsDistributedLimiter(DistributedLimitStore store, String key, int permitsPerSecond) {
        this(store, key, permitsPerSecond, permitsPerSecond, 16, 8, 256);
    }

    public HighQpsDistributedLimiter(DistributedLimitStore store, String key, int permitsPerSecond, int capacity,
        int shardCount, int minBatchSize, int maxBatchSize) {
        if (store == null) {
            throw new IllegalArgumentException("store must not be null");
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty");
        }
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive");
        }
        if (minBatchSize <= 0 || maxBatchSize < minBatchSize) {
            throw new IllegalArgumentException("batch size config invalid");
        }
        this.store = store;
        this.key = key;
        this.shardCount = shardCount;
        this.permitsPerShard = ceilDiv(permitsPerSecond, shardCount);
        this.capacityPerShard = ceilDiv(capacity, shardCount);
        this.minBatchSize = minBatchSize;
        this.maxBatchSize = maxBatchSize;
        this.batchSize = new AtomicInteger(minBatchSize);
    }

    @Override
    public boolean tryAcquire() {
        if (consumeLocalToken()) {
            return true;
        }
        synchronized (this) {
            if (consumeLocalToken()) {
                return true;
            }
            int requested = batchSize.get();
            int acquired = tryAcquireFromShards(requested);
            adjustBatchSize(requested, acquired);
            if (acquired <= 0) {
                return false;
            }
            localTokens.addAndGet(acquired);
            return consumeLocalToken();
        }
    }

    private int tryAcquireFromShards(int requested) {
        int acquired = 0;
        for (int i = 0; i < shardCount && acquired <= 0; i++) {
            acquired = store.tryAcquireTokens(nextShardKey(), permitsPerShard, capacityPerShard, requested);
        }
        return acquired;
    }

    private boolean consumeLocalToken() {
        while (true) {
            int current = localTokens.get();
            if (current <= 0) {
                return false;
            }
            if (localTokens.compareAndSet(current, current - 1)) {
                return true;
            }
        }
    }

    private String nextShardKey() {
        int shard = Math.floorMod(shardCursor.getAndIncrement(), shardCount);
        return key + ":shard:" + shard;
    }

    private void adjustBatchSize(int requested, int acquired) {
        if (acquired >= requested) {
            batchSize.updateAndGet(current -> Math.min(maxBatchSize, current << 1));
            return;
        }
        batchSize.updateAndGet(current -> Math.max(minBatchSize, current >> 1));
    }

    @Override
    public void release(int permits) {
        //本地 token 池按批量取号消费，不需要释放令牌
    }

    int currentBatchSize() {
        return batchSize.get();
    }

    private static int ceilDiv(int value, int divisor) {
        return (value + divisor - 1) / divisor;
    }
}
