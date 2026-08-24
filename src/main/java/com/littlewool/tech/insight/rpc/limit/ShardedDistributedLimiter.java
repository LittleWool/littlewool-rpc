package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName: ShardedDistributedLimiter
 * @Description: 分片分布式限流，用多个中心 key 分摊热点 key 的读写压力
 * @Author: LittleWool
 * @Date: 2026/8/24 23:10
 * @Version: 1.0
 **/
public class ShardedDistributedLimiter implements Limiter {

    private final DistributedLimitStore store;

    private final String key;

    private final int permitsPerShard;

    private final long window;

    private final TimeUnit unit;

    private final int shardCount;

    private final AtomicInteger shardCursor = new AtomicInteger();

    public ShardedDistributedLimiter(DistributedLimitStore store, String key, int permitsPerSecond) {
        this(store, key, permitsPerSecond, 1, TimeUnit.SECONDS, 16);
    }

    public ShardedDistributedLimiter(DistributedLimitStore store, String key, int maxPermits, long window,
        TimeUnit unit, int shardCount) {
        if (store == null) {
            throw new IllegalArgumentException("store must not be null");
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty");
        }
        if (maxPermits <= 0) {
            throw new IllegalArgumentException("maxPermits must be positive");
        }
        if (window <= 0) {
            throw new IllegalArgumentException("window must be positive");
        }
        if (unit == null) {
            throw new IllegalArgumentException("unit must not be null");
        }
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive");
        }
        this.store = store;
        this.key = key;
        this.permitsPerShard = ceilDiv(maxPermits, shardCount);
        this.window = window;
        this.unit = unit;
        this.shardCount = shardCount;
    }

    @Override
    public boolean tryAcquire() {
        return store.tryAcquire(nextShardKey(), permitsPerShard, window, unit);
    }

    @Override
    public void release(int permits) {
        // 固定窗口计数限流不需要释放令牌
    }

    private String nextShardKey() {
        int shard = Math.floorMod(shardCursor.getAndIncrement(), shardCount);
        return key + ":shard:" + shard;
    }

    private static int ceilDiv(int value, int divisor) {
        return (value + divisor - 1) / divisor;
    }
}
