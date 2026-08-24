package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName: BatchTokenDistributedLimiter
 * @Description: 基于中心 token bucket 的批量取号限流，单个实例本地缓存一批 token 降低中心调用次数
 * @Author: LittleWool
 * @Date: 2026/8/24 23:10
 * @Version: 1.0
 **/
public class BatchTokenDistributedLimiter implements Limiter {

    private final DistributedLimitStore store;

    private final String key;

    private final int permitsPerSecond;

    private final int capacity;

    private final int batchSize;

    private final AtomicInteger localTokens = new AtomicInteger();

    public BatchTokenDistributedLimiter(DistributedLimitStore store, String key, int permitsPerSecond, int batchSize) {
        this(store, key, permitsPerSecond, permitsPerSecond, batchSize);
    }

    public BatchTokenDistributedLimiter(DistributedLimitStore store, String key, int permitsPerSecond, int capacity,
        int batchSize) {
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
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.store = store;
        this.key = key;
        this.permitsPerSecond = permitsPerSecond;
        this.capacity = capacity;
        this.batchSize = batchSize;
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
            int acquired = store.tryAcquireTokens(key, permitsPerSecond, capacity, batchSize);
            if (acquired <= 0) {
                return false;
            }
            localTokens.addAndGet(acquired);
            return consumeLocalToken();
        }
    }

    @Override
    public void release(int permits) {
        // 本地 token 按批量取号消费，不需要释放令牌
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
}
