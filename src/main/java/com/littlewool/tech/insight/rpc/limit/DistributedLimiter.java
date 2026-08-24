package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName: DistributedLimiter
 * @Description: 分布式限流，通过中心存储按全局 key 统一计数
 * @Author: LittleWool
 * @Date: 2026/8/24 21:45
 * @Version: 1.0
 **/
public class DistributedLimiter implements Limiter {

    private final DistributedLimitStore store;

    private final String key;

    private final int maxPermits;

    private final long window;

    private final TimeUnit unit;

    public DistributedLimiter(DistributedLimitStore store, String key, int permitsPerSecond) {
        this(store, key, permitsPerSecond, 1, TimeUnit.SECONDS);
    }

    public DistributedLimiter(DistributedLimitStore store, String key, int maxPermits, long window, TimeUnit unit) {
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
        this.store = store;
        this.key = key;
        this.maxPermits = maxPermits;
        this.window = window;
        this.unit = unit;
    }

    @Override
    public boolean tryAcquire() {
        return store.tryAcquire(key, maxPermits, window, unit);
    }

    @Override
    public void release(int permits) {
        //中心计数按窗口自然过期，不需要释放令牌
    }
}
