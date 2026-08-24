package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ClassName: FixedWindowLimiter
 * @Description: 固定窗口限流
 * @Author: LittleWool
 * @Date: 2026/8/24 21:30
 * @Version: 1.0
 **/
public class FixedWindowLimiter implements Limiter {

    private final int maxPermits;

    private final long windowNs;

    private final AtomicLong windowStartNs;

    private final AtomicInteger requestCount = new AtomicInteger();

    public FixedWindowLimiter(int permitsPerSecond) {
        this(permitsPerSecond, 1, TimeUnit.SECONDS);
    }

    public FixedWindowLimiter(int maxPermits, long window, TimeUnit unit) {
        if (maxPermits <= 0) {
            throw new IllegalArgumentException("maxPermits must be positive");
        }
        if (window <= 0) {
            throw new IllegalArgumentException("window must be positive");
        }
        this.maxPermits = maxPermits;
        this.windowNs = unit.toNanos(window);
        this.windowStartNs = new AtomicLong(System.nanoTime());
    }

    @Override
    public boolean tryAcquire() {
        long now = System.nanoTime();
        slideWindowIfNecessary(now);

        while (true) {
            int current = requestCount.get();
            if (current >= maxPermits) {
                return false;
            }
            if (requestCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private void slideWindowIfNecessary(long now) {
        while (true) {
            long windowStart = windowStartNs.get();
            if (now - windowStart < windowNs) {
                return;
            }
            if (windowStartNs.compareAndSet(windowStart, now)) {
                requestCount.set(0);
                return;
            }
        }
    }

    @Override
    public void release(int permits) {
        //固定窗口按请求次数限流，不需要释放令牌
    }
}
