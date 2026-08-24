package com.littlewool.tech.insight.rpc.limit;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: SlidingWindowLimiter
 * @Description: 精确滑动窗口限流
 * @Author: LittleWool
 * @Date: 2026/8/24 21:30
 * @Version: 1.0
 **/
public class SlidingWindowLimiter implements Limiter {

    private final int maxPermits;

    private final long windowNs;

    private final Deque<Long> requestTimes = new ArrayDeque<>();

    public SlidingWindowLimiter(int permitsPerSecond) {
        this(permitsPerSecond, 1, TimeUnit.SECONDS);
    }

    public SlidingWindowLimiter(int maxPermits, long window, TimeUnit unit) {
        if (maxPermits <= 0) {
            throw new IllegalArgumentException("maxPermits must be positive");
        }
        if (window <= 0) {
            throw new IllegalArgumentException("window must be positive");
        }
        this.maxPermits = maxPermits;
        this.windowNs = unit.toNanos(window);
    }

    @Override
    public synchronized boolean tryAcquire() {
        long now = System.nanoTime();
        long expiredBefore = now - windowNs;
        while (!requestTimes.isEmpty() && requestTimes.peekFirst() <= expiredBefore) {
            requestTimes.pollFirst();
        }
        if (requestTimes.size() >= maxPermits) {
            return false;
        }
        requestTimes.addLast(now);
        return true;
    }

    @Override
    public void release(int permits) {
        //滑动窗口按请求记录限流，不需要释放令牌
    }
}
