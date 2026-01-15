package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ClassName: RateLimiter
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 10:46
 * @Version: 1.0
 **/

public class RateLimiter implements Limiter{

    private final AtomicLong nextNs;
    private final long intervalNs;
    private static final int MAX_TRY_ACQUIRE=512;
    private static final long MAX_QUEUE_NS=TimeUnit.MILLISECONDS.toNanos(500);

    public RateLimiter(int permitsPerSecond) {
        this.nextNs = new AtomicLong(0l);
        this.intervalNs = TimeUnit.SECONDS.toNanos(1)/permitsPerSecond;
    }

    @Override
    public boolean tryAcquire() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < MAX_TRY_ACQUIRE; i++) {
            long pre = nextNs.get();
            if(start+MAX_QUEUE_NS<pre){
                return false;
            }
            if(nextNs.compareAndSet(pre,Math.max(pre,start)+intervalNs)){
                return true;
            }
        }
        return false;
    }

    @Override
    public void release(int permits) {

    }
}
