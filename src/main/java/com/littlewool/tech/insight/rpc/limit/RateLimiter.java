package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ClassName: RateLimiter
 * @Description: 速率限流 防止突然流量激增 consumer端流量都是自己发出的，可以本地快速失败，故采用速率限流
 * @Author: LittleWool
 * @Date: 2026/1/15 10:46
 * @Version: 1.0
 **/

/***
 * 无论多么精准的定时任务,都比不上事件驱动
 */
public class RateLimiter implements Limiter {

    private final AtomicLong nextNs;
    private final long intervalNs;
    private static final int MAX_TRY_ACQUIRE = 512;
    private static final long MAX_QUEUE_NS = TimeUnit.MILLISECONDS.toNanos(500);

    public RateLimiter(int permitsPerSecond) {
        this.nextNs = new AtomicLong(0l);
        //刷新令牌的间隔
        this.intervalNs = TimeUnit.SECONDS.toNanos(1) / permitsPerSecond;
    }

    @Override
    public boolean tryAcquire() {
        long start = System.nanoTime();
        for (int i = 0; i < MAX_TRY_ACQUIRE; i++) {
            long pre = nextNs.get();
            //若是只有两个发生竞争,但令牌刚好被替换了，不应当直接返回false，应当给一定时间进行重试
            if (pre - start > MAX_QUEUE_NS) {
                return false;
            }
            if (nextNs.compareAndSet(pre, Math.max(pre, start) + intervalNs)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void release(int permits) {
        //不需要释放令牌
    }
}
