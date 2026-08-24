package com.littlewool.tech.insight.rpc.limit;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName: BucketLimiter
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 10:31
 * @Version: 1.0
 **/

/***
 * 现在实现并不平滑,假设允许1s进入1w请求，在0.95-1.05的时间区间，就可以进入2w,压力巨大。
 * 可以通过调短定时任务间隔，增加任务频率，限流越精准。但对定时任务压力来说巨大
 * 所以改为事件驱动的LeakyBucketLimiter
 * */

@Deprecated
public class BucketLimiter implements Limiter {

    private final AtomicInteger tokens;
    private final ScheduledFuture<?> scheduledFuture;

    //这是个轻量级的任务，故可以所有对象共用一个，设置为守护线程,不用专门取消
    private static final EventLoopGroup REFILL_EVENT_LOOP = new DefaultEventLoop(r -> {
        //设置成守护线程，这样子主线程结束时候就可以自动结束
        Thread thread = new Thread("regill_event_loop");
        thread.setDaemon(true);
        return thread;
    });


    public BucketLimiter(int permitsPerSecond) {
        this.tokens = new AtomicInteger(permitsPerSecond);
        this.scheduledFuture = this.REFILL_EVENT_LOOP.scheduleAtFixedRate(() -> tokens.set(permitsPerSecond), 1, 1,
                TimeUnit.SECONDS);
    }

    @Override
    public boolean tryAcquire() {
        while (true) {
            int currrentTokens = tokens.get();
            if (currrentTokens <= 0) {
                return false;
            }
            if (tokens.compareAndSet(currrentTokens, currrentTokens - 1)) {
                return true;
            }
        }
    }

    @Override
    public void release(int permits) {
        //定时刷新 不需要释放
    }

    public void destroy() {
        scheduledFuture.cancel(false);

    }
}
