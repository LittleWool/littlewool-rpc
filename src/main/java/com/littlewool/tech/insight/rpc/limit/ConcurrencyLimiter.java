package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.Semaphore;

/**
 * @ClassName: ConcurrencyLimiter
 * @Description: 并发限流 指的是服务器承载上限。一般是全局
 * @Author: LittleWool
 * @Date: 2026/1/15 10:30
 * @Version: 1.0
 **/

public class ConcurrencyLimiter implements Limiter {
    private final Semaphore semaphore;

    public ConcurrencyLimiter(int limitNum) {
        this.semaphore = new Semaphore(limitNum);
    }

    @Override
    public boolean tryAcquire() {
        return semaphore.tryAcquire();
    }

    @Override
    public void release(int permits) {
        semaphore.release(permits);
    }
}
