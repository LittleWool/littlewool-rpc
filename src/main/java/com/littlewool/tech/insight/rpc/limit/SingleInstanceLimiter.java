package com.littlewool.tech.insight.rpc.limit;

/**
 * @ClassName: SingleInstanceLimiter
 * @Description: 单实例限流，每个实例本地独立计数
 * @Author: LittleWool
 * @Date: 2026/8/24 21:45
 * @Version: 1.0
 **/
public class SingleInstanceLimiter implements Limiter {

    private final Limiter delegate;

    public SingleInstanceLimiter(int permitsPerSecond) {
        this(new TokenBucketLimiter(permitsPerSecond));
    }

    public SingleInstanceLimiter(Limiter delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        this.delegate = delegate;
    }

    @Override
    public boolean tryAcquire() {
        return delegate.tryAcquire();
    }

    @Override
    public void release(int permits) {
        delegate.release(permits);
    }
}
