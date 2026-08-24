package com.littlewool.tech.insight.rpc.limit;

/**
 * @ClassName: SimpleClusterLimiter
 * @Description: 简单集群限流，把总阈值按实例数平均分配到单机
 * @Author: LittleWool
 * @Date: 2026/8/24 21:45
 * @Version: 1.0
 **/
public class SimpleClusterLimiter implements Limiter {

    private final Limiter localLimiter;

    public SimpleClusterLimiter(int clusterPermitsPerSecond, int instanceCount) {
        this(new TokenBucketLimiter(ceilDiv(clusterPermitsPerSecond, instanceCount)));
    }

    public SimpleClusterLimiter(Limiter localLimiter) {
        if (localLimiter == null) {
            throw new IllegalArgumentException("localLimiter must not be null");
        }
        this.localLimiter = localLimiter;
    }

    @Override
    public boolean tryAcquire() {
        return localLimiter.tryAcquire();
    }

    @Override
    public void release(int permits) {
        localLimiter.release(permits);
    }

    private static int ceilDiv(int value, int divisor) {
        if (value <= 0) {
            throw new IllegalArgumentException("value must be positive");
        }
        if (divisor <= 0) {
            throw new IllegalArgumentException("divisor must be positive");
        }
        return (value + divisor - 1) / divisor;
    }
}
