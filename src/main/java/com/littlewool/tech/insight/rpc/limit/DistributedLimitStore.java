package com.littlewool.tech.insight.rpc.limit;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName: DistributedLimitStore
 * @Description: 分布式限流中心存储接口，可由 Redis/Lua 等实现替换
 * @Author: LittleWool
 * @Date: 2026/8/24 21:45
 * @Version: 1.0
 **/
public interface DistributedLimitStore {

    boolean tryAcquire(String key, int maxPermits, long window, TimeUnit unit);

    int tryAcquireTokens(String key, int permitsPerSecond, int capacity, int requestPermits);
}
