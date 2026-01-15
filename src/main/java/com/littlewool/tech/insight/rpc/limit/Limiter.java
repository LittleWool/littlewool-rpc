package com.littlewool.tech.insight.rpc.limit;

/**
 * @ClassName: Limiter
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 9:35
 * @Version: 1.0
 **/

public interface Limiter {

    boolean tryAcquire();

    void release();
}
