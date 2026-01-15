package com.littlewool.tech.insight.rpc.breaker;

import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;

/**
 * @ClassName: CirCuitBreaker
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 17:26
 * @Version: 1.0
 **/

public interface CirCuitBreaker {

    boolean allowRequest();

    void recordRpc(RpcCallMetrics metrics);

    enum State{
        OPEN,CLOSE,HALF_OPEN

        ;
    }
}

