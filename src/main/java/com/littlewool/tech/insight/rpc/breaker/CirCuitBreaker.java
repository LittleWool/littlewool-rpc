package com.littlewool.tech.insight.rpc.breaker;

import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;

/**
 * @ClassName: CirCuitBreaker
 * @Description:
 * 在无法提供良好服务的情况下 触发  记录每一个provider的状态
 * 记录每个provider的异常率 超时率 等
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

