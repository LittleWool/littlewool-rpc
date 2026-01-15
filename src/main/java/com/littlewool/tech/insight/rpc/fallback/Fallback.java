package com.littlewool.tech.insight.rpc.fallback;

import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;

/**
 * @ClassName: Fallback
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 20:25
 * @Version: 1.0
 **/

public interface Fallback {

    Object fallback(RpcCallMetrics metrics)throws Exception;

    default void recordMetrics(RpcCallMetrics successMetrics){

    }
}
