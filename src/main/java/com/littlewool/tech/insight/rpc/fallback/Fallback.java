package com.littlewool.tech.insight.rpc.fallback;

import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;

/**
 * @ClassName: Fallback
 * @Description: 降级 经历了负载均衡 重试等还是不行 则使用降级  缓存降级 本地服务替换
 * @Author: LittleWool
 * @Date: 2026/1/15 20:25
 * @Version: 1.0
 **/

public interface Fallback {

    Object fallback(RpcCallMetrics metrics)throws Exception;

    default void recordMetrics(RpcCallMetrics successMetrics){

    }
}
