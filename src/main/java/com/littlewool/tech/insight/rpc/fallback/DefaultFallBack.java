package com.littlewool.tech.insight.rpc.fallback;

import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;
import lombok.extern.slf4j.Slf4j;

/**
 * @ClassName: DefaultFallBack
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 20:29
 * @Version: 1.0
 **/

@Slf4j
public class DefaultFallBack implements Fallback {

    private CacheFallback cacheFallback;

    private MockFallback mockFallback;

    public DefaultFallBack(CacheFallback cacheFallback, MockFallback mockFallback) {
        this.cacheFallback = cacheFallback;
        this.mockFallback = mockFallback;
    }

    @Override
    public void recordMetrics(RpcCallMetrics successMetrics) {
        cacheFallback.recordMetrics(successMetrics);
        mockFallback.recordMetrics(successMetrics);
    }

    @Override
    public Object fallback(RpcCallMetrics metrics) throws Exception {
        try {
            Object fallbackRes = cacheFallback.fallback(metrics);
            return fallbackRes;
        } catch (Exception e) {
            log.warn("缓存降级没有生效");
            return mockFallback.fallback(metrics);
        }
    }
}
