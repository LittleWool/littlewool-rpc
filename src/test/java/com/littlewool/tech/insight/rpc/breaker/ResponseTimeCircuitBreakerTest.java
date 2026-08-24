package com.littlewool.tech.insight.rpc.breaker;

import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResponseTimeCircuitBreakerTest {

    @Test
    public void opensWhenSlowRatioExceedsThreshold() {
        ResponseTimeCircuitBreaker breaker = new ResponseTimeCircuitBreaker(0.5, 1);

        for (int i = 0; i < 5; i++) {
            breaker.recordRpc(failedMetrics());
        }

        assertFalse(breaker.allowRequest());
    }

    @Test
    public void staysClosedForHealthyCalls() {
        ResponseTimeCircuitBreaker breaker = new ResponseTimeCircuitBreaker(0.5, 1000);

        for (int i = 0; i < 5; i++) {
            breaker.recordRpc(successMetrics());
        }

        assertTrue(breaker.allowRequest());
    }

    private RpcCallMetrics failedMetrics() {
        RpcCallMetrics metrics = RpcCallMetrics.createRpcMetrics(null, new Object[0], null);
        metrics.errorComplete(new RuntimeException("boom"));
        return metrics;
    }

    private RpcCallMetrics successMetrics() {
        RpcCallMetrics metrics = RpcCallMetrics.createRpcMetrics(null, new Object[0], null);
        metrics.setComplete(true);
        metrics.setDuration(1);
        return metrics;
    }
}
