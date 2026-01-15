package com.littlewool.tech.insight.rpc.breaker;

import com.littlewool.tech.insight.rpc.consumer.ConsumerProperties;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: CircuitBreakerManager
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 17:31
 * @Version: 1.0
 **/

public class CircuitBreakerManager {

    private final ConsumerProperties consumerProperties;

    public CircuitBreakerManager(ConsumerProperties consumerProperties) {
        this.consumerProperties = consumerProperties;
    }

    private final Map<ServiceMetadata, CirCuitBreaker> cirCuitBreakerMap = new ConcurrentHashMap<>();

    public CirCuitBreaker createOrGetBreaker(ServiceMetadata metadata) {
        return cirCuitBreakerMap.computeIfAbsent(metadata, m -> createBreaker(m));
    }

    private CirCuitBreaker createBreaker(ServiceMetadata metadata) {
        return new ResponseTimeCircuitBreaker(consumerProperties.getSlowRequestBreakingRatio(),
                consumerProperties.getSlowRequestMs());
    }
}
