package com.littlewool.tech.insight.rpc.register;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @ClassName: RedisServiceRegistry
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 10:01
 * @Version: 1.0
 **/

@Slf4j
public class RedisServiceRegistry implements ServieRegistry {
    @Override
    public void init(RegistryConfig registryConfig) throws Exception {
        log.info("Redis 注册中心还未实现");
    }

    @Override
    public void registerService(ServiceMetadata metadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ServiceMetadata> fetchServiceList(String serviceName) {
        throw new UnsupportedOperationException();
    }
}
