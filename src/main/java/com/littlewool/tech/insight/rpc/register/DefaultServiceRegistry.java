package com.littlewool.tech.insight.rpc.register;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: DefaultServiceRegistry
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 10:21
 * @Version: 1.0
 **/

@Slf4j
public class DefaultServiceRegistry implements ServieRegistry {
    ServieRegistry delegate;
    Map<String, List<ServiceMetadata>> cache = new HashMap<>();

    @Override
    public void init(RegistryConfig registryConfig) throws Exception {
        this.delegate = createServiceRegister(registryConfig);
        this.delegate.init(registryConfig);
    }

    @Override
    public void registerService(ServiceMetadata metadata) {
        log.info("向{}注册了一个Service{}", delegate.getClass(), metadata.getServiceName());
        delegate.registerService(metadata);
    }

    @Override
    public List<ServiceMetadata> fetchServiceList(String serviceName) {

        try {
            List<ServiceMetadata> serviceMetadata = delegate.fetchServiceList(serviceName);
            cache.put(serviceName, serviceMetadata);
            return serviceMetadata;
        } catch (Exception e) {
            log.error("{}注册中心出现{}异常",delegate.getClass().getSimpleName(),serviceName,e);
            return cache.getOrDefault(serviceName,new ArrayList<>());
        }
    }

    public static ServieRegistry createServiceRegister(RegistryConfig registryConfig) {
        if ("zookeeper".equals(registryConfig.getRegisterType())) {
            return new ZookeeperServiceRegistry();
        }
        if ("Redis".equals(registryConfig.getRegisterType())) {
            return new RedisServiceRegistry();
        }
        throw new IllegalArgumentException(registryConfig.getRegisterType() + "没有实现");
    }
}
