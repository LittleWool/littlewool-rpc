package com.littlewool.tech.insight.rpc.register;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: DefaultServiceRegister
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 10:21
 * @Version: 1.0
 **/

@Slf4j
public class DefaultServiceRegister implements ServieRegister {
    ServieRegister delegate;
    Map<String, List<ServiceMetadata>> cache = new HashMap<>();

    @Override
    public void init(RegisterConfig registerConfig) throws Exception {
        this.delegate = createServiceRegister(registerConfig);
        this.delegate.init(registerConfig);
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

    public static ServieRegister createServiceRegister(RegisterConfig registerConfig) {
        if ("zookeeper".equals(registerConfig.getRegisterType())) {
            return new ZookeeperServiceRegister();
        }
        if ("Redis".equals(registerConfig.getRegisterType())) {
            return new RedisServiceRegister();
        }
        throw new IllegalArgumentException(registerConfig.getRegisterType() + "没有实现");
    }
}
