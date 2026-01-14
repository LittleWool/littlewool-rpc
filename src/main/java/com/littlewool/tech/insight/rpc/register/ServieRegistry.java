package com.littlewool.tech.insight.rpc.register;

import java.util.List;

/**
 * @ClassName: ServieRegistry
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/13 20:09
 * @Version: 1.0
 **/

public interface ServieRegistry {
    void init(RegistryConfig registryConfig) throws Exception;

    void registerService(ServiceMetadata metadata);

    List<ServiceMetadata> fetchServiceList(String serviceName) throws Exception;
}
