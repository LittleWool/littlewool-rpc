package com.littlewool.tech.insight.rpc.register;

import java.util.List;

/**
 * @ClassName: ServieRegister
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/13 20:09
 * @Version: 1.0
 **/

public interface ServieRegister {
    void init(RegisterConfig registerConfig) throws Exception;

    void registerService(ServiceMetadata metadata);

    List<ServiceMetadata> fetchServiceList(String serviceName) throws Exception;
}
