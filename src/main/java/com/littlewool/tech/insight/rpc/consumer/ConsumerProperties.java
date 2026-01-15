package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.register.RegistryConfig;
import lombok.Data;

/**
 * @ClassName: ConsumerProperties
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 10:40
 * @Version: 1.0
 **/
@Data
public class ConsumerProperties {
    private Integer workThreadNum = 4;
    private Integer connectTimeoutMs=3000;
    private Integer requestTimeoutMs=3000;
    private Integer methodTimeoutMs=10000;
    private String loadBalancePolicy="random";
    private String retryPolicy="forking";
    private RegistryConfig registryConfig =new RegistryConfig();
}
