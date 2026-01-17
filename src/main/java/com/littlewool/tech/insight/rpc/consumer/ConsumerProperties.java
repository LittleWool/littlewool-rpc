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
    private Integer methodTimeoutMs=30000;
    private Integer rpcPerSecond=100;
    private Integer rpcPerChannel=3;
    private String loadBalancePolicy="robin";
    private String retryPolicy="forking";
    private double slowRequestBreakingRatio=0.5;
    private long slowRequestMs=1000L;
    private RegistryConfig registryConfig =new RegistryConfig();
}
