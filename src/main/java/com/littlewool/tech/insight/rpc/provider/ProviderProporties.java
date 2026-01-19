package com.littlewool.tech.insight.rpc.provider;

import com.littlewool.tech.insight.rpc.register.RegistryConfig;
import lombok.Data;

/**
 * @ClassName: ProviderProporties
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 10:46
 * @Version: 1.0
 **/
@Data
public class ProviderProporties {
    private String host;
    private int port;
    private int workThreadNum=4;
    private int globalMaxRequest=1000;
    private int perConsumerMaxRequest=500;
    private String serialize="json";
    private String compress="none";
    private RegistryConfig registryConfig;
}
