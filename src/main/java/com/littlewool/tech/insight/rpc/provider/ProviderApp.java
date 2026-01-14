package com.littlewool.tech.insight.rpc.provider;

import com.littlewool.tech.insight.rpc.api.Add;
import com.littlewool.tech.insight.rpc.register.RegistryConfig;

/**
 * @ClassName: ProviderApp
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:22
 * @Version: 1.0
 **/

public class ProviderApp {
    public static void main(String[] args) {
        RegistryConfig registryConfig =new RegistryConfig();
        registryConfig.setRegisterType("zookeeper");
        registryConfig.setConnectString("127.0.0.1:2181");
        ProviderProporties providerProporties=new ProviderProporties();
        providerProporties.setHost("127.0.0.1");
        providerProporties.setPort(8888);
        providerProporties.setRegistryConfig(registryConfig);
        ProviderServer providerServer = new ProviderServer(providerProporties);
        providerServer.register(Add.class,new AddImpl());
        providerServer.start();
    }
}
