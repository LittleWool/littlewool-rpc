package com.littlewool.tech.insight.rpc.provider;

import com.littlewool.tech.insight.rpc.api.Add;
import com.littlewool.tech.insight.rpc.register.RegisterConfig;

/**
 * @ClassName: ProviderApp
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:22
 * @Version: 1.0
 **/

public class ProviderApp {
    public static void main(String[] args) {
        RegisterConfig registerConfig=new RegisterConfig();
        registerConfig.setRegisterType("zookeeper");
        registerConfig.setConnectString("127.0.0.1:2181");
        ProviderServer providerServer = new ProviderServer("127.0.0.1",8888,registerConfig);
        providerServer.register(Add.class,new AddImpl());
        providerServer.start();
    }
}
