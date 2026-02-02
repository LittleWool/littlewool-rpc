package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.api.Add;
import com.littlewool.tech.insight.rpc.api.User;
import com.littlewool.tech.insight.rpc.register.RegistryConfig;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @ClassName: ConsumerApp
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:22
 * @Version: 1.0
 **/
@Slf4j
public class ConsumerApp {
    public static void main(String[] args) throws Exception {
        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setRegisterType("zookeeper");
        registryConfig.setConnectString("127.0.0.1:2181");
        ConsumerProperties consumerProperties = new ConsumerProperties();
        consumerProperties.setRegistryConfig(registryConfig);
        ConsumerProxyFactory consumerProxyFactory = new ConsumerProxyFactory(consumerProperties);
        Add consumer = consumerProxyFactory.createConsumerProxy(Add.class);
        System.out.println("第一次调用"+consumer.add(1,2));
        GenericConsumer genericConsumer=consumerProxyFactory.createConsumerProxy(GenericConsumer.class);
        System.out.println("第二次调用"+genericConsumer.$invoke(Add.class.getName(),"add",new String[]{"int","int"},new Object[]{12,13}));

        Map<String,Object> user1=new HashMap<>();
        user1.put("name","zhangsan");
        user1.put("age",13);
        Map<String,Object> user2=new HashMap<>();
        user2.put("name","lisi");
        user2.put("age",12);
        System.out.println("第三次调用"+genericConsumer.$invoke(Add.class.getName()
                ,"mergeAge"
                ,new String[]{User.class.getName()
                        ,User.class.getName()}
                ,new Object[]{user1,user2}));

    }


}
