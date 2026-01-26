package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.api.Add;
import com.littlewool.tech.insight.rpc.register.RegistryConfig;

import lombok.extern.slf4j.Slf4j;

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
        new Thread(()->{
            while (true){
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                long startTime=System.currentTimeMillis();
                log.info("开始时间{}",System.currentTimeMillis());
                System.out.println(consumer.add(1, 2)+" "+(System.currentTimeMillis()-startTime)+"毫秒");
            }
        }).start();
//        new Thread(()->{
//            while (true){
//                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
//                long startTime=System.currentTimeMillis();
//
//                System.out.println(consumer.minus(1,2)+" "+(System.currentTimeMillis()-startTime)+"毫秒");
//                System.out.println(startTime+"--------"+System.currentTimeMillis());
//            }
//        }).start();


    }


}
