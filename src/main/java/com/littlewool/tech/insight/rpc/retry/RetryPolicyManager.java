package com.littlewool.tech.insight.rpc.retry;

import com.littlewool.tech.insight.rpc.serializer.Serializer;
import com.littlewool.tech.insight.rpc.spi.Spi;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @ClassName: RetryPolicyManager
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/2/1 21:09
 * @Version: 1.0
 **/
@Slf4j
public class RetryPolicyManager {
    private Map<String, RetryPolicy> nameMap = new HashMap<>();

    public RetryPolicyManager() {
        init();
    }
    public RetryPolicy getRetryPolicy(String name){
        return nameMap.get(name.toUpperCase());
    }
    public void init(){
        ServiceLoader<RetryPolicy> retryPolicies = ServiceLoader.load(RetryPolicy.class);
        for (RetryPolicy retryPolicy : retryPolicies) {
            Class<? extends RetryPolicy> aClass = retryPolicy.getClass();
            if (!aClass.isAnnotationPresent(Spi.class)){
                log.warn("这个类{} 没有spi注解无法被管理",aClass);
            }
            Spi annotation = aClass.getAnnotation(Spi.class);
            if (null!=nameMap.put(annotation.value().toUpperCase(),retryPolicy)){
                throw new IllegalArgumentException("重试策略名称重复");
            }

        }
    }
}
