package com.littlewool.tech.insight.rpc.loadbalance;

import com.littlewool.tech.insight.rpc.register.ServiceMetadata;

import java.util.List;
import java.util.Random;

/**
 * @ClassName: RandomLoadBalancer
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 11:06
 * @Version: 1.0
 **/

public class RandomLoadBalancer implements LoadBalancer {
    private final Random random=new Random();
    @Override
    public ServiceMetadata select(List<ServiceMetadata> metadataList) {
        if (metadataList == null || metadataList.isEmpty()) {
            return null; // 或抛出自定义异常
        }
        int index = random.nextInt( metadataList.size());
        return metadataList.get(Math.abs(index));
    }
}
