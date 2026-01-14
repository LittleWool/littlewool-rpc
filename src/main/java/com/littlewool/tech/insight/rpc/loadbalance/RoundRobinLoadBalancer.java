package com.littlewool.tech.insight.rpc.loadbalance;

import com.littlewool.tech.insight.rpc.register.ServiceMetadata;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName: RoundRobinLoadBalancer
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 11:04
 * @Version: 1.0
 **/

public class RoundRobinLoadBalancer implements LoadBalancer {
    private final AtomicInteger index=new AtomicInteger();
    @Override
    public ServiceMetadata select(List<ServiceMetadata> metadataList) {
        int metadataIndex=index.getAndIncrement()% metadataList.size();
        return metadataList.get(Math.abs(metadataIndex));
    }
}
