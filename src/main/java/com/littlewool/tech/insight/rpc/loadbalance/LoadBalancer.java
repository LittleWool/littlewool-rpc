package com.littlewool.tech.insight.rpc.loadbalance;

import com.littlewool.tech.insight.rpc.register.ServiceMetadata;

import java.util.List;

/**
 * @ClassName: LoadBalancer
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 11:01
 * @Version: 1.0
 **/

public interface LoadBalancer {
    ServiceMetadata select(List<ServiceMetadata> metadataList);
}
