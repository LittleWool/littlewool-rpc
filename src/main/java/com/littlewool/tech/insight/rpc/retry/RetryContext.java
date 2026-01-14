package com.littlewool.tech.insight.rpc.retry;

import com.littlewool.tech.insight.rpc.loadbalance.LoadBalancer;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import lombok.Data;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * @ClassName: RetryContext
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 17:47
 * @Version: 1.0
 **/
@Data
public class RetryContext {

    private ServiceMetadata failedService;

    private List<ServiceMetadata> serviceMetadataList;

    private long methodTimeoutMs;

    private long requestTimeoutMs;

    private LoadBalancer loadBalancer;

    private Function<ServiceMetadata,CompletableFuture<Response>> doRpcFunction;
    public CompletableFuture<Response> doRpc(ServiceMetadata serviceMetadata){
        return doRpcFunction.apply(serviceMetadata);
    }
}
