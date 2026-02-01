package com.littlewool.tech.insight.rpc.retry;

import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import com.littlewool.tech.insight.rpc.spi.Spi;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: FailOverPolicy
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 19:20
 * @Version: 1.0
 **/

@Slf4j
@Spi("failover")
public class FailOverRetryPolicy implements RetryPolicy {
    @Override
    public Response retry(RetryContext retryContext) throws Exception {
        List<ServiceMetadata> serviceMetadataList = new ArrayList<>(retryContext.getServiceMetadataList());
        ServiceMetadata failedService = retryContext.getFailedService();
        serviceMetadataList.remove(failedService);
        if (serviceMetadataList.isEmpty()) {
            throw new RpcException("没有可重试的Provider");
        }
        ServiceMetadata failOverService = retryContext.getLoadBalancer().select(serviceMetadataList);
        CompletableFuture<Response> future = retryContext.doRpc(failOverService);

        return future.get(Math.min(retryContext.getRequestTimeoutMs(), retryContext.getMethodTimeoutMs()),
            TimeUnit.MILLISECONDS);
    }
}
