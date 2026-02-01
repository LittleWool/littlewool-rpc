package com.littlewool.tech.insight.rpc.retry;

import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import com.littlewool.tech.insight.rpc.spi.Spi;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: ForkingRetryPolicy
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 19:26
 * @Version: 1.0
 **/
@Spi("forking")
public class ForkingRetryPolicy implements RetryPolicy{
    @Override
    public Response retry(RetryContext retryContext) throws Exception {
        List<ServiceMetadata> serviceMetadataList = new ArrayList<>(retryContext.getServiceMetadataList());
        serviceMetadataList.remove(retryContext.getFailedService());
        if(serviceMetadataList.isEmpty()){
            throw new RpcException("没有可重试的Provider");
        }
        CompletableFuture[] allFutures=new CompletableFuture[serviceMetadataList.size()];
        for (int i = 0; i < serviceMetadataList.size(); i++) {
            allFutures[i]=retryContext.doRpc(serviceMetadataList.get(i));
        }
        CompletableFuture<Object> mainFuture = CompletableFuture.anyOf(allFutures);
        return (Response) mainFuture.get(Math.min(retryContext.getRequestTimeoutMs(), retryContext.getMethodTimeoutMs()), TimeUnit.MILLISECONDS);
    }
}
