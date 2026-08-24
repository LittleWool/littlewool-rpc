package com.littlewool.tech.insight.rpc.retry;

import com.littlewool.tech.insight.rpc.loadbalance.LoadBalancer;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

public class RetryPolicyTest {

    @Test
    public void failOverRetriesAnotherProvider() throws Exception {
        ServiceMetadata failed = provider("127.0.0.1", 8080);
        ServiceMetadata backup = provider("127.0.0.1", 8081);

        RetryContext context = new RetryContext();
        context.setFailedService(failed);
        context.setServiceMetadataList(Arrays.asList(failed, backup));
        context.setRequestTimeoutMs(1000);
        context.setMethodTimeoutMs(1000);
        context.setLoadBalancer(new LoadBalancer() {
            @Override
            public ServiceMetadata select(java.util.List<ServiceMetadata> serviceMetadataList) {
                return serviceMetadataList.get(0);
            }
        });
        context.setDoRpcFunction(provider -> CompletableFuture.completedFuture(Response.success(provider.getPort(), 1)));

        Response response = new FailOverRetryPolicy().retry(context);

        assertEquals(8081, response.getResult());
    }

    private ServiceMetadata provider(String host, int port) {
        ServiceMetadata metadata = new ServiceMetadata();
        metadata.setHost(host);
        metadata.setPort(port);
        metadata.setServiceName("demo");
        return metadata;
    }
}
