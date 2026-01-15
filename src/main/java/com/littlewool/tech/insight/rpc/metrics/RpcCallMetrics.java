package com.littlewool.tech.insight.rpc.metrics;

import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import lombok.Data;

import java.lang.reflect.Method;

/**
 * @ClassName: RpcCallMetrics
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 17:28
 * @Version: 1.0
 **/
@Data
public class RpcCallMetrics {
    private boolean complete;
    private Throwable throwable;
    private long duration;
    private long startTime;
    private  Method method;
    private  ServiceMetadata provider;
    private  Object[] params;
    private Object result;

    private RpcCallMetrics() {

    }

    public static RpcCallMetrics createRpcMetrics(Method method,Object[] params,ServiceMetadata provider) {
        RpcCallMetrics rpcCallMetrics = new RpcCallMetrics();
        rpcCallMetrics.setStartTime(System.currentTimeMillis());
        rpcCallMetrics.setMethod(method);
        rpcCallMetrics.setProvider(provider);
        rpcCallMetrics.setParams(params);
        return rpcCallMetrics;
    }

    public void doComplete(Response response){
        this.result=response.getResult();
        this.complete=true;
        this.duration=System.currentTimeMillis()-startTime;
    }
    public void errorComplete(Throwable throwable){
        this.throwable=throwable;
        this.duration=System.currentTimeMillis()-startTime;
    }
}
