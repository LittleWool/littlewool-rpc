package com.littlewool.tech.insight.rpc.fallback;

import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;
import lombok.Data;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: CacheFallback
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 20:30
 * @Version: 1.0
 **/

public class CacheFallback implements Fallback{
    private final Map<InvokeKey,Object> rpcResultCache=new ConcurrentHashMap();

    private static final Object NULL_OBJECT=new Object();
    @Override
    public Object fallback(RpcCallMetrics metrics) {
        InvokeKey invokeKey=new InvokeKey(metrics.getMethod(),metrics.getParams());
        Object cacheResult = rpcResultCache.get(invokeKey);
        //正常请求返回的结果就是null
        if(cacheResult==NULL_OBJECT){
            return null;
        }
        //缓存中没有这个请求
        if (cacheResult==null){
            throw new RpcException("缓存降级没招了");
        }
        return cacheResult;
    }

    @Override
    public void recordMetrics(RpcCallMetrics successMetrics) {
        InvokeKey invokeKey=new InvokeKey(successMetrics.getMethod(),successMetrics.getParams());
        Object result = successMetrics.getResult();
        if(null==result){
            result=NULL_OBJECT;
        }
        rpcResultCache.put(invokeKey,result);

    }
    @Data
    private class InvokeKey{
        public InvokeKey(Method method, Object[] args) {
            this.method = method;
            this.args = args;
        }

        final Method method;
        final Object[] args;

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            InvokeKey invokeKey = (InvokeKey) o;
            return Objects.equals(method, invokeKey.method) && Objects.deepEquals(args, invokeKey.args);
        }

        @Override
        public int hashCode() {
            return Objects.hash(method, Arrays.hashCode(args));
        }
    }

}
