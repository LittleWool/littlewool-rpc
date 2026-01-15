package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.exception.LimitException;
import com.littlewool.tech.insight.rpc.limit.ConcurrencyLimiter;
import com.littlewool.tech.insight.rpc.limit.Limiter;
import com.littlewool.tech.insight.rpc.limit.RateLimiter;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @ClassName: InFlightRequestManager
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 11:17
 * @Version: 1.0
 **/

@Slf4j
public class InFlightRequestManager {

    private final Map<Integer, CompletableFuture<Response>> inFlightRequestTable;

    private final HashedWheelTimer timeoutTimer;

    private final Limiter globalLimiter;

    private final Map<ServiceMetadata,Limiter> channelLimiterMap;

    private final ConsumerProperties consumerProperties;

    public InFlightRequestManager(ConsumerProperties consumerProperties) {
//        this.globalLimiter = new RateLimiter(consumerProperties.getRpcPerSecond());
        this.globalLimiter = new ConcurrencyLimiter(consumerProperties.getRpcPerSecond());
        this.inFlightRequestTable = new ConcurrentHashMap<>();
        this.timeoutTimer = new HashedWheelTimer(100,TimeUnit.MILLISECONDS,256);
        this.channelLimiterMap=new ConcurrentHashMap<>();
        this.consumerProperties=consumerProperties;
    }

    public CompletableFuture<Response> inFlightRequest(Request request, long timeoutMs, ServiceMetadata metadata) {
        CompletableFuture<Response> responseFuture = new CompletableFuture<>();

        //先判断全局限流器
        if(!globalLimiter.tryAcquire()){
            responseFuture.completeExceptionally(new LimitException("全局限流,当前在途请求超过阈值"));
            return responseFuture;
        }
        Limiter channelLimiter = channelLimiterMap.computeIfAbsent(metadata, k -> new RateLimiter(consumerProperties.getRpcPerChannel()));

        if(!channelLimiter.tryAcquire()){
            responseFuture.completeExceptionally(new LimitException("channel限流,当前在途请求超过阈值"));
            return responseFuture;
        }

        inFlightRequestTable.put(request.getRequestId(), responseFuture);
        //定时给请求进行异常结束 也就是超时 方便超时时移除request
        Timeout timeout = timeoutTimer.newTimeout((t) -> responseFuture.completeExceptionally(new TimeoutException()),
                timeoutMs, TimeUnit.MILLISECONDS);
        //正常或者异常结束时候移除,但超时时候自身实际上并不会触发异常所以需要定时任务
        responseFuture.whenComplete((r, e) -> {
            inFlightRequestTable.remove(request.getRequestId());
            channelLimiter.release();
            globalLimiter.release();
            timeout.cancel();
        });
        return responseFuture;
    }
    public boolean completeRequest(int requestId, Response response){
        CompletableFuture<Response> future = inFlightRequestTable.remove(requestId);
        if(null==future){
            log.warn("空闲返回 {}",requestId);
            return false;
        }
        return future.complete(response);
    }
    public boolean completeExceptionallyRequest(int requestId, Exception e){
        CompletableFuture<Response> future = inFlightRequestTable.remove(requestId);
        if(null==future){
            log.warn("空闲异常 {}",requestId,e);
            return false;
        }
        return future.completeExceptionally(e);
    }
}
