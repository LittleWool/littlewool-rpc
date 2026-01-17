package com.littlewool.tech.insight.rpc.retry;

import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.message.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @ClassName: RetrySame
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 17:48
 * @Version: 1.0
 **/

@Slf4j
public class RetrySame implements RetryPolicy{
    final int retryMax=3;
    private final Random random=new Random();
    @Override
    public Response retry(RetryContext retryContext)throws Exception {
        int retryCount=0;
        long startTime=System.currentTimeMillis();
        while (retryCount<retryMax){
            long nextDelay = nextDelay(retryCount++);
            if(nextDelay>=1000L){
                nextDelay=1000L;
            }
            //若是等不到下次重试,则抛出超时异常
            long methodTime=retryContext.getMethodTimeoutMs()-(System.currentTimeMillis()-startTime);
            if(methodTime<=0||nextDelay>=methodTime){
                throw new TimeoutException();
            }
            //为什么sleep是可以的，因为阻塞的是主线程。consumer调用的线程，请求拿到结果之前就应该等待
            Thread.sleep(nextDelay);
            try {
                log.info("开始重试");
                CompletableFuture<Response> responseCompletableFuture = retryContext.doRpc(retryContext.getFailedService());
                return responseCompletableFuture.get(Math.min(methodTime,retryContext.getRequestTimeoutMs()), TimeUnit.MILLISECONDS);
            }catch (Exception e){
                log.error("重试失败第{}次",retryCount,e);
            }

        }
        throw new RpcException("重试失败");

    }

    //使用指数回避 来避免短时间内的频繁重试造成网络风暴，为避免有规律 加入扰动
    private long nextDelay(int retryCount){
        return 100L*(1L<<retryCount)+random.nextInt(50);
    }
}
