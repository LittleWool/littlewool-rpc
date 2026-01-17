package com.littlewool.tech.insight.rpc.retry;

import com.littlewool.tech.insight.rpc.message.Response;

/**
 * @ClassName: RetryPolicy
 * @Description: 超时的颗粒度问题,请求等待时间,方法等待时间
 * @Author: LittleWool
 * @Date: 2026/1/14 15:06
 * @Version: 1.0
 **/

//核心问题 将发送请求解耦并封装成为异步函数
public interface RetryPolicy {
    Response retry(RetryContext retryContext) throws Exception;
}
