package com.littlewool.tech.insight.rpc.api;

import com.littlewool.tech.insight.rpc.api.fallbackimpl.ConsumerAddImpl;
import com.littlewool.tech.insight.rpc.fallback.RpcFallback;

/**
 * @ClassName: add
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/13 8:10
 * @Version: 1.0
 **/
@RpcFallback(ConsumerAddImpl.class)
public interface Add{
    public int add(int a,int b);

    public int minus(int a,int b);
}
