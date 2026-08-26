package com.littlewool.tech.insight.rpc.message;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName: Request
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:02
 * @Version: 1.0
 **/
@Data
public class Request  implements Serializable {
    private static final AtomicInteger REQUEST_INTEGER = new AtomicInteger();

    private int requestId = REQUEST_INTEGER.getAndIncrement();

    private String serviceName;

    private InvocationType invocationType = InvocationType.NORMAL;

    private boolean genericInvoke;

    private String methodName;

    private String[] paramsClassStr;

    private Class<?>[] paramClass;

    private Object[] params;

    public void markNormalInvoke() {
        this.invocationType = InvocationType.NORMAL;
        this.genericInvoke = false;
    }

    public void markGenericInvoke() {
        this.invocationType = InvocationType.GENERIC;
        this.genericInvoke = true;
    }

    @JSONField(serialize = false)
    public boolean isGenericCall() {
        return invocationType == InvocationType.GENERIC || genericInvoke;
    }
}
