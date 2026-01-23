package com.littlewool.tech.insight.rpc.message;

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

    private String methodName;

    private Class<?>[] paramClass;

    private Object[] params;

}
