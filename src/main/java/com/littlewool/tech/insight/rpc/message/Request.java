package com.littlewool.tech.insight.rpc.message;

import lombok.Data;

/**
 * @ClassName: Request
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:02
 * @Version: 1.0
 **/
@Data
public class Request {

    private String serviceName;
    private String methodName;
    private Class<?>[] paramClass;
    private Object[] params;

}
