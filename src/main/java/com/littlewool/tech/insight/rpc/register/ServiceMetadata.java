package com.littlewool.tech.insight.rpc.register;

import lombok.Data;

/**
 * @ClassName: ServiceMetadata
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/13 20:11
 * @Version: 1.0
 **/

@Data
public class ServiceMetadata {
    private String host;
    private int port;
    private String serviceName;
}
