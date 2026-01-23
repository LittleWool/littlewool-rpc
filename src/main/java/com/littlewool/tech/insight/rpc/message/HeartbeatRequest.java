package com.littlewool.tech.insight.rpc.message;

import lombok.Data;

import java.io.Serializable;

/**
 * @ClassName: HeartBeatRequest
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/23 9:52
 * @Version: 1.0
 **/
@Data
public class HeartbeatRequest implements Serializable {
    private final long requestTime=System.currentTimeMillis();

}
