package com.littlewool.tech.insight.rpc.message;

import lombok.Data;

import java.io.Serializable;

/**
 * @ClassName: HeartbeatResponse
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/23 9:52
 * @Version: 1.0
 **/
@Data
public class HeartbeatResponse implements Serializable {
    private long requestTime;

    public HeartbeatResponse(long requestTime) {
        this.requestTime = requestTime;
    }
}
