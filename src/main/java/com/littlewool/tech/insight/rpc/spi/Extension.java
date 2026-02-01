package com.littlewool.tech.insight.rpc.spi;

/**
 * @ClassName: Extension
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/2/1 20:15
 * @Version: 1.0
 **/

public interface Extension {
    String getName();

    default int code(){
        return -1;
    }
}
