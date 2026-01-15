package com.littlewool.tech.insight.rpc.api.fallbackimpl;

import com.littlewool.tech.insight.rpc.api.Add;
import lombok.extern.slf4j.Slf4j;

/**
 * @ClassName: ConsumerFallbackImpl
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 20:59
 * @Version: 1.0
 **/

@Slf4j
public class ConsumerAddImpl implements Add {
    @Override
    public int add(int a, int b) {
        log.info("consumer降级实现");
        return a+b;
    }

    @Override
    public int minus(int a, int b) {
        log.info("consumer降级实现");
        return a-b;
    }
}
