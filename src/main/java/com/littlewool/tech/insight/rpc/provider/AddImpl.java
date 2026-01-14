package com.littlewool.tech.insight.rpc.provider;

import com.littlewool.tech.insight.rpc.api.Add;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @ClassName: AddImpl
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/13 8:25
 * @Version: 1.0
 **/

@Slf4j
public class AddImpl implements Add {
    @Override
    public int add(int a, int b) {
        Random random=new Random();
        if(random.nextBoolean()){
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(4));
        }
        return a+b;
    }
    @Override
    public int minus(int a, int b) {
        return a-b;
    }
}
