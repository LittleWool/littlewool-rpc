package com.littlewool.tech.insight.rpc.provider;

import com.littlewool.tech.insight.rpc.api.Add;

import com.littlewool.tech.insight.rpc.api.User;
import lombok.extern.slf4j.Slf4j;

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
//        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        return a + b;
    }

    @Override
    public int minus(int a, int b) {
        return a - b;
    }

    @Override
    public User mergeAge(User user1, User user2) {
        User user=new User();
        user.setAge(user1.getAge()+user2.getAge());
        user.setName("provider Merge后的User");
        return user;
    }
}
