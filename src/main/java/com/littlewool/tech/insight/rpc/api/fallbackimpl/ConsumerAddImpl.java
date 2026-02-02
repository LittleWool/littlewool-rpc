package com.littlewool.tech.insight.rpc.api.fallbackimpl;

import com.littlewool.tech.insight.rpc.api.Add;
import com.littlewool.tech.insight.rpc.api.User;
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

    @Override
    public User mergeAge(User user1, User user2) {
        User user=new User();
        user.setAge(user1.getAge()+user2.getAge());
        user.setName("降级服务的的User");
        return user;
    }
}
