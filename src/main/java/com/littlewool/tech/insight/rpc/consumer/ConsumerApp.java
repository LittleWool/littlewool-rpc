package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.api.Add;

import java.util.concurrent.ExecutionException;

/**
 * @ClassName: ConsumerApp
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:22
 * @Version: 1.0
 **/

public class ConsumerApp {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
       Add add= new Consumer();
        System.out.println(add.add(1, 2));
        System.out.println(add.add(11, 23));

    }
}
