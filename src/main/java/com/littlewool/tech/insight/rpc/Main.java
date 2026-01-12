package com.littlewool.tech.insight.rpc;

import java.util.concurrent.ExecutionException;

/**
 * @ClassName: Main
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 10:04
 * @Version: 1.0
 **/

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Consumer consumer = new Consumer();

        System.out.println(consumer.add(1, 2));
        System.out.println(consumer.add(13, 27));
    }
}
