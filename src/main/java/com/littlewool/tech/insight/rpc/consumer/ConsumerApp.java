package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.api.Add;
import com.littlewool.tech.insight.rpc.codec.LWDecoder;
import com.littlewool.tech.insight.rpc.codec.RequestEncoder;
import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: ConsumerApp
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:22
 * @Version: 1.0
 **/
@Slf4j
public class ConsumerApp {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ConsumerProxyFactory consumerProxyFactory=new ConsumerProxyFactory();

        for (int i = 0; i < 12; i++) {
            Add consumer = consumerProxyFactory.createConsumerProxy(Add.class);
            System.out.println(consumer.add(35, 23));
            System.out.println(consumer.minus(5, 7));
        }


    }


}
