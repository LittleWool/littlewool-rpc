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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: ConsumerProxyFactory
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/13 17:32
 * @Version: 1.0
 **/
@Slf4j
public class ConsumerProxyFactory {

    private Map<Integer, CompletableFuture<Response>> inFlightRequestTable = new ConcurrentHashMap<>();

    private final ConnectionManager connectionManager = new ConnectionManager(createBootstrap());

    public <I> I createConsumerProxy(Class<I> interfaceClass) {
       return   (I) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{Add.class},
                new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if (method.getDeclaringClass() == Object.class) {
                    if (method.getName().equals("toString")) {
                        return "LittleWool Proxy Consumer ";
                    }
                    if (method.getName().equals("equals")) {
                        return proxy == args[0];
                    }
                    if (method.getName().equals("hashCOde")) {
                        return System.identityHashCode(proxy);
                    }
                    throw new UnsupportedOperationException("代理对象不支持该函数" + method.getName());
                }
                try {
                    CompletableFuture<Response> responseFuture = new CompletableFuture<>();

                    Channel channel = connectionManager.getChannel("localhost", 8888);
                    if (null == channel) {
                        throw new RuntimeException("provider 连接失败");
                    }
                    Request request = new Request();
                    request.setServiceName(interfaceClass.getName());
                    request.setMethodName(method.getName());
                    request.setParamClass(method.getParameterTypes());
                    request.setParams(args);
                    channel.writeAndFlush(request).addListener(f -> {
                        if (f.isSuccess()) {
                            inFlightRequestTable.put(request.getRequestId(), responseFuture);
                        }
                    });
                    Response response = responseFuture.get(3, TimeUnit.SECONDS);
                    if (response.getCode() == 200) {
                        return response.getResult();
                    }
                        throw new RpcException(response.getErrorMessage());

                } catch (RpcException rpcException) {
                    throw rpcException;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private  Bootstrap createBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        return bootstrap.group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
                        nioSocketChannel.pipeline()
                                .addLast(new LWDecoder())
                                .addLast(new RequestEncoder())
                                .addLast(new SimpleChannelInboundHandler<Response>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext channelHandlerContext,
                                                                Response response) throws Exception {
                                        CompletableFuture<Response> requestFuture =
                                                inFlightRequestTable.remove(response.getRequestId());
                                        if (null == requestFuture) {
                                            log.warn("requstId {} 找不到", response.getRequestId());
                                            return;
                                        }
                                        requestFuture.complete(response);
                                    }
                                });
                    }
                });
    }
}
