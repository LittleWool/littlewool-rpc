package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.api.Add;
import com.littlewool.tech.insight.rpc.codec.LWDecoder;
import com.littlewool.tech.insight.rpc.codec.RequestEncoder;
import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName: Consumer
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 10:04
 * @Version: 1.0
 **/

public class Consumer implements Add {

    @Override
    public int add(int a, int b) {
        try {
            CompletableFuture<Integer> resFuture = new CompletableFuture<>();
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(new NioEventLoopGroup())
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
                                            if (response.getCode() == 200) {
                                                resFuture.complete(Integer.valueOf(response.getResult().toString()));
                                            } else {
                                                resFuture.completeExceptionally(new RpcException(response.getErrorMessage()));
                                            }
                                            channelHandlerContext.close();
                                        }
                                    });
                        }
                    });
            ChannelFuture channelFuture = bootstrap.connect("localhost", 8888).sync();
            Request request = new Request();
            request.setServiceName(Add.class.getName());
            request.setMethodName("add");
            request.setParamClass(new Class[]{int.class, int.class});
            request.setParams(new Object[]{a, b});
            channelFuture.channel().writeAndFlush(request);
            return resFuture.get();
        } catch (Exception e) {
            throw new RuntimeException("方法调用异常",e);
        }

    }

    public class ConsumerClass extends SimpleChannelInboundHandler {

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {

        }
    }
}
