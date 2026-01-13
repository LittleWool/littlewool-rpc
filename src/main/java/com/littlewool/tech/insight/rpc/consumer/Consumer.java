package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.api.Add;
import com.littlewool.tech.insight.rpc.codec.LWDecoder;
import com.littlewool.tech.insight.rpc.codec.RequestEncoder;
import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: Consumer
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 10:04
 * @Version: 1.0
 **/

public class Consumer implements Add {

    private Map<Integer, CompletableFuture<?>> inFlightRequestTable = new ConcurrentHashMap<>();

    private final ConnectionManager connectionManager=new ConnectionManager(createBootstrap());

    private Bootstrap createBootstrap() {
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
                                        CompletableFuture requestFuture =
                                                inFlightRequestTable.remove(response.getRequestId());
                                        if (response.getCode() == 200) {
                                            requestFuture.complete(Integer.valueOf(response.getResult().toString()));
                                        } else {
                                            requestFuture.completeExceptionally(new RpcException(response.getErrorMessage()));
                                        }
//                                        channelHandlerContext.close();
                                    }
                                });
                    }
                });
    }

    @Override
    public int add(int a, int b) {
        try {
            CompletableFuture<Integer> resFuture = new CompletableFuture<>();
            Bootstrap bootstrap = new Bootstrap();


            Channel channel = connectionManager.getChannel("localhost", 8888);
            if (null == channel) {
                throw new RuntimeException("provider 连接失败");
            }
            Request request = new Request();
            request.setServiceName(Add.class.getName());
            request.setMethodName("" +
                    "add");
            request.setParamClass(new Class[]{int.class, int.class});
            request.setParams(new Object[]{a, b});
            channel.writeAndFlush(request).addListener(f -> {
                if (f.isSuccess()) {
                    inFlightRequestTable.put(request.getRequestId(), resFuture);
                }
            });
            return resFuture.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public class ConsumerClass extends SimpleChannelInboundHandler {

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {

        }
    }
}
