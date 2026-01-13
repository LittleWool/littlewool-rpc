package com.littlewool.tech.insight.rpc.provider;

import com.littlewool.tech.insight.rpc.codec.LWDecoder;
import com.littlewool.tech.insight.rpc.codec.ResponseEncoder;
import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @ClassName: Provider
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 10:04
 * @Version: 1.0
 * @Name
 **/

@Slf4j
public class ProviderServer {

    private int port;

    private EventLoopGroup bossEventLoopGroup;

    private EventLoopGroup workerEventLoopGroup;

    private  final ProviderRegistry registry;

    public ProviderServer(int port) {
        this.port = port;
        this.registry=new ProviderRegistry();
    }

    public void start() {
        bossEventLoopGroup = new NioEventLoopGroup();
        workerEventLoopGroup = new NioEventLoopGroup(4);

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossEventLoopGroup, workerEventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
                        nioSocketChannel.pipeline()
                                .addLast(new LWDecoder())
                                .addLast(new ResponseEncoder())
                                .addLast(new ProviderHandler());
                    }
                });
        try {
            serverBootstrap.bind(port).sync();
        } catch (InterruptedException e) {
            throw new RuntimeException("服务器启动异常", e);
        }
    }

    public void stop() {
        if (null != bossEventLoopGroup) {
            bossEventLoopGroup.shutdown();
        }
        if (null != workerEventLoopGroup) {
            workerEventLoopGroup.shutdown();
        }
    }

    public <I> void register(Class<I> interfaceClass, I serviceInstance){
        registry.register(interfaceClass,serviceInstance);
    }
    public class ProviderHandler extends SimpleChannelInboundHandler<Request>{

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("地址:{}连接了",ctx.channel().remoteAddress());
            super.channelActive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("发生了异常",cause);
            ctx.channel().close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("地址:{}断开连接",ctx.channel().remoteAddress());
            super.channelInactive(ctx);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext,
                                    Request request) throws Exception {
            ProviderRegistry.Invocation<?> invocation = registry.findService(request.getServiceName());

            if(null==invocation){
                Response fail = Response.fail(String.format("%s 没有对应的处理服务", request.getServiceName()),request.getRequestId());
                channelHandlerContext.writeAndFlush(fail);
                return;
            }

            try {
                Object result = invocation.invoke(request.getMethodName(),request.getParamClass(), request.getParams());
                log.info("{} 函数被调用了{}，结果是{}",request.getServiceName(),request.getMethodName(),request);
                channelHandlerContext.writeAndFlush(Response.success(result,request.getRequestId()));
            }catch (Exception e ){
                channelHandlerContext.writeAndFlush(Response.fail(e.getMessage(),request.getRequestId()));
            }

        }
    }


}
