package com.littlewool.tech.insight.rpc.provider;

import com.littlewool.tech.insight.rpc.codec.LWDecoder;
import com.littlewool.tech.insight.rpc.codec.ResponseEncoder;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.DefaultServiceRegistry;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import com.littlewool.tech.insight.rpc.register.ServieRegistry;
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

    private final ProviderRegistry registry;

    private final ServieRegistry servieRegistry;

    private ProviderProporties providerProporties;

    private EventLoopGroup bossEventLoopGroup;

    private EventLoopGroup workerEventLoopGroup;


    public ProviderServer(ProviderProporties providerProporties) {
        this.providerProporties=providerProporties;
        this.registry = new ProviderRegistry();
        this.servieRegistry = new DefaultServiceRegistry();
    }

    public void start() {
        bossEventLoopGroup = new NioEventLoopGroup();
        workerEventLoopGroup = new NioEventLoopGroup(providerProporties.getWorkThreadNum());
        try {
            this.servieRegistry.init(providerProporties.getRegistryConfig());
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

            serverBootstrap.bind(providerProporties.getHost(),providerProporties.getPort()).sync();
            //注册到注册中心
            registry.allServiceName().stream().map(this::buildMetadata).forEach(this.servieRegistry::registerService);
        } catch (Exception e) {
            throw new RuntimeException("服务器启动异常", e);
        }
    }

    private ServiceMetadata buildMetadata(String serviceName) {
        ServiceMetadata metadata = new ServiceMetadata();
        metadata.setServiceName(serviceName);
        metadata.setHost(providerProporties.getHost());
        metadata.setPort(providerProporties.getPort());
        return metadata;
    }

    public void stop() {
        if (null != bossEventLoopGroup) {
            bossEventLoopGroup.shutdown();
        }
        if (null != workerEventLoopGroup) {
            workerEventLoopGroup.shutdown();
        }
    }

    public <I> void register(Class<I> interfaceClass, I serviceInstance) {
        registry.register(interfaceClass, serviceInstance);
    }

    public class ProviderHandler extends SimpleChannelInboundHandler<Request> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("地址:{}连接了", ctx.channel().remoteAddress());
            super.channelActive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("发生了异常", cause);
            ctx.channel().close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("地址:{}断开连接", ctx.channel().remoteAddress());
            super.channelInactive(ctx);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext,
                                    Request request) {
            ProviderRegistry.Invocation<?> invocation = registry.findService(request.getServiceName());
            log.info("收到request {}",request.getRequestId());
            if (null == invocation) {
                Response fail = Response.fail(String.format("%s 没有对应的处理服务", request.getServiceName()),
                        request.getRequestId());
                channelHandlerContext.writeAndFlush(fail);
                return;
            }

            try {
                long startTime=System.currentTimeMillis();
                log.info("开始时间"+startTime);
                Object result = invocation.invoke(request.getMethodName(), request.getParamClass(),
                        request.getParams());
                log.info("requestId{},{}函数被调用了{},结果是{},耗时是{}",request.getRequestId(), request.getServiceName(), request.getMethodName(), request,System.currentTimeMillis()-startTime);
                channelHandlerContext.writeAndFlush(Response.success(result, request.getRequestId()));
            } catch (Exception e) {
                Response failReso=Response.fail(e.getMessage(),request.getRequestId());
                channelHandlerContext.writeAndFlush(Response.fail(e.getMessage(), request.getRequestId()));
            }

        }
    }


}
