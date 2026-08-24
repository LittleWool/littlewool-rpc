package com.littlewool.tech.insight.rpc.provider;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.fastjson2.JSONObject;
import com.littlewool.tech.insight.rpc.codec.LWDecoder;
import com.littlewool.tech.insight.rpc.codec.LWEncoder;
import com.littlewool.tech.insight.rpc.compress.Compression;
import com.littlewool.tech.insight.rpc.compress.CompressionManager;
import com.littlewool.tech.insight.rpc.handler.HeartbeatHandler;
import com.littlewool.tech.insight.rpc.handler.TrafficRecordHandler;
import com.littlewool.tech.insight.rpc.limit.ConcurrencyLimiter;
import com.littlewool.tech.insight.rpc.limit.Limiter;
import com.littlewool.tech.insight.rpc.limit.LeakyBucketLimiter;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.DefaultServiceRegistry;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import com.littlewool.tech.insight.rpc.register.ServieRegistry;
import com.littlewool.tech.insight.rpc.serializer.Serializer;
import com.littlewool.tech.insight.rpc.serializer.SerizalizerManager;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

/**
 * @ClassName: Provider
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 10:04
 * @Version: 1.0
 **/

@Slf4j
public class ProviderServer {

    // 注册表 记录当前provider提供的服务
    private final ProviderRegistry registry;

    private final Set<Class<?>> NO_RESOLVE_CLASS_SET;

    // 注册中心 将自己注册到注册中心以便被consumer发现，在这里负责将注册表中的服务注册到注册中心
    private final ServieRegistry servieRegistry;

    private final ProviderProporties providerProporties;

    private final Limiter globalLimiter;

    private EventLoopGroup bossEventLoopGroup;

    private EventLoopGroup workerEventLoopGroup;

    private final SerizalizerManager serizalizerManager;

    private final CompressionManager compressionManager;

    //解放EventLoopGroup
    private ThreadPoolExecutor invokeExecutor;

    public ProviderServer(ProviderProporties providerProporties) {
        this.providerProporties = providerProporties;
        this.globalLimiter = new ConcurrencyLimiter(providerProporties.getGlobalMaxRequest());
        this.registry = new ProviderRegistry();
        this.servieRegistry = new DefaultServiceRegistry();
        this.serizalizerManager = new SerizalizerManager();
        this.compressionManager = new CompressionManager();
        this.invokeExecutor=new ThreadPoolExecutor(4,4,10,TimeUnit.SECONDS,new ArrayBlockingQueue<>(1024),new FastFailResponseHandler());
        this.NO_RESOLVE_CLASS_SET=new HashSet<>();
        NO_RESOLVE_CLASS_SET.add(int.class);
        NO_RESOLVE_CLASS_SET.add(String.class);
        NO_RESOLVE_CLASS_SET.add(Integer.class);

    }

    public void start() {
        bossEventLoopGroup = new NioEventLoopGroup();
        workerEventLoopGroup = new NioEventLoopGroup(providerProporties.getWorkThreadNum());

        try {
            this.servieRegistry.init(providerProporties.getRegistryConfig());
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossEventLoopGroup, workerEventLoopGroup).channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel nioSocketChannel) {
                        nioSocketChannel.pipeline()
                                .addLast(new TrafficRecordHandler())
                                .addLast(new LWDecoder()).addLast(new LWEncoder())
                                .addLast(new IdleStateHandler(30,5,0, TimeUnit.SECONDS))
                                .addLast(new HeartbeatHandler())
                            .addLast(new LimitHandler()).addLast(new ProviderHandler());
                    }
                });

            serverBootstrap.bind(providerProporties.getHost(), providerProporties.getPort()).sync();
            // 注册到注册中心
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

    // 限流获取令牌之后需要释放，故是双向处理器
    public class LimitHandler extends ChannelDuplexHandler {
        private final AttributeKey<Limiter> CHANNEL_LIMITER_KEY = AttributeKey.valueOf("channle_limiter_key");
        private final AttributeKey<AtomicInteger> GLOBAL_PERMITS = AttributeKey.valueOf("global_permits");

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            Request request = (Request)msg;

            if (!globalLimiter.tryAcquire()) {
                ctx.writeAndFlush(Response.fail("全局 provider 限流", request.getRequestId()));
                return;
            }

            Limiter channelLimiter = ctx.channel().attr(CHANNEL_LIMITER_KEY).get();
            if (!channelLimiter.tryAcquire()) {
                // 把全局限流器的令牌返回
                globalLimiter.release();
                ctx.writeAndFlush(Response.fail("channel provider 限流", request.getRequestId()));
                return;
            }

            ctx.channel().attr(GLOBAL_PERMITS).get().incrementAndGet();
            ctx.fireChannelRead(request);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

            promise.addListener(f -> {
                int remain = ctx.channel().attr(GLOBAL_PERMITS).get().getAndDecrement();
                if (remain >= 0) {
                    ctx.channel().attr(CHANNEL_LIMITER_KEY).get().release();
                    globalLimiter.release();
                }
                // 小于0说明 断开连接释放许可已经触发过了
            });
            ctx.write(msg, promise);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            // 防止有请求进入但未响应,导致全局许可无法释放, 在断开连接时做兜底释放
            int remain = ctx.channel().attr(GLOBAL_PERMITS).get().getAndSet(0);
            globalLimiter.release(remain);
            ctx.fireChannelInactive();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            LeakyBucketLimiter channelLimiter = new LeakyBucketLimiter(providerProporties.getPerConsumerMaxRequest());
            ctx.channel().attr(CHANNEL_LIMITER_KEY).set(channelLimiter);
            ctx.channel().attr(GLOBAL_PERMITS).set(new AtomicInteger(0));
            ctx.fireChannelActive();
        }

    }
    private class FastFailResponseHandler implements RejectedExecutionHandler{

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            if(task instanceof InvokeTask){
                InvokeTask invokeTask = (InvokeTask)task;
                Response response = Response.fail("服务器繁忙", invokeTask.request.getRequestId());
                invokeTask.ctx.writeAndFlush(response);
                return;
            }
            throw new RuntimeException("任务不是InvokeTask,有问题");
        }
    }

    private class InvokeTask implements Runnable{

        private Request request;
        private ChannelHandlerContext ctx;
        private  ProviderRegistry.Invocation<?> invocation;
        public InvokeTask(Request request, ChannelHandlerContext ctx, ProviderRegistry.Invocation<?> invocation) {
            this.request=request;
            this.ctx=ctx;
            this.invocation=invocation;
        }

        @Override
        public void run() {
            EventLoop eventLoop = ctx.channel().eventLoop();
            try {
                Class[] paramsType = resolveMethodParamsType(request);
                long startTime = System.currentTimeMillis();
                Object result =
                        invocation.invoke(request.getMethodName(),paramsType , resolveMethodParams(request,paramsType));
                log.info("requestId{},{}函数被调用了{},结果是{},耗时是{},时间是{}", request.getRequestId(), request.getServiceName(),
                        request.getMethodName(), request, System.currentTimeMillis() - startTime,System.currentTimeMillis());
                Object finalResult=request.isGenericInvoke()?resolveResult(result):result;
                eventLoop.execute(()->ctx.writeAndFlush(Response.success(finalResult, request.getRequestId())));
            } catch (Exception e) {
                eventLoop.execute(()->ctx.writeAndFlush(Response.fail(e.getMessage(), request.getRequestId())));
            }
        }
        private Object resolveResult(Object originResult){
            Class<?> resultClass = originResult.getClass();
            if (NO_RESOLVE_CLASS_SET.contains(resultClass)){
                return originResult;
            }
            //如果传入是list 或是本身map 也需要判断
            return new HashMap<>(JSONObject.from(originResult));
        }

        @SuppressWarnings("all")
        private Object[] resolveMethodParams(Request request,Class[] paramsType){
            if(!request.isGenericInvoke()){
                return request.getParams();
            }
            Object[] params=request.getParams();
            Object[] result=new Object[params.length];
            for (int i=0;i<params.length;i++){
                if (params[i] instanceof Map){
                    result[i]=new JSONObject((Map)params[i]).toJavaObject(paramsType[i]);
                }else {
                    result[i]=params[i];
                }
            }
            return result;

        }
        @SuppressWarnings("all")
        private Class[] resolveMethodParamsType(Request request) throws ClassNotFoundException {
            if(!request.isGenericInvoke()){
                return request.getParamClass();
            }
            String[] paramsClassStr = request.getParamsClassStr();
            Class[] paramClass=new Class[paramsClassStr.length];
            for (int i = 0; i < paramsClassStr.length; i++) {
                String classStr=paramsClassStr[i];
                paramClass[i]=analysisFromString(classStr);
            }
            return paramClass;
        }

        //为什么这里是class<?> 而不是class
        private Class<?> analysisFromString(String classStr) throws ClassNotFoundException {
            if (classStr.equals("int")){
                return int.class;
            }
            return Class.forName(classStr);
        }
    }

    public class ProviderHandler extends SimpleChannelInboundHandler<Request> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("providerHandler 地址:{}连接了", ctx.channel().remoteAddress());
            //这里放置的是配置文件里的序列化和压缩
            Serializer serializer = serizalizerManager.getSerializer(providerProporties.getSerialize());
            ctx.channel().attr(LWEncoder.SERIALIZE_KEY).set(serializer.getName());
            ctx.channel().attr(LWEncoder.SERIALIZER_MANAGER_KEY).set(serizalizerManager);
            Compression compression = compressionManager.getCompression(providerProporties.getCompress());

            ctx.channel().attr(LWEncoder.COMPRESS_KEY).set(compression.getName());
            ctx.channel().attr(LWEncoder.COMPRESS_MANAGER_KEY).set(compressionManager);
            ctx.fireChannelActive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("providerHandler 发生了异常", cause);
            ctx.channel().close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("providerHandler地址:{}断开连接", ctx.channel().remoteAddress());
            super.channelInactive(ctx);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Request request) {
            ProviderRegistry.Invocation<?> invocation = registry.findService(request.getServiceName());
            if (null == invocation) {
                Response fail =
                    Response.fail(String.format("%s 没有对应的处理服务", request.getServiceName()), request.getRequestId());
                ctx.writeAndFlush(fail);
                return;
            }
            invokeExecutor.execute(new InvokeTask(request,ctx,invocation));
        }
    }

}
