package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.codec.LWDecoder;
import com.littlewool.tech.insight.rpc.codec.RequestEncoder;
import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.loadbalance.LoadBalancer;
import com.littlewool.tech.insight.rpc.loadbalance.RandomLoadBalancer;
import com.littlewool.tech.insight.rpc.loadbalance.RoundRobinLoadBalancer;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.DefaultServiceRegistry;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import com.littlewool.tech.insight.rpc.register.ServieRegistry;
import com.littlewool.tech.insight.rpc.retry.FailOverRetryPolicy;
import com.littlewool.tech.insight.rpc.retry.ForkingRetryPolicy;
import com.littlewool.tech.insight.rpc.retry.RetryContext;
import com.littlewool.tech.insight.rpc.retry.RetryPolicy;
import com.littlewool.tech.insight.rpc.retry.RetrySame;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @ClassName: ConsumerProxyFactory
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/13 17:32
 * @Version: 1.0
 **/
@Slf4j
public class ConsumerProxyFactory {

    private Map<Integer, CompletableFuture<Response>> inFlightRequestTable;

    private final ConnectionManager connectionManager;

    private final ServieRegistry servieRegistry;

    private final ConsumerProperties consumerProperties;

    private final HashedWheelTimer timeoutTimer;


    public ConsumerProxyFactory(ConsumerProperties consumerProperties) throws Exception {
        this.consumerProperties = consumerProperties;
        this.servieRegistry = new DefaultServiceRegistry();
        this.servieRegistry.init(consumerProperties.getRegistryConfig());
        connectionManager = new ConnectionManager(createBootstrap(consumerProperties));
        inFlightRequestTable = new ConcurrentHashMap<>();
        timeoutTimer = new HashedWheelTimer(1, TimeUnit.SECONDS, 64);
    }

    @SuppressWarnings("unchecked")
    public <I> I createConsumerProxy(Class<I> interfaceClass) {
        return (I) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{interfaceClass},
                new ConsumerInvocationHandler(interfaceClass, createLoadBalancer(), createRetryPolicy()));
    }

    private RetryPolicy createRetryPolicy() {
        switch (consumerProperties.getRetryPolicy()){
            case "retrySame":return new RetrySame();
            case "failover":return new FailOverRetryPolicy();
            case "forking":return new ForkingRetryPolicy();
        }
        throw new IllegalArgumentException("没有这个重试策略"+consumerProperties.getRetryPolicy());
    }

    private LoadBalancer createLoadBalancer() {
        switch (this.consumerProperties.getLoadBalancePolicy()) {
            case "robin":
                return new RoundRobinLoadBalancer();
            case "random":
                return new RandomLoadBalancer();
            default:
                throw new IllegalArgumentException(this.consumerProperties.getLoadBalancePolicy() + "负载均衡不支持");
        }
    }

    private class ConsumerInvocationHandler implements InvocationHandler {

        private Class<?> interfaceClass;

        private final LoadBalancer loadBalancer;

        private final RetryPolicy retryPolicy;


        public ConsumerInvocationHandler(Class<?> interfaceClass, LoadBalancer loadBalancer, RetryPolicy retryPolicy) {
            this.interfaceClass = interfaceClass;
            this.loadBalancer = loadBalancer;
            this.retryPolicy = retryPolicy;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Object.class) {
                return invokeObjectMethod(proxy, method, args);
            }

            long startTime = System.currentTimeMillis();
            List<ServiceMetadata> serviceMetadata = servieRegistry.fetchServiceList(interfaceClass.getName());
            if (serviceMetadata.isEmpty()) {
                throw new RpcException(interfaceClass.getName() + "没有对应的provider");
            }

            ServiceMetadata providerMetadata = loadBalancer.select(serviceMetadata);
            Request request = buildRequest(method, args);
            Response response;
            try {
                CompletableFuture<Response> requestFuture = callRpcAsync(request, providerMetadata);
                response = requestFuture.get(consumerProperties.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                //重试
                long timeRemaining = consumerProperties.getMethodTimeoutMs() - (System.currentTimeMillis() - startTime);
                if (timeRemaining<=0){
                    throw new TimeoutException();
                }
                log.warn("超时了,进行重试");
                RetryContext retryContext = new RetryContext();
                retryContext.setFailedService(providerMetadata);
                retryContext.setServiceMetadataList(serviceMetadata);
                retryContext.setMethodTimeoutMs(timeRemaining);
                retryContext.setLoadBalancer(this.loadBalancer);
                retryContext.setRequestTimeoutMs(consumerProperties.getRequestTimeoutMs());
                //需要重新buildrequest
                retryContext.setDoRpcFunction(provider -> callRpcAsync(buildRequest(method, args), provider));
                response = this.retryPolicy.retry(retryContext);
            }
            return processResponse(response);

        }

        private static Object processResponse(Response response) {
            if (response.getCode() == 200) {
                return response.getResult();
            }
            throw new RpcException(response.getErrorMessage());
        }

        private Request buildRequest(Method method, Object[] args) {
            Request request = new Request();
            request.setServiceName(interfaceClass.getName());
            request.setMethodName(method.getName());
            request.setParamClass(method.getParameterTypes());
            request.setParams(args);
            return request;
        }

        private CompletableFuture<Response> callRpcAsync(Request request, ServiceMetadata provider) {
            CompletableFuture<Response> responseFuture = new CompletableFuture<>();
            Channel channel = connectionManager.getChannel(provider.getHost(), provider.getPort());

            if (null == channel) {
                responseFuture.completeExceptionally(new RpcException("provider 连接失败"));
                return responseFuture;
            }
            inFlightRequestTable.put(request.getRequestId(), responseFuture);
            //定时给请求进行异常结束 也就是超时 方便超时时移除request
            Timeout timeout =
                    timeoutTimer.newTimeout((t) -> responseFuture.completeExceptionally(new TimeoutException()),
                            consumerProperties.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
            //正常或者异常结束时候移除,但超时时候自身实际上并不会触发异常所以需要定时任务
            responseFuture.whenComplete((r, e) -> {
                inFlightRequestTable.remove(request.getRequestId());
                timeout.cancel();
            });

            //先放入，防止调用过快，还未将request放入map
            channel.writeAndFlush(request).addListener(f -> {
                log.info("发送了request {}",request.getRequestId());
                if (!f.isSuccess()) {
                    //发送失败直接结束，不再继续等待3秒
                    responseFuture.completeExceptionally(f.cause());
                }
            });
            return responseFuture;
        }

        private Object invokeObjectMethod(Object proxy, Method method, Object[] args) {
            if (method.getName().equals("toString")) {
                return "LittleWool Proxy Consumer " + interfaceClass.getName();
            }
            if (method.getName().equals("equals")) {
                return proxy == args[0];
            }
            if (method.getName().equals("hashCOde")) {
                return System.identityHashCode(proxy);
            }
            throw new UnsupportedOperationException("代理对象不支持该函数" + method.getName());
        }
    }

    private Bootstrap createBootstrap(ConsumerProperties consumerProperties) {
        Bootstrap bootstrap = new Bootstrap();
        return bootstrap.group(new NioEventLoopGroup(consumerProperties.getWorkThreadNum()))
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, consumerProperties.getConnectTimeoutMs())
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
                        nioSocketChannel.pipeline()
                                .addLast(new LWDecoder())
                                .addLast(new RequestEncoder())
                                .addLast(new ConsumerHandler());
                    }
                });
    }

    private class ConsumerHandler extends SimpleChannelInboundHandler<Response> {

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Response response) throws Exception {
            CompletableFuture<Response> requestFuture =
                    inFlightRequestTable.remove(response.getRequestId());
            if (null == requestFuture) {
                log.warn("requstId {} 找不到", response.getRequestId());
                return;
            }
            requestFuture.complete(response);
        }

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
    }
}
