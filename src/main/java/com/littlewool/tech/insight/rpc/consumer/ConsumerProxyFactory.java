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
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
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

    private Map<Integer, CompletableFuture<Response>> inFlightRequestTable;

    private final ConnectionManager connectionManager;

    private final ServieRegistry servieRegistry;

    private final ConsumerProperties consumerProperties;

    public ConsumerProxyFactory(ConsumerProperties consumerProperties) throws Exception {

        this.servieRegistry = new DefaultServiceRegistry();
        this.servieRegistry.init(consumerProperties.getRegistryConfig());
        connectionManager = new ConnectionManager(createBootstrap(consumerProperties));
        inFlightRequestTable = new ConcurrentHashMap<>();
        this.consumerProperties = consumerProperties;
    }

    @SuppressWarnings("unchecked")
    public <I> I createConsumerProxy(Class<I> interfaceClass) {
        return (I) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{interfaceClass},
                new ConsumerInvocationHandler(interfaceClass,createLoadBalanmcer()));
    }
    private LoadBalancer createLoadBalanmcer(){
        switch (this.consumerProperties.getLoadBalancePolicy()){
            case "robin":
                return new RoundRobinLoadBalancer();
            case "random":
                return new RandomLoadBalancer();
            default:
                throw new IllegalArgumentException(this.consumerProperties.getLoadBalancePolicy()+"负载均衡不支持");
        }
    }

    private class ConsumerInvocationHandler implements InvocationHandler {

        private Class<?> interfaceClass;

        final LoadBalancer loadBalancer;

        public ConsumerInvocationHandler(Class<?> interfaceClass, LoadBalancer loadBalancer) {
            this.interfaceClass = interfaceClass;
            this.loadBalancer = loadBalancer;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Object.class) {
                return invokeObjectMethod(proxy, method, args);
            }
            CompletableFuture<Response> responseFuture = new CompletableFuture<>();

            try {
                List<ServiceMetadata> serviceMetadata = servieRegistry.fetchServiceList(interfaceClass.getName());
                if (serviceMetadata.isEmpty()) {
                    throw new RpcException(interfaceClass.getName() + "没有对应的provider");
                }

                ServiceMetadata providerMetadata = loadBalancer.select(serviceMetadata);

                Channel channel = connectionManager.getChannel(providerMetadata.getHost(), providerMetadata.getPort());
                if (null == channel) {
                    throw new RuntimeException("provider 连接失败");
                }
                Request request = buildRequest(method, args);
                inFlightRequestTable.put(request.getRequestId(), responseFuture);
                //先放入，防止调用过快，还未将request放入map
                channel.writeAndFlush(request).addListener(f -> {
                    if (!f.isSuccess()) {
                        inFlightRequestTable.remove(request.getRequestId());
                        //发送失败直接结束，不再继续等待3秒
                        responseFuture.completeExceptionally(f.cause());
                    }
                });
                Response response = responseFuture.get(consumerProperties.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
                return processResponse(response);

            } catch (RpcException rpcException) {
                throw rpcException;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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
