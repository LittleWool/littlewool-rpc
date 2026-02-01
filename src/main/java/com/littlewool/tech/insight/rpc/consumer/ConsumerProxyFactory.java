package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.breaker.CirCuitBreaker;
import com.littlewool.tech.insight.rpc.breaker.CircuitBreakerManager;
import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.fallback.CacheFallback;
import com.littlewool.tech.insight.rpc.fallback.DefaultFallBack;
import com.littlewool.tech.insight.rpc.fallback.Fallback;
import com.littlewool.tech.insight.rpc.fallback.MockFallback;
import com.littlewool.tech.insight.rpc.loadbalance.LoadBalancer;
import com.littlewool.tech.insight.rpc.loadbalance.RandomLoadBalancer;
import com.littlewool.tech.insight.rpc.loadbalance.RoundRobinLoadBalancer;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;
import com.littlewool.tech.insight.rpc.register.DefaultServiceRegistry;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import com.littlewool.tech.insight.rpc.register.ServieRegistry;
import com.littlewool.tech.insight.rpc.retry.FailOverRetryPolicy;
import com.littlewool.tech.insight.rpc.retry.ForkingRetryPolicy;
import com.littlewool.tech.insight.rpc.retry.RetryContext;
import com.littlewool.tech.insight.rpc.retry.RetryPolicy;
import com.littlewool.tech.insight.rpc.retry.RetryPolicyManager;
import com.littlewool.tech.insight.rpc.retry.RetrySame;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @ClassName: ConsumerProxyFactory
 * @Description: 动态代理生成consumer,
 * @Author: LittleWool
 * @Date: 2026/1/13 17:32
 * @Version: 1.0
 **/
@Slf4j
public class ConsumerProxyFactory {

    private final ConnectionManager connectionManager;

    private final ServieRegistry servieRegistry;

    private final ConsumerProperties consumerProperties;

    private final InFlightRequestManager inFlightRequestManager;

    private final CircuitBreakerManager circuitBreakerManager;

    private final Fallback fallback;

    private final RetryPolicyManager retryPolicyManager;

    public ConsumerProxyFactory(ConsumerProperties consumerProperties) throws Exception {
        this.servieRegistry = new DefaultServiceRegistry();
        this.consumerProperties = consumerProperties;
        this.servieRegistry.init(consumerProperties.getRegistryConfig());
        this.inFlightRequestManager = new InFlightRequestManager(consumerProperties);
        this.circuitBreakerManager = new CircuitBreakerManager(consumerProperties);
        this.fallback = new DefaultFallBack(new CacheFallback(), new MockFallback());
        this.retryPolicyManager=new RetryPolicyManager();
        connectionManager = new ConnectionManager(inFlightRequestManager, consumerProperties);
    }

    @SuppressWarnings("unchecked")
    public <I> I createConsumerProxy(Class<I> interfaceClass) {
        return (I)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {interfaceClass},
            new ConsumerInvocationHandler(interfaceClass, createLoadBalancer(), createRetryPolicy(consumerProperties.getRetryPolicy())));
    }

    private RetryPolicy createRetryPolicy(String retryPolicyName) {
        RetryPolicy retryPolicy = retryPolicyManager.getRetryPolicy(retryPolicyName);
        if(null==retryPolicy){
            throw new IllegalArgumentException("没有对应的重试策略"+retryPolicyName);
        }
        return retryPolicy;
    }

    private LoadBalancer createLoadBalancer() {
        return switch (this.consumerProperties.getLoadBalancePolicy()) {
            case "robin" -> new RoundRobinLoadBalancer();
            case "random" -> new RandomLoadBalancer();
            default ->
                    throw new IllegalArgumentException(this.consumerProperties.getLoadBalancePolicy() + "负载均衡不支持");
        };
    }

    private class ConsumerInvocationHandler implements InvocationHandler {

        private final Class<?>  interfaceClass;

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
            //注册中心查询服务
            List<ServiceMetadata> serviceMetadata =
                new ArrayList<>(servieRegistry.fetchServiceList(interfaceClass.getName()));
            //负载均衡和熔断选择具体的provider
            ServiceMetadata provider = decideProvider(serviceMetadata);
            System.out.println("发送请求前"+System.currentTimeMillis());

            //本次请求的参数和统计信息
            RpcCallMetrics metrics = RpcCallMetrics.createRpcMetrics(method, args, provider);
            if (null == provider) {
                // 降级
                return fallback.fallback(metrics);
            }

            //构建请求
            Request request = buildRequest(method, args);

            //或者即将请求的provider的熔断器
            CirCuitBreaker breaker = circuitBreakerManager.createOrGetBreaker(provider);

            try {
                //经过消费端限流，放入在途请求。获取对应连接 然后发送请求
                System.out.println("发送请求前"+System.currentTimeMillis());

                CompletableFuture<Response> requestFuture = callRpcAsync(request, provider);
                System.out.println("发送请求后"+System.currentTimeMillis());
                Response response = requestFuture.get(consumerProperties.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
                System.out.println("响应后"+System.currentTimeMillis());
                metrics.doComplete(response);
                breaker.recordRpc(metrics);
                fallback.recordMetrics(metrics);
                return processResponse(response);
            } catch (Exception e) {
                //请求发送失败(超时，获取连接失败，限流 会捕获异常进入这里)
                //先记录本次异常的请求数据
                metrics.errorComplete(e);
                breaker.recordRpc(metrics);
            }
            try {
                //进行重试
                return processResponse(doRetry(metrics, serviceMetadata));
            } catch (Exception e) {
                //重试失败则降级
                return fallback.fallback(metrics);
            }
        }

        /***
         * 使用负载均衡和熔断选择provider
         * @param candidate
         * @return
         */
        private ServiceMetadata decideProvider(List<ServiceMetadata> candidate) {
            while (!candidate.isEmpty()) {
                ServiceMetadata select = this.loadBalancer.select(candidate);
                CirCuitBreaker breaker = circuitBreakerManager.createOrGetBreaker(select);
                if (breaker.allowRequest()) {
                    return select;
                }
                //这里直接修改传入列表,减少后续工作量
                candidate.remove(select);
            }
            return null;
        }

        private Response doRetry(RpcCallMetrics metrics, List<ServiceMetadata> serviceMetadata) throws Exception {
            Throwable e = metrics.getThrowable();
            //completeException异常结束之后,异常会用ExecutionException装着
            if (e instanceof ExecutionException ee && ee.getCause() instanceof RpcException rpcException
                && !rpcException.retry()) {
                //被限流之后是不应该重试的
                throw rpcException;
            }
            // 重试
            long timeRemaining = consumerProperties.getMethodTimeoutMs() - metrics.getDuration();
            if (timeRemaining <= 0) {
                throw new TimeoutException();
            }
            log.warn("rpc出现了异常,进行重试", e);
            RetryContext retryContext = createRetryContextFromFailMetrics(metrics, serviceMetadata, timeRemaining);

            return this.retryPolicy.retry(retryContext);
        }


        private RetryContext createRetryContextFromFailMetrics(RpcCallMetrics metrics,
            List<ServiceMetadata> serviceMetadata, long timeRemaining) {
            RetryContext retryContext = new RetryContext();

            retryContext.setFailedService(metrics.getProvider());
            retryContext.setServiceMetadataList(serviceMetadata);
            retryContext.setMethodTimeoutMs(timeRemaining);
            retryContext.setLoadBalancer(this.loadBalancer);
            retryContext.setRequestTimeoutMs(consumerProperties.getRequestTimeoutMs());


            //把重试和 远程调用二者解耦
            retryContext.setDoRpcFunction(provider -> {
                CirCuitBreaker breaker = circuitBreakerManager.createOrGetBreaker(provider);
                if (!breaker.allowRequest()) {
                    CompletableFuture<Response> breakerFuture = new CompletableFuture<>();
                    breakerFuture.completeExceptionally(new RpcException("provider熔断"));
                    return breakerFuture;
                }
                //统计请求信息
                RpcCallMetrics retryMetrics =
                    RpcCallMetrics.createRpcMetrics(metrics.getMethod(), metrics.getParams(), provider);
                CompletableFuture<Response> retryFuture =
                    callRpcAsync (buildRequest(metrics.getMethod(), metrics.getParams()), provider);
                retryFuture.whenComplete((r, retryE) -> {
                    if (null == retryE) {
                        retryMetrics.doComplete(r);
                    } else {
                        retryMetrics.errorComplete(retryE);
                    }
                    breaker.recordRpc(retryMetrics);
                });
                return retryFuture;
            });
            return retryContext;
        }

        private CompletableFuture<Response> callRpcAsync(Request request, ServiceMetadata provider) {
            //TODO 如果 在在途请求管理器限流 是不是该直接返回,而不是继续访问

            CompletableFuture<Response> responseFuture =
                inFlightRequestManager.inFlightRequest(request, consumerProperties.getRequestTimeoutMs(), provider);
            if (responseFuture.isCompletedExceptionally()){
                return responseFuture;
            }

            Channel channel = connectionManager.getChannel(provider);


            if (null == channel) {
                //没有provider直接快速失败
                responseFuture.completeExceptionally(new RpcException("provider 连接失败"));
                return responseFuture;
            }

            // 先放入，防止调用过快，还未将request放入map4
            channel.writeAndFlush(request).addListener(f -> {
                log.info("发送了request {}", request.getRequestId());
                if (!f.isSuccess()) {
                    log.info("request {}发送失败", request.getRequestId());
                    // 发送失败直接结束，不再继续等待3秒
                    responseFuture.completeExceptionally(f.cause());
                }
            });
            return responseFuture;
        }

        private static Object processResponse(Response response) {
            log.info(response.toString());
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
            return switch (method.getName()) {
                case "toString" -> "LittleWool Proxy Consumer " + interfaceClass.getName();
                case "equals" -> proxy == args[0];
                case "hashCOde" -> System.identityHashCode(proxy);
                default -> throw new UnsupportedOperationException("代理对象不支持该函数" + method.getName());
            };
        }
    }

}
