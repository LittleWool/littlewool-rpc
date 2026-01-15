package com.littlewool.tech.insight.rpc.fallback;

import com.littlewool.tech.insight.rpc.exception.RpcException;
import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: MockFallback
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 20:30
 * @Version: 1.0
 **/

public class MockFallback implements Fallback {

    private Map<Class<?>, Object> mockObjectCache = new ConcurrentHashMap<>();

    @Override
    public Object fallback(RpcCallMetrics metrics) throws Exception {
        Method method = metrics.getMethod();
        RpcFallback annotation = method.getDeclaringClass().getAnnotation(RpcFallback.class);
        if (null == annotation) {
            throw new RpcException("属实是没招了!");
        }
        Class<?> methodClass = annotation.value();
        if (!method.getDeclaringClass().isAssignableFrom(methodClass)) {
            throw new RpcException(String.format("你调用了%s,但降级策略是%s", method, methodClass));
        }
        Object mockObj = mockObjectCache.computeIfAbsent(methodClass, this::createMockObject);
        return method.invoke(mockObj, metrics.getParams());


    }

    private Object createMockObject(Class<?> methodClass) {
        try {
            return methodClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new RpcException("创建mock对象失败", e);
        }

    }
}
