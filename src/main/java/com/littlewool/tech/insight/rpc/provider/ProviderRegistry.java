package com.littlewool.tech.insight.rpc.provider;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: ProviderRegistry
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 21:19
 * @Version: 1.0
 **/

public class ProviderRegistry {

    private final Map<String, Invocation> map = new ConcurrentHashMap<>();

    public <I> void register(Class<I> interfaceClass, I serviceInstance) {
        if(!interfaceClass.isInterface()){
            throw  new IllegalArgumentException("注册类型只能为接口");
        }
        if(map.putIfAbsent(interfaceClass.getName(),new Invocation(interfaceClass,serviceInstance))!=null){
            throw new IllegalArgumentException(interfaceClass.getName()+"重复注册!");
        }
    }

    public Invocation findService(String serviceName) {
        return map.get(serviceName);
    }

    public static class Invocation<I> {
        final I serviceInstance;
        final Class<?> intefaceClass;

        public Invocation(Class<I> interfaceClass, I serviceInstance) {
            this.serviceInstance = serviceInstance;
            this.intefaceClass=interfaceClass;
        }
        public Object invoke(String methodName,Class<?>[] paramsClass,Object[] params) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            //使用接口类来进行调用,就不会涉及实现类的私有方法
            Method invokeMethod= intefaceClass.getDeclaredMethod(methodName, paramsClass);
            return invokeMethod.invoke(serviceInstance,params);
        }
    }
    public List<String> allServiceName(){
        return new ArrayList<>(map.keySet());
    }
}
