package com.littlewool.tech.insight.rpc.consumer;

/**
 * @ClassName: GenericConsumer
 * @Description: 若是a调用b时候 中间需要经过网关，但网关不可能包含所有服务调用的接口，所以需要进行泛化调用
 * @Author: LittleWool
 * @Date: 2026/2/2 7:47
 * @Version: 1.0
 **/

public interface GenericConsumer {
    //中间调用的网关可能根本不包含参数的类型,所以使用字符串传递
    Object $invoke(String serviceName,String methodName,String[] paramsType,Object[] params);
}
