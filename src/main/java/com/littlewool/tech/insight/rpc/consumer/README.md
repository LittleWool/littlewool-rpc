# consumer

消费者侧核心模块。

## 职责

消费者侧负责把本地接口调用转换成远程 RPC 调用，并处理连接、超时、重试、熔断、降级和限流。

## 主要文件

- `ConsumerApp`: 消费者示例启动入口。
- `ConsumerProperties`: 消费者配置，例如连接超时、请求超时、限流阈值。
- `ConsumerProxyFactory`: 生成接口代理，把方法调用封装成 `Request`。
- `ConnectionManager`: 管理 Netty channel 和服务节点连接。
- `InFlightRequestManager`: 管理已经发出但尚未收到响应的请求。
- `GenericConsumer`: 通用消费者入口。

## 调用流程

1. 业务代码调用接口代理。
2. `ConsumerProxyFactory` 生成 `Request`。
3. 注册中心返回服务实例列表。
4. 负载均衡选出目标实例。
5. `ConnectionManager` 获取或建立连接。
6. `InFlightRequestManager` 登记请求并设置超时。
7. Netty pipeline 编码并发送请求。
8. 收到响应后完成对应 `CompletableFuture`。

如果发生失败，代理层会结合 `retry`、`breaker` 和 `fallback` 决定是否重试、熔断或降级。
