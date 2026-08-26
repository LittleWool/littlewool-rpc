# rpc

RPC 框架主包。

## 包结构

- `api`: 示例服务接口和请求/响应对象。
- `consumer`: 消费者侧代理、连接管理、请求飞行队列。
- `provider`: 服务端启动、请求分发、服务实现注册。
- `message`: RPC 协议消息模型。
- `codec`: Netty 编解码器。
- `serializer`: 序列化扩展。
- `compress`: 压缩扩展。
- `register`: 服务注册与发现。
- `loadbalance`: 服务节点选择策略。
- `retry`: 调用失败后的重试策略。
- `breaker`: 熔断器。
- `fallback`: 降级策略。
- `limit`: 单机、集群、分布式和高 QPS 限流。
- `handler`: Netty 通用处理器。
- `metrics`: 调用耗时和状态统计。
- `exception`: RPC 框架自定义异常。
- `spi`: 扩展点注解。
- `version`: 框架版本信息。

## 推荐阅读顺序

先看 `message` 和 `codec` 理解协议结构，再看 `consumer` / `provider` 理解端到端调用，最后看 `retry`、`breaker`、`fallback`、`limit` 这些治理模块。
