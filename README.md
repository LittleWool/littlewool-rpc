# littlewool-rpc

`littlewool-rpc` 是一个轻量级 RPC 框架实验项目，覆盖消费者代理、服务端处理、编解码、序列化、压缩、注册发现、重试、熔断、限流和降级等核心链路。

## 核心调用链路

一次普通 RPC 调用大致经过这些模块：

1. `consumer` 生成接口代理，把本地方法调用转换成 RPC `Request`。
2. `register` 提供服务发现，消费者选择一个可用服务节点。
3. `loadbalance` 在多个服务节点之间选择目标实例。
4. `codec` 把 `Request` / `Response` 编码成网络字节流。
5. `serializer` 把 Java 对象序列化为二进制载荷。
6. `compress` 在需要时压缩请求或响应体。
7. `provider` 接收请求，执行本地服务实现，并写回 `Response`。
8. `retry`、`breaker`、`fallback` 在失败、超时或熔断时提供容错策略。
9. `limit` 在消费者侧和服务端侧控制并发量和请求速率。

## 重点能力

- 多种本地限流算法：固定窗口、滑动窗口、令牌桶、漏桶、并发数限制。
- 分布式限流：Redis/Lua 固定窗口、Redis/Lua token bucket 批量取号。
- 高 QPS 限流优化：本地 token 池、批量取号、动态 step、多 Redis 分片扩展。
- 基础容错：重试、熔断、降级、超时控制。
- SPI 扩展：序列化、压缩、重试策略等组件可通过 SPI 注册。

## 构建和测试

项目当前按 Java 11 编译：

```bash
mvn test
```

限流相关 benchmark 在 `src/test/java/com/littlewool/tech/insight/rpc/limit` 下，可以通过系统参数调整请求次数：

```bash
mvn -Dtest=DistributedLimiterBenchmarkTest -Dlimit.benchmark.attempts=1000000 test
```

## 目录说明

- `src/main/java/com/littlewool/tech/insight/rpc`: RPC 框架主代码。
- `src/main/resources`: SPI 声明和日志配置。
- `src/test/java`: 单元测试、限流 benchmark 和基础回归测试。
