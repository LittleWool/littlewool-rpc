# provider

服务端模块。

## 职责

Provider 负责启动服务端网络监听，注册本地服务实现，接收 RPC 请求，执行目标方法，并返回响应。

## 主要文件

- `ProviderApp`: 服务端示例启动入口。
- `ProviderProporties`: 服务端配置。
- `ProviderRegistry`: 本地服务实现注册表。
- `ProviderServer`: Netty 服务端，包含请求处理和限流逻辑。
- `AddImpl`: 示例服务实现。

## 请求处理流程

1. Netty 收到请求字节流。
2. `codec` 解码为 `Request`。
3. 服务端进行全局和连接级限流。
4. `ProviderRegistry` 找到目标服务实现。
5. 反射执行目标方法。
6. 构造 `Response` 并写回消费者。

服务端 handler 不应在 event loop 中执行耗时任务，生产化时可以把业务调用投递到独立线程池。
