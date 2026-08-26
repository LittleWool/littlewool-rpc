# exception

RPC 框架自定义异常。

## 主要文件

- `RpcException`: 通用 RPC 异常。
- `LimitException`: 限流拒绝时使用的异常。

自定义异常用于把框架内部错误转换成更明确的调用语义，避免上层直接依赖 Netty、Redis、序列化库等底层异常。
