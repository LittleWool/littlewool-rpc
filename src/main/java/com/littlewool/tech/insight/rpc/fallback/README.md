# fallback

降级模块。

## 职责

当远程调用失败、超时、熔断或限流时，消费者可以执行本地 fallback，返回可控结果。

## 主要文件

- `Fallback`: 降级接口。
- `RpcFallback`: 标注接口对应的降级实现。
- `DefaultFallBack`: 默认降级策略。
- `MockFallback`: Mock 降级策略。
- `CacheFallback`: 缓存兜底策略。

## 使用方式

示例接口 `api.Add` 使用 `@RpcFallback(ConsumerAddImpl.class)` 声明 fallback。消费者代理在异常路径上解析注解并执行对应实现。

降级实现应该保持快速、无副作用，并避免再次依赖同一个已经失败的远程服务。
