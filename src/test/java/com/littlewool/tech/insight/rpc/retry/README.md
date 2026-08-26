# retry tests

重试策略测试目录。

当前主要验证重试策略在请求失败、超时和剩余时间不足时的行为。

测试重点：

- 不突破 `requestTimeoutMs` 和 `methodTimeoutMs`。
- 失败转移策略是否能切换服务实例。
- 并发重试是否能返回最快成功结果。
