# breaker tests

熔断器测试目录。

当前主要覆盖 `ResponseTimeCircuitBreaker` 的窗口统计和状态转换行为。

重点关注：

- 慢请求是否能触发熔断。
- 窗口过期后统计是否重置。
- 半开探测是否能恢复到正常状态。
