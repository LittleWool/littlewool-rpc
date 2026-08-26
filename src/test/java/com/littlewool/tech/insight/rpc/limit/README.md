# limit tests

限流测试和 benchmark 目录。

## 主要测试

- `LimiterTest`: 基础限流算法行为测试。
- `DistributedLimiterOptimizationTest`: 分布式限流、Redis 配置、fail-closed 和路由行为测试。
- `DistributedLimiterBenchmarkTest`: 限流方案阶段 benchmark。

## benchmark 关注指标

- `allowed`: 放行请求数。
- `rejected`: 被限流或 Redis 超时拒绝的请求数。
- `centerCalls`: 访问中心存储的次数。
- `maxCallsForOneKey`: 单个中心 key 承受的最大访问次数。
- `finalBatchSize`: 动态 step 最终批量取号大小。

当前压测结论是：高 QPS 场景优先使用本地 token 池 + 批量取号；Redis 分片作为单 Redis 扛不住后的扩展能力。
