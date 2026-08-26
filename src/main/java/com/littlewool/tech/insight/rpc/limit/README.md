# limit

限流模块。

## 设计目标

限流用于控制请求进入系统的速度，避免消费者、服务端、Redis 或下游服务被瞬时流量打穿。

## 通用接口

- `Limiter`: 所有限流器的统一接口。
- `tryAcquire()`: 尝试获取额度，成功返回 `true`，失败立即返回 `false`。
- `release(int permits)`: 对并发类限流器释放额度；时间窗口和 token bucket 通常不需要释放。

## 单机算法

- `FixedWindowLimiter`: 固定窗口限流，实现简单，但窗口边界可能有突刺。
- `SlidingWindowLimiter`: 滑动窗口限流，边界更平滑，维护成本略高。
- `TokenBucketLimiter`: 令牌桶，允许一定突发流量。
- `LeakyBucketLimiter`: 漏桶，按稳定速率流出，适合削峰。
- `ConcurrencyLimiter`: 并发数限制，控制同时进行中的请求数量。
- `BucketLimiter`: 旧的事件驱动 token 方案，保留用于对比，不推荐作为新入口。

## 部署形态

- `SingleInstanceLimiter`: 单实例本地限流，每个实例独立计数。
- `SimpleClusterLimiter`: 简单集群限流，把总额度按实例数均分到每台机器。
- `DistributedLimiter`: 基础分布式限流，每个请求访问中心存储。
- `BatchTokenDistributedLimiter`: 本地 token 池 + 批量取号，显著减少 Redis 访问。
- `ShardedDistributedLimiter`: 多 shard key 分片，降低单 key 热点。
- `HighQpsDistributedLimiter`: 高 QPS 方案，支持本地 token 池、动态 step 和可选分片。

## 分布式存储

- `DistributedLimitStore`: 中心存储接口。
- `InMemoryDistributedLimitStore`: 内存实现，用于单测和 benchmark。
- `RedisDistributedLimitStore`: Redis/Lua 实现，默认短超时：
  - connect timeout: `50ms`
  - socket timeout: `30ms`
  - pool max wait: `10ms`
  - Redis 连接失败或超时按拒绝处理。
- `ShardedRedisDistributedLimitStore`: 多 Redis 节点路由，按 `:shard:N` 均匀分发到不同 Redis。

## 当前推荐

普通场景优先使用：

```text
DistributedLimiter + RedisDistributedLimitStore
```

高 QPS 热点接口优先使用：

```text
BatchTokenDistributedLimiter + RedisDistributedLimitStore
```

如果需要动态调整批量大小：

```text
HighQpsDistributedLimiter(shardCount = 1) + RedisDistributedLimitStore
```

只有当单 Redis 仍然接近瓶颈，或同一限流 key 极热时，再上：

```text
HighQpsDistributedLimiter + ShardedRedisDistributedLimitStore
```

## 压测结论

百万请求、64 并发、超时即拒绝的本机 Redis 测试中，单 Redis + 批量 token 池已经能把 Redis 调用从 100 万次降到约 2000 次，并把拒绝降为 0。分片主要是扩展能力，不应作为默认复杂度。
