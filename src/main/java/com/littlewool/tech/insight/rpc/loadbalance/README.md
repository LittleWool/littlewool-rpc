# loadbalance

负载均衡模块。

## 职责

当注册中心返回多个服务实例时，消费者需要选择一个目标节点发起请求。

## 主要文件

- `LoadBalancer`: 负载均衡接口。
- `RandomLoadBalancer`: 随机选择实例。
- `RoundRobinLoadBalancer`: 轮询选择实例。

后续可以扩展权重轮询、一致性哈希、最小连接数、响应时间加权等策略。
