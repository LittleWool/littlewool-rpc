# register

服务注册与发现模块。

## 职责

Provider 启动后把服务实例注册到注册中心；Consumer 调用前从注册中心拉取可用实例列表。

## 主要文件

- `ServieRegistry`: 注册中心接口。当前文件名里 `Servie` 是历史拼写，语义上是 `ServiceRegistry`。
- `DefaultServiceRegistry`: 本地内存注册中心，适合测试。
- `RedisServiceRegistry`: Redis 注册中心实现。
- `ZookeeperServiceRegistry`: ZooKeeper/Curator 注册中心实现。
- `RegistryConfig`: 注册中心配置。
- `ServiceMetadata`: 服务实例元数据，包括 host、port、服务名等信息。

## 注意点

注册中心返回的是服务发现结果，不负责真正的 RPC 调用。消费者还需要结合 `loadbalance` 选择一个实例，并通过 `consumer.ConnectionManager` 建立连接。
