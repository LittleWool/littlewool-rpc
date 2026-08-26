# handler

Netty 通用处理器模块。

## 主要文件

- `HeartbeatHandler`: 心跳处理，用于连接保活和空闲检测。
- `TrafficRecordHandler`: 流量统计处理器，用于记录连接级别读写情况。

这些 handler 通常被放入消费者或服务端的 Netty pipeline 中。修改 handler 时要注意线程模型：业务逻辑不要长时间阻塞 event loop。
