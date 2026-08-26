# metrics

指标模块。

## 主要文件

- `RpcCallMetrics`: 记录单次 RPC 调用的耗时、开始时间、结束时间和状态。

指标当前主要服务于重试、熔断和调用链路观察。后续可以接入日志、Prometheus、Micrometer 或自定义监控上报。
