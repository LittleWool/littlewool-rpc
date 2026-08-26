# src/main

生产代码入口目录。

## 子目录

- `java`: Java 源码，包含 RPC 框架的核心实现。
- `resources`: 运行时配置，包括 SPI 服务声明和日志配置。

这里的代码会被打包进最终 jar。测试辅助类和 benchmark 不应该放在这里。
