# resources

运行时资源目录。

## 主要内容

- `logback.xml`: 日志配置。
- `META-INF/services`: Java SPI 服务声明。

这里的文件会被打包进 jar，并在运行时通过 classpath 读取。
