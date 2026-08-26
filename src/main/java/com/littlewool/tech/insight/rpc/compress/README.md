# compress

压缩模块。

## 职责

对 RPC 请求或响应体进行可选压缩，降低网络传输体积。

## 主要文件

- `Compression`: 压缩扩展接口。
- `CompressionManager`: 压缩算法管理器，通过 SPI 加载可用实现。
- `GzipCompression`: Gzip 压缩实现。
- `NullCompression`: 空压缩实现，表示不压缩。

## 使用方式

编解码器根据消息头里的压缩类型选择对应 `Compression` 实现。新增压缩算法时，需要：

1. 实现 `Compression`。
2. 在 `META-INF/services/com.littlewool.tech.insight.rpc.compress.Compression` 中登记实现类。
3. 补充编解码测试。
