# services

SPI 服务声明目录。

每个文件名对应一个扩展接口的全限定类名，文件内容是一行或多行实现类的全限定类名。

当前包含：

- `com.littlewool.tech.insight.rpc.compress.Compression`
- `com.littlewool.tech.insight.rpc.retry.RetryPolicy`
- `com.littlewool.tech.insight.rpc.serializer.Serializer`

新增扩展实现时，除了写 Java 类，还要把实现类写入对应 service 文件，否则运行时无法自动发现。
