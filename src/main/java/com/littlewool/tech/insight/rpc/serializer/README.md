# serializer

序列化模块。

## 职责

把 Java 对象转换为二进制，或从二进制恢复为 Java 对象。

## 主要文件

- `Serializer`: 序列化接口。
- `SerizalizerManager`: 序列化实现管理器。当前类名里 `Serizalizer` 是历史拼写。
- `JsonSerializer`: JSON 序列化实现。
- `HessianSerializer`: Hessian 二进制序列化实现。

## 扩展方式

新增序列化算法时：

1. 实现 `Serializer`。
2. 在 `META-INF/services/com.littlewool.tech.insight.rpc.serializer.Serializer` 中登记实现类。
3. 更新编解码测试，确保请求和响应都能往返。
