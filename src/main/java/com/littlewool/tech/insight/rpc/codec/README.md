# codec

RPC 协议编解码模块。

## 职责

把 Java 层的 `Message`、`Request`、`Response` 转换成网络字节流，并在收到字节流后还原为消息对象。

## 主要文件

- `LWEncoder`: 通用消息编码器。
- `LWDecoder`: 通用消息解码器。
- `RequestEncoder`: 请求编码器。
- `ResponseEncoder`: 响应编码器。

## 和其他模块的关系

- 依赖 `message` 中的协议模型。
- 依赖 `serializer` 把对象 body 转换为二进制。
- 依赖 `compress` 对 payload 做可选压缩。
- 运行在 Netty pipeline 中，由 `consumer` 和 `provider` 两侧共同使用。

修改协议字段时，要同时检查编码、解码、测试用例和兼容性。
