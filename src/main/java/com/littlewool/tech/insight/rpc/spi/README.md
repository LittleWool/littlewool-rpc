# spi

SPI 扩展点模块。

## 主要文件

- `Spi`: 标记一个接口是 SPI 扩展点。
- `Extension`: 标记具体扩展实现的名称或元信息。

项目里压缩、序列化、重试等模块都可以通过 SPI 增加实现。SPI 声明文件位于 `src/main/resources/META-INF/services`。
