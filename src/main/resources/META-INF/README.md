# META-INF

Java 标准元信息目录。

当前主要用于放置 `services` 子目录，给 Java SPI 机制声明扩展实现。

打包成 jar 后，`META-INF/services/*` 会被 `ServiceLoader` 或项目内扩展加载逻辑读取。
