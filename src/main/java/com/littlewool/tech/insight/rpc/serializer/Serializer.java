package com.littlewool.tech.insight.rpc.serializer;

import com.littlewool.tech.insight.rpc.spi.Extension;

/**
 * @ClassName: Serializer
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 9:03
 * @Version: 1.0
 **/

public interface Serializer extends Extension {
    byte[] serialize(Object object);

    <T> T deserialize(byte[] bytes, Class<T> objectClass);

}
