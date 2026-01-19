package com.littlewool.tech.insight.rpc.serializer;

/**
 * @ClassName: Serializer
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 9:03
 * @Version: 1.0
 **/

public interface Serializer {
    byte[] serialize(Object object);

    <T> T deserialize(byte[] bytes, Class<T> objectClass);

    enum SerizalizerType {
        JSON(0), HESSIAN(1);

        private final int typeCode;

        SerizalizerType(int typeCode) {
            this.typeCode = typeCode;
        }

        public int getTypeCode() {
            return typeCode;
        }
    }
}
