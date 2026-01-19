package com.littlewool.tech.insight.rpc.compress;

/**
 * @ClassName: Compression
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 10:15
 * @Version: 1.0
 **/

public interface Compression {
    byte[] compress(byte[] bytes);

    byte[] decompress(byte[] bytes);

    enum CompressionType {
        NONE(0),GZIP(1)
        ;

        private final int typeCode;

        CompressionType(int typeCode) {
            this.typeCode = typeCode;
        }

        public int getTypeCode() {
            return typeCode;
        }
    }
}
