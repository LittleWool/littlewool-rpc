package com.littlewool.tech.insight.rpc.compress;

/**
 * @ClassName: NullCompression
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 10:16
 * @Version: 1.0
 **/

public class NullCompression implements Compression{
    @Override
    public byte[] compress(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] decompress(byte[] bytes) {
        return bytes;
    }

    @Override
    public String getName() {
        return "null";
    }

    @Override
    public int code() {
        return 0;
    }
}
