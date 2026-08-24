package com.littlewool.tech.insight.rpc.compress;

import com.littlewool.tech.insight.rpc.exception.RpcException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @ClassName: GzipCompression
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 10:16
 * @Version: 1.0
 **/

public class GzipCompression implements Compression {
    @Override
    public byte[] compress(byte[] bytes) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bos)) {
            gzipOutputStream.write(bytes);
            gzipOutputStream.flush();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RpcException("消息压缩失败",e);
        }
    }

    @Override
    public byte[] decompress(byte[] bytes) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
            byte[] buffer = new byte[4096];
            int len;
            while ((len = gzipInputStream.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RpcException("消息解压缩失败",e);
        }
    }

    @Override
    public String getName() {
        return "gzip";
    }

    @Override
    public int code() {
        return 1;
    }
}
