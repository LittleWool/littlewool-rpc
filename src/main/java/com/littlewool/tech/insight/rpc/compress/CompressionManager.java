package com.littlewool.tech.insight.rpc.compress;

import com.littlewool.tech.insight.rpc.serializer.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @ClassName: CompressManager
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 10:22
 * @Version: 1.0
 **/

public class CompressionManager {
    private Map<Integer, Compression> codeMap =new HashMap<>();
    private Map<String, Compression> nameMap =new HashMap<>();

    public CompressionManager() {
        init();
    }
    public Compression getCompression(int typeCode){
        return codeMap.get(typeCode);
    }

    public Compression getCompression(String name) {
        return nameMap.get(name.toUpperCase());
    }
    private void init(){
        ServiceLoader<Compression> compressions = ServiceLoader.load(Compression.class);
        for (Compression compression : compressions) {
            if (compression.code() >= 16) {
                throw new IllegalArgumentException("输入的压缩器编码不能超过15");
            }
            if (null!=codeMap.put(compression.code(), compression)) {
                throw new IllegalArgumentException("压缩器code重复");
            }
            if (null!=nameMap.put(compression.getName().toUpperCase(), compression)){
                throw new IllegalArgumentException("压缩器名称重复");
            }
        }
    }
}
