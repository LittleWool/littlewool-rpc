package com.littlewool.tech.insight.rpc.serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @ClassName: SerizalizerManager
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 9:20
 * @Version: 1.0
 **/

public class SerizalizerManager {
    private Map<Integer, Serializer> codeMap = new HashMap<>();
    private Map<String, Serializer> nameMap = new HashMap<>();

    public SerizalizerManager() {
        init();
    }

    public Serializer getSerializer(int typeCode) {
        return codeMap.get(typeCode);
    }

    public Serializer getSerializer(String name) {
        return nameMap.get(name.toUpperCase());
    }

    private void init() {
        ServiceLoader<Serializer> serializers = ServiceLoader.load(Serializer.class);
        for (Serializer serializer : serializers) {
            if (serializer.code() >= 16) {
                throw new IllegalArgumentException("输入的序列化器编码不能超过15");
            }
            if (null!=codeMap.put(serializer.code(), serializer)) {
                throw new IllegalArgumentException("序列化器code重复");
            }
            if (null!=nameMap.put(serializer.getName().toUpperCase(), serializer)){
                throw new IllegalArgumentException("序列化器名称重复");
            }
        }
    }
}
