package com.littlewool.tech.insight.rpc.serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: SerizalizerManager
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 9:20
 * @Version: 1.0
 **/

public class SerizalizerManager {
    private Map<Integer,Serializer> serializerMap=new HashMap<>();

    public SerizalizerManager() {
        init();
    }
    public Serializer getSerializer(int typeCode){
        return serializerMap.get(typeCode);
    }
    private void init(){
        serializerMap.put(Serializer.SerizalizerType.JSON.getTypeCode(),new JsonSerializer());
        serializerMap.put(Serializer.SerizalizerType.HESSIAN.getTypeCode(), new HessonSerializer());
    }
}
