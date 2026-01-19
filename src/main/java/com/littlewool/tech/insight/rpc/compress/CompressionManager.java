package com.littlewool.tech.insight.rpc.compress;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: CompressManager
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 10:22
 * @Version: 1.0
 **/

public class CompressionManager {
    private Map<Integer, Compression> compressionMap=new HashMap<>();

    public CompressionManager() {
        init();
    }
    public Compression getCompression(int typeCode){
        return compressionMap.get(typeCode);
    }
    private void init(){
        compressionMap.put(Compression.CompressionType.NONE.getTypeCode(), new NullCompression());
        compressionMap.put(Compression.CompressionType.GZIP.getTypeCode(), new GzipCompression());
    }
}
