package com.littlewool.tech.insight.rpc.serializer;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * @ClassName: HessonSerializer
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 9:09
 * @Version: 1.0
 **/

@Slf4j
public class HessonSerializer implements Serializer{
    @Override
    public byte[] serialize(Object object) {
        try (ByteArrayOutputStream oos=new ByteArrayOutputStream()){
            Hessian2Output hessian2Output=new Hessian2Output(oos);
            hessian2Output.writeObject(object);
            hessian2Output.flush();
            return oos.toByteArray();
        }catch (Exception e){
            return new byte[0];
        }

    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> objectClass) {
        try (ByteArrayInputStream is=new ByteArrayInputStream(bytes)) {
            Hessian2Input hessian2Input = new Hessian2Input(is);
            return (T)hessian2Input.readObject();
        }catch (Exception e){
            log.error("Hessian 反序列化失败{}",objectClass.getClass().getName(),e);
            return null;
        }
    }
}
