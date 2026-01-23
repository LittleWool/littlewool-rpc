package com.littlewool.tech.insight.rpc.message;

import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: Message
 * @Description: 数据协议
 * @Author: LittleWool
 * @Date: 2026/1/12 19:30
 * @Version: 1.0
 **/
@Data
public class Message {

    private static final Map<Class<?>, MessageType> CLASS_CACHE = new HashMap<>();
    private static final Map<Byte, MessageType> CODE_CACHE = new HashMap<>();

    public static final byte[] MAGIC = "littlewool".getBytes(StandardCharsets.UTF_8);

    private byte[] magic;

    private byte messageType;
    // 可以做一个向前兼容向后兼容，兼容性保证的服务器
    private short version;

    private byte[] body;

    // 所使用的序列化算法和压缩 前思维序列化 后四位压缩 这样子省空间
    private byte serializeAndCompress;

    public enum MessageType {

        REQUEST(1, Request.class),
        RESPONSE(2, Response.class),
        HEARTBEAT_REQUEST(3,HeartbeatRequest.class),
        HEARTBEAT_RESPONSE(4,HeartbeatResponse.class)
        ;

        public byte getCode() {
            return code;
        }

        private final byte code;

        private final Class<?> messageClass;

        static {
            for (MessageType value : values()) {
                if (CLASS_CACHE.put(value.messageClass, value) != null) {
                    throw new IllegalArgumentException(value + "没有唯一对应消息类");
                }
                if (CODE_CACHE.put(value.code, value) != null) {
                    throw new IllegalArgumentException(value.code + "没有唯一对应消息类");
                }
            }

        }

        MessageType(int code, Class<?> messageClass) {
            this.code = (byte)code;
            this.messageClass = messageClass;
        }

        public static MessageType ofClass(Class<?> messageClass) {
            return CLASS_CACHE.get(messageClass);
        }

        public static MessageType OfCode(byte code) {
            return CODE_CACHE.get(code);
        }

        public Class<?> getMessageClass() {
            return this.messageClass;
        }
    }

}
