package com.littlewool.tech.insight.rpc.message;

import java.nio.charset.StandardCharsets;

/**
 * @ClassName: Message
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 19:30
 * @Version: 1.0
 **/

public class Message {

    public static final byte[] MAGIC = "littlewool".getBytes(StandardCharsets.UTF_8);

    private byte[] magic;

    private byte messageType;

    private byte[] body;

    public enum MessageType {
        REQUEST(1), RESPONSE(2);

        private final byte code;

        MessageType(int code) {
            this.code = (byte)code;
        }

        public byte getCode() {
            return code;
        }
    }

}
