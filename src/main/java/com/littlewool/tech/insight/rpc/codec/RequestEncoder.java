package com.littlewool.tech.insight.rpc.codec;

import com.alibaba.fastjson2.JSONObject;
import com.littlewool.tech.insight.rpc.message.Message;
import com.littlewool.tech.insight.rpc.message.Request;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/**
 * @ClassName: RequestEncoder
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:03
 * @Version: 1.0
 **/

public class RequestEncoder extends MessageToByteEncoder<Request> {
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Request request, ByteBuf out) throws Exception {
        byte[] magic= Message.MAGIC;
        byte messageType=Message.MessageType.REQUEST.getCode();
        byte[] body = serializeRequest(request);
        int length=magic.length+Byte.BYTES+body.length;
        out.writeInt(length);
        out.writeBytes(magic);
        out.writeByte(messageType);
        out.writeBytes(body);

    }
    private byte[] serializeRequest(Request request){
        return JSONObject.toJSONString(request).getBytes(StandardCharsets.UTF_8);
    }
}
