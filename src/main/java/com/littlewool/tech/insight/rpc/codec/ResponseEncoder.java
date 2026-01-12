package com.littlewool.tech.insight.rpc.codec;

import com.alibaba.fastjson2.JSONObject;
import com.littlewool.tech.insight.rpc.message.Message;
import com.littlewool.tech.insight.rpc.message.Response;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/**
 * @ClassName: ResponseEncoder
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:10
 * @Version: 1.0
 **/

public class ResponseEncoder extends MessageToByteEncoder<Response> {



    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Response response, ByteBuf out) throws Exception {
        byte[] magic= Message.MAGIC;
        byte messageType=Message.MessageType.RESPONSE.getCode();
        byte[] body = serializeResponse(response);
        int length=magic.length+Byte.BYTES+body.length;
        out.writeInt(length);
        out.writeBytes(magic);
        out.writeByte(messageType);
        out.writeBytes(body);
    }

    private byte[] serializeResponse(Response response){
        return JSONObject.toJSONString(response).getBytes(StandardCharsets.UTF_8);
    }
}
