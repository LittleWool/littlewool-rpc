package com.littlewool.tech.insight.rpc.codec;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.TypeReference;
import com.littlewool.tech.insight.rpc.message.Message;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.message.Response;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.util.Arrays;
import java.util.Objects;


/**
 * @ClassName: LWDecoder
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 19:51
 * @Version: 1.0
 **/

public class LWDecoder extends LengthFieldBasedFrameDecoder {

    public LWDecoder() {
        //0 4   0   4
        super(1024 * 1024, 0, Integer.BYTES, 0, Integer.BYTES);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        byte[] magic = new byte[Message.MAGIC.length];
        frame.readBytes(magic);
        if (!Arrays.equals(magic, Message.MAGIC)) {
            throw new RuntimeException("魔数不正确,协议无效");
        }
        byte messageType = frame.readByte();
        byte[] body = new byte[frame.readableBytes()];
        frame.readBytes(body);
        if (Objects.equals(Message.MessageType.REQUEST.getCode(), messageType)) {
            return deserializeRequest(body);
        }
        if (Objects.equals(Message.MessageType.RESPONSE.getCode(), messageType)) {
            return deserializeResponse(body);
        }
        throw new RuntimeException("解析类型不支持");

    }

    private Object deserializeResponse(byte[] body) {

        try {
            String jsonString = new String(body, "UTF-8");
            // 如果Request类有泛型参数
            return JSON.parseObject(jsonString, new TypeReference<Response>() {
            },JSONReader.Feature.SupportClassForName);
        } catch (Exception e) {
            throw new RuntimeException("FastJSON2反序列化失败", e);
        }
    }

    private Object deserializeRequest(byte[] body) {
        try {
            String jsonString = new String(body, "UTF-8");
            // 如果Request类有泛型参数
            return JSON.parseObject(jsonString, new TypeReference<Request>() {
            }, JSONReader.Feature.SupportClassForName);
        } catch (Exception e) {
            throw new RuntimeException("FastJSON2反序列化失败", e);
        }
    }
}
