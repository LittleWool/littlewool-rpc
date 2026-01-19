package com.littlewool.tech.insight.rpc.codec;

import com.littlewool.tech.insight.rpc.compress.Compression;
import com.littlewool.tech.insight.rpc.compress.CompressionManager;
import com.littlewool.tech.insight.rpc.message.Message;
import com.littlewool.tech.insight.rpc.serializer.Serializer;
import com.littlewool.tech.insight.rpc.serializer.SerizalizerManager;
import com.littlewool.tech.insight.rpc.version.Version;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

/**
 * @ClassName: LWEncoder
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 8:47
 * @Version: 1.0
 **/

@Slf4j
public class LWEncoder extends MessageToByteEncoder<Object> {
    public static final AttributeKey<Integer> SERIALIZE_KEY = AttributeKey.valueOf("serializeKey");
    public static final AttributeKey<SerizalizerManager> SERIALIZER_MANAGER_KEY =
        AttributeKey.valueOf("serializeManagerKey");

    public static final AttributeKey<Integer> COMPRESS_KEY = AttributeKey.valueOf("compresKey");
    public static final AttributeKey<CompressionManager> COMPRESS_MANAGER_KEY =
        AttributeKey.valueOf("compressManagerKey");

    private volatile Serializer defaultSerializer;
    private volatile Compression defaultCompression;
    private volatile byte defaultSerializeAndCompressCode;
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        initIfNecessary(ctx);
        Message.MessageType messageType = Message.MessageType.ofClass(msg.getClass());
        if (null == messageType) {
            log.warn("{}不支持序列化", msg.getClass().getName());
            return;
        }
        byte[] magic = Message.MAGIC;
        byte messageTypeCode = messageType.getCode();
        Version current = Version.V1;

        byte[] body = defaultSerializer.serialize(msg);
        byte finalSac=defaultSerializeAndCompressCode;
        if (body.length<256){
             finalSac &=(byte) 0b11110000l;
        }else {
            body=defaultCompression.compress(body);
        }
        int length = magic.length + Byte.BYTES * 2 + Short.BYTES + body.length;
        //长度
        out.writeInt(length);
        //魔数
        out.writeBytes(magic);
        //发送消息类型 这里是request或response
        out.writeByte(messageTypeCode);
        //版本号
        out.writeShort(current.getVersionNum());
        //所使用的序列化和压缩算法
        out.writeByte(finalSac);
        //消息体
        out.writeBytes(body);

    }

    private void initIfNecessary(ChannelHandlerContext ctx) {
        Integer serializeCode = ctx.channel().attr(SERIALIZE_KEY).get();
        SerizalizerManager serizalizerManager = ctx.channel().attr(SERIALIZER_MANAGER_KEY).get();
        defaultSerializer=serizalizerManager.getSerializer(serializeCode);

        Integer compressCode = ctx.channel().attr(COMPRESS_KEY).get();
        CompressionManager compressionManager = ctx.channel().attr(COMPRESS_MANAGER_KEY).get();
        defaultCompression=compressionManager.getCompression(compressCode);
        if (null == defaultSerializer) {
            throw new IllegalArgumentException("不存在默认的序列化器");
        }

        if (null == defaultCompression) {
            throw new IllegalArgumentException("不存在默认的压缩器");
        }
        defaultSerializeAndCompressCode=(byte)((serializeCode << 4) | compressCode);
    }
}
