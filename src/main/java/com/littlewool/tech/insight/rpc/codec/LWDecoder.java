package com.littlewool.tech.insight.rpc.codec;

import com.littlewool.tech.insight.rpc.compress.Compression;
import com.littlewool.tech.insight.rpc.compress.CompressionManager;
import com.littlewool.tech.insight.rpc.message.Message;
import com.littlewool.tech.insight.rpc.serializer.Serializer;
import com.littlewool.tech.insight.rpc.serializer.SerizalizerManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.AttributeKey;

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

    public static final AttributeKey<Integer> SERIALIZE_KEY = AttributeKey.valueOf("serializeKey");
    public static final AttributeKey<SerizalizerManager> SERIALIZER_MANAGER_KEY =
        AttributeKey.valueOf("serializeManagerKey");

    public static final AttributeKey<Integer> COMPRESS_KEY = AttributeKey.valueOf("compresKey");
    public static final AttributeKey<CompressionManager> COMPRESS_MANAGER_KEY =
        AttributeKey.valueOf("compressManagerKey");

    private volatile SerizalizerManager serizalizerManager;
    private volatile CompressionManager compressionManager;

    public LWDecoder() {
        // 0 4 0 4
        super(1024 * 1024, 0, Integer.BYTES, 0, Integer.BYTES);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        initIfNecessary(ctx);
        ByteBuf frame = (ByteBuf)super.decode(ctx, in);
        if (null == frame) {
            return null;
        }
        try {
            byte[] magic = new byte[Message.MAGIC.length];
            frame.readBytes(magic);
            if (!Arrays.equals(magic, Message.MAGIC)) {
                throw new RuntimeException("魔数不正确,协议无效");
            }
            byte messageType = frame.readByte();
            short version = frame.readShort();
            byte finalSac = frame.readByte();
            Serializer serializer=this.serizalizerManager.getSerializer((finalSac&0b11110000)>>>4);
            if(null==serializer){
                throw new IllegalArgumentException("没有支持的序列化器");
            }
            Compression compression=this.compressionManager.getCompression(finalSac&0b00001111);
            if(null==compression){
                throw new IllegalArgumentException("没有支持的压缩器");
            }
            byte[] body = new byte[frame.readableBytes()];
            frame.readBytes(body);
            body= compression.decompress(body);
            Message.MessageType type=Message.MessageType.OfCode(messageType);
            if(null==type){
                throw new IllegalArgumentException("不支持的消息类型");
            }
            return serializer.deserialize(body, type.getMessageClass());
        } finally {
            frame.release();
        }

    }

    private void initIfNecessary(ChannelHandlerContext ctx) {
        if (serizalizerManager != null) {
            return;
        }
        serizalizerManager = ctx.channel().attr(SERIALIZER_MANAGER_KEY).get();
        compressionManager = ctx.channel().attr(COMPRESS_MANAGER_KEY).get();
        if (serizalizerManager == null) {
            throw new IllegalStateException("序列化管理器未在通道属性中设置");
        }
        if (compressionManager == null) {
            throw new IllegalStateException("压缩管理器未在通道属性中设置");
        }
        return;
    }
}
