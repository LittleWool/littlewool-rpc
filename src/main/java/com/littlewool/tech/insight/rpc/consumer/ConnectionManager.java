package com.littlewool.tech.insight.rpc.consumer;

import com.littlewool.tech.insight.rpc.codec.LWDecoder;
import com.littlewool.tech.insight.rpc.codec.LWEncoder;
import com.littlewool.tech.insight.rpc.codec.RequestEncoder;
import com.littlewool.tech.insight.rpc.compress.Compression;
import com.littlewool.tech.insight.rpc.compress.CompressionManager;
import com.littlewool.tech.insight.rpc.message.Response;
import com.littlewool.tech.insight.rpc.register.ServiceMetadata;
import com.littlewool.tech.insight.rpc.serializer.Serializer;
import com.littlewool.tech.insight.rpc.serializer.SerizalizerManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.net.PortUnreachableException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: ConnectionManager
 * @Description: 连接管理器,实现连接复用
 * @Author: LittleWool
 * @Date: 2026/1/13 10:33
 * @Version: 1.0
 **/

@Slf4j
public class ConnectionManager {
    private final Map<String, ChannelWrapper> channelTables = new ConcurrentHashMap<>();

    private final Bootstrap bootstrap;
    private final InFlightRequestManager inFlightRequestManager;
    private final  ConsumerProperties consumerProperties;
    private final SerizalizerManager serizalizerManager;
    private final CompressionManager compressionManager;

    public ConnectionManager(InFlightRequestManager inFlightRequestManager, ConsumerProperties consumerProperties) {
        this.inFlightRequestManager = inFlightRequestManager;
        this.consumerProperties=consumerProperties;
        this.bootstrap = createBootstrap(consumerProperties);
        this.serizalizerManager = new SerizalizerManager();
        this.compressionManager=new CompressionManager();
    }

    public Channel getChannel(ServiceMetadata metadata) {
        String host = metadata.getHost();
        int port = metadata.getPort();
        String key = host + ":" + port;

        ChannelWrapper channelWrapper = channelTables.computeIfAbsent(key, (k) -> {
            try {
                ChannelFuture channelFuture = bootstrap.connect(host, port).sync();
                // 这里加入监听器，是防止注册失败 之后调用会因为之前存入的null一直受阻
                channelFuture.channel().closeFuture().addListener((f) -> {
                    channelTables.remove(key);
                    inFlightRequestManager.clearChannel(metadata);
                });

                return new ChannelWrapper(channelFuture.channel());
            } catch (InterruptedException e) {
                log.error("连接超时{},{}", host, port, e);
                return new ChannelWrapper(null);
                // throw new RuntimeException(e);
            }
        });
        Channel channel = channelWrapper.channel;
        if (null == channel || !channel.isActive()) {
            channelTables.remove(key);
            return null;
        }
        return channel;
    }

    private static class ChannelWrapper {
        final Channel channel;

        public ChannelWrapper(Channel channel) {
            this.channel = channel;
        }

    }

    private Bootstrap createBootstrap(ConsumerProperties consumerProperties) {
        Bootstrap bootstrap = new Bootstrap();
        return bootstrap.group(new NioEventLoopGroup(consumerProperties.getWorkThreadNum()))
            .channel(NioSocketChannel.class)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, consumerProperties.getConnectTimeoutMs())
            .handler(new ChannelInitializer<NioSocketChannel>() {
                @Override
                protected void initChannel(NioSocketChannel nioSocketChannel) {
                    nioSocketChannel.pipeline().addLast(new LWDecoder()).addLast(new LWEncoder())
                        .addLast(new ConsumerHandler());
                }
            });
    }

    private class ConsumerHandler extends SimpleChannelInboundHandler<Response> {

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Response response) {
            inFlightRequestManager.completeRequest(response.getRequestId(), response);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            //防止重复创建序列化器,读取配置存放需要时用的序列化器和压缩器种类 和管理器
            log.info("地址:{}连接了", ctx.channel().remoteAddress());
            Serializer.SerizalizerType serizalizerType=Serializer.SerizalizerType.valueOf(consumerProperties.getSerialize().toUpperCase(Locale.ROOT));
            ctx.channel().attr(LWEncoder.SERIALIZE_KEY).set(serizalizerType.getTypeCode());
            ctx.channel().attr(LWEncoder.SERIALIZER_MANAGER_KEY).set(serizalizerManager);
            Compression.CompressionType compressionType = Compression.CompressionType.valueOf(consumerProperties.getCompress().toUpperCase(Locale.ROOT));
            ctx.channel().attr(LWEncoder.COMPRESS_KEY).set(compressionType.getTypeCode());
            ctx.channel().attr(LWEncoder.COMPRESS_MANAGER_KEY).set(compressionManager);
            ctx.fireChannelActive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("发生了异常", cause);
            ctx.channel().close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("地址:{}断开连接", ctx.channel().remoteAddress());
           ctx.fireChannelInactive();
        }
    }

}
