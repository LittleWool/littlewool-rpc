package com.littlewool.tech.insight.rpc.consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: ConnectionManager
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/13 10:33
 * @Version: 1.0
 **/

@Slf4j
public class ConnectionManager {
    private final Map<String, ChannelWrapper> channelTables = new ConcurrentHashMap<>();

    private final Bootstrap bootstrap;

    public ConnectionManager(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    //若是之前有未连接的channel，但之后可以联通了，貌似不能更新状态
    public Channel getChannel(String host, int port) {
        String key = host + ":" + port;
        ChannelWrapper channelWrapper = channelTables.computeIfAbsent(key, (k) -> {
            try {
                ChannelFuture channelFuture = bootstrap.connect(host, port).sync();

                //TODO 这里的监听器原理是什么,
                channelFuture.channel().closeFuture().addListener((f) -> {channelTables.remove(key);});

                return new ChannelWrapper(channelFuture.channel());
            } catch (InterruptedException e) {
                log.error("连接超时{},{}", host, port, e);
                return new ChannelWrapper(null);
                //throw new RuntimeException(e);
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

}
