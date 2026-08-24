package com.littlewool.tech.insight.rpc.limit;

import redis.clients.jedis.JedisPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: ShardedRedisDistributedLimitStore
 * @Description: 多 Redis 节点分片限流存储，按 shard key 均匀路由到不同 Redis 节点
 * @Author: LittleWool
 * @Date: 2026/8/24 23:35
 * @Version: 1.0
 **/
public class ShardedRedisDistributedLimitStore implements DistributedLimitStore, Closeable {

    private static final String SHARD_MARKER = ":shard:";

    private final List<DistributedLimitStore> stores;

    public ShardedRedisDistributedLimitStore(List<RedisNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        List<DistributedLimitStore> redisStores = new ArrayList<>(nodes.size());
        for (RedisNode node : nodes) {
            redisStores.add(new RedisDistributedLimitStore(node.host, node.port));
        }
        this.stores = Collections.unmodifiableList(redisStores);
    }

    public ShardedRedisDistributedLimitStore(JedisPool... jedisPools) {
        if (jedisPools == null || jedisPools.length == 0) {
            throw new IllegalArgumentException("jedisPools must not be empty");
        }
        List<DistributedLimitStore> redisStores = new ArrayList<>(jedisPools.length);
        for (JedisPool jedisPool : jedisPools) {
            redisStores.add(new RedisDistributedLimitStore(jedisPool));
        }
        this.stores = Collections.unmodifiableList(redisStores);
    }

    ShardedRedisDistributedLimitStore(DistributedLimitStore... stores) {
        if (stores == null || stores.length == 0) {
            throw new IllegalArgumentException("stores must not be empty");
        }
        List<DistributedLimitStore> delegates = new ArrayList<>(stores.length);
        Collections.addAll(delegates, stores);
        this.stores = Collections.unmodifiableList(delegates);
    }

    @Override
    public boolean tryAcquire(String key, int maxPermits, long window, TimeUnit unit) {
        return selectStore(key).tryAcquire(key, maxPermits, window, unit);
    }

    @Override
    public int tryAcquireTokens(String key, int permitsPerSecond, int capacity, int requestPermits) {
        return selectStore(key).tryAcquireTokens(key, permitsPerSecond, capacity, requestPermits);
    }

    @Override
    public void close() {
        for (DistributedLimitStore store : stores) {
            if (store instanceof Closeable) {
                closeQuietly((Closeable)store);
            }
        }
    }

    private DistributedLimitStore selectStore(String key) {
        int shard = parseShardId(key);
        if (shard >= 0) {
            return stores.get(shard % stores.size());
        }
        return stores.get(Math.floorMod(key.hashCode(), stores.size()));
    }

    private static int parseShardId(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        int index = key.lastIndexOf(SHARD_MARKER);
        if (index < 0) {
            return -1;
        }
        int start = index + SHARD_MARKER.length();
        if (start >= key.length()) {
            return -1;
        }
        int shard = 0;
        for (int i = start; i < key.length(); i++) {
            char ch = key.charAt(i);
            if (ch < '0' || ch > '9') {
                return -1;
            }
            shard = shard * 10 + ch - '0';
        }
        return shard;
    }

    private static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // close best effort
        }
    }

    public static RedisNode node(String host, int port) {
        return new RedisNode(host, port);
    }

    public static class RedisNode {

        private final String host;

        private final int port;

        public RedisNode(String host, int port) {
            if (host == null || host.isEmpty()) {
                throw new IllegalArgumentException("host must not be empty");
            }
            if (port <= 0) {
                throw new IllegalArgumentException("port must be positive");
            }
            this.host = host;
            this.port = port;
        }
    }
}
