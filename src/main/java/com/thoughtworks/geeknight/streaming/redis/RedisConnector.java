package com.thoughtworks.geeknight.streaming.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;
import java.util.Set;

public class RedisConnector implements Serializable{
    private static JedisPool pool;

    public RedisConnector() {
        pool = new JedisPool("localhost", 6379);
    }

    public void increment(String key) {
        Jedis jedisConnection = pool.getResource();
        jedisConnection.incr(key);
        jedisConnection.close();
    }
}
