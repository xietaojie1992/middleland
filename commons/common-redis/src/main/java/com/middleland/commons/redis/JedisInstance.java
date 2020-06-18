package com.middleland.commons.redis;

import lombok.Data;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Data
public class JedisInstance {

    private final JedisPool jedisPool;

    public JedisInstance(RedisConfig redisConfig) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(redisConfig.getPool().getMaxTotal());
        jedisPoolConfig.setMaxIdle(redisConfig.getPool().getMaxIdle());
        jedisPoolConfig.setMinIdle(redisConfig.getPool().getMinIdle());
        jedisPoolConfig.setMaxWaitMillis(redisConfig.getPool().getMaxWaitMillis());
        jedisPool = new JedisPool(jedisPoolConfig, redisConfig.getHost(), redisConfig.getPort());
    }

    public Jedis getInstance() {
        return jedisPool.getResource();
    }
}
