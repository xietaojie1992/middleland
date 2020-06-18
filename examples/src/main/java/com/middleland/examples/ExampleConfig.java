package com.middleland.examples;

import com.middleland.commons.curator.CuratorConfig;
import com.middleland.commons.curator.CuratorInstance;
import com.middleland.commons.curator.CuratorInstanceImpl;
import com.middleland.commons.rabbit.RabbitConfig;
import com.middleland.commons.rabbit.RabbitFactory;
import com.middleland.commons.redis.JedisInstance;
import com.middleland.commons.redis.RedisConfig;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.Jedis;

/**
 * @author xietaojie
 */
@Configuration
public class ExampleConfig {

    @Bean
    @ConfigurationProperties(prefix = "rabbitmq")
    public RabbitConfig rabbitConfig() {
        return new RabbitConfig();
    }

    @Bean
    @ConditionalOnBean(name = "rabbitConfig")
    public RabbitFactory rabbitFactory(RabbitConfig rabbitConfig) {
        return new RabbitFactory(rabbitConfig);
    }

    @Bean
    @ConfigurationProperties(prefix = "curator")
    public CuratorConfig curatorConfig() {
        return new CuratorConfig();
    }

    @Bean
    @ConditionalOnBean(name = "curatorConfig")
    public CuratorInstance curatorInstance(CuratorConfig curatorConfig) throws Exception {
        return new CuratorInstanceImpl(curatorConfig);
    }

    @Bean
    @ConditionalOnBean(name = "curatorInstance")
    public CuratorFramework curatorFramework(CuratorInstance curatorInstance) {
        return curatorInstance.getCurator();
    }

    @Bean
    @ConfigurationProperties(prefix = "redis")
    public RedisConfig redisConfig() {
        return new RedisConfig();
    }

    @Bean
    @ConditionalOnBean(name = "redisConfig")
    public JedisInstance jedisInstance(RedisConfig redisConfig) {
        return new JedisInstance(redisConfig);
    }

    @Bean
    @ConditionalOnBean(name = "jedisInstance")
    public Jedis jedis(JedisInstance jedisInstance) {
        return jedisInstance.getInstance();
    }
}
