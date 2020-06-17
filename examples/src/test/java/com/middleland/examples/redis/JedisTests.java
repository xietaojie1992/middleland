package com.middleland.examples.redis;

import com.middleland.examples.ExamplesApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Set;

/**
 * @author xietaojie
 */
@Slf4j
public class JedisTests extends ExamplesApplicationTests {

    private Jedis jedis = new Jedis("localhost", 6379);

    @Test
    public void stringTest() {
        jedis.set("key", "value");
        Assert.assertTrue(jedis.exists("key"));
        Assert.assertEquals("value", jedis.get("key"));
        jedis.del("key");
        Assert.assertNull(jedis.get("key"));

        // multiple
        jedis.mset("key1", "value1", "key2", "value2", "key3", "value3");
        Assert.assertEquals("value1", jedis.get("key1"));
        Assert.assertEquals("value2", jedis.get("key2"));
        Assert.assertEquals("value3", jedis.get("key3"));
        List<String> values = jedis.mget("key1", "key2", "key3");
        log.info("mget values: {}", values);


        jedis.del("key1", "key2", "key3");
        Assert.assertNull(jedis.get("key1"));
        Assert.assertNull(jedis.get("key2"));
        Assert.assertNull(jedis.get("key3"));

        // increase and decrease
        jedis.set("key", "100");
        Assert.assertEquals("100", jedis.get("key"));
        jedis.incr("key");
        Assert.assertEquals("101", jedis.get("key"));
        jedis.decr("key");
        Assert.assertEquals("100", jedis.get("key"));

        jedis.incrBy("key", 100);
        Assert.assertEquals("200", jedis.get("key"));
        jedis.decrBy("key", 100);
        Assert.assertEquals("100", jedis.get("key"));
        jedis.del("key");
    }

    @Test
    public void hashTest() {
        jedis.hset("book:1", "name", "Harry Potter");
        jedis.hset("book:1", "author", "J. K. Rowling");
        jedis.hset("book:1", "price", "59.99$");
        log.info("book:1, {}", jedis.hgetAll("book:1"));
        Assert.assertEquals("59.99$", jedis.hget("book:1", "price"));

        jedis.hset("book:1", "price", "69.99$");
        Assert.assertEquals("69.99$", jedis.hget("book:1", "price"));

        jedis.hdel("book:1", "price");
        Assert.assertFalse(jedis.hexists("book:1", "price"));

        jedis.del("book:1");
        Assert.assertTrue(jedis.hgetAll("book:1").isEmpty());
    }

    @Test
    public void listTest() {
        jedis.rpush("list", "1", "2", "3");// 从右边 push
        log.info("list: {}", jedis.lrange("list", 0, -1)); // 获取所有元素
        Assert.assertEquals("1", jedis.lpop("list"));// 从左边弹出
        Assert.assertEquals("2", jedis.lpop("list"));
        Assert.assertEquals("3", jedis.lpop("list"));
        jedis.del("list");

        jedis.lpush("stack", "1", "2", "3");// 从左边 push
        log.info("stack: {}", jedis.lrange("stack", 0, -1));
        Assert.assertEquals("3", jedis.lpop("stack"));// 从左边弹出，达到栈的效果
        Assert.assertEquals("2", jedis.lpop("stack"));
        Assert.assertEquals("1", jedis.lpop("stack"));
        jedis.del("stack");
    }

    @Test
    public void setTest() {
        jedis.sadd("set", "1", "2", "2", "2", "3");// 添加元素
        Assert.assertEquals(3, jedis.scard("set").intValue());// 计算总数
        log.info("set: {}", jedis.smembers("set"));// 获取所有元素

        Assert.assertTrue(jedis.sismember("set", "1"));// 判断元素是否存在
        Assert.assertEquals(1, jedis.srem("set", "1").intValue()); // 删除指定元素

        jedis.spop("set");// 随机弹出一个元素
        Assert.assertEquals(1, jedis.scard("set").intValue());

        jedis.del("set");


        // 集合操作
        jedis.sadd("set1", "1", "2", "3");
        jedis.sadd("set2", "2", "3", "4");
        log.info("set1: {}", jedis.smembers("set1"));
        log.info("set2: {}", jedis.smembers("set2"));
        // 交集
        Set<String> inter = jedis.sinter("set1", "set2");
        log.info("inter: {}", inter);
        // 并集
        Set<String> union = jedis.sunion("set1", "set2");
        log.info("union: {}", union);
        // 差集
        log.info("diff set1 and set2: {}", jedis.sdiff("set1", "set2"));
        log.info("diff set2 and set1: {}", jedis.sdiff("set2", "set1"));

        jedis.del("set1");
        jedis.del("set2");
    }

    @Test
    public void zsetTest() {
        // 升序
        jedis.zadd("member", 1, "Tom");
        jedis.zadd("member", 9, "Tony");
        jedis.zadd("member", 5, "John");
        log.info("zset: {}", jedis.zrange("member", 0, -1));
        Assert.assertEquals(3, jedis.zcard("member").intValue());// 计算元素总数
        Assert.assertEquals(9, jedis.zscore("member", "Tony").intValue());// 获取某个元素的分值
        Assert.assertEquals(2, jedis.zrank("member", "Tony").intValue());// 获取某个元素的排名

        jedis.zincrby("member", 100, "Tom");// 增加指定元素的分数
        log.info("zset: {}", jedis.zrange("member", 0, -1));
        Assert.assertEquals(2, jedis.zrank("member", "Tom").intValue());// 获取某个元素的排名

        Set<String> m = jedis.zrange("member", 0, 1);// 返回指定排名的元素
        log.info("zset rank:0-1 range: {}", m);

        jedis.del("member");
    }


    @Test
    public void geoTest() {
    }


    @Test
    public void bitmapsTest() {
    }


    @Test
    public void hyperloglogTest() {
    }

}
