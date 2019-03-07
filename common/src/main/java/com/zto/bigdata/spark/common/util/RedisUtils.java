package com.zto.bigdata.spark.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Redis连接池
 * @author ChengLong 2018年8月20日 07:39:51
 */
public class RedisUtils {
    private static final Logger logger = LoggerFactory.getLogger(RedisUtils.class);

    // redis实例
	private static JedisCluster redisCluster = null;
    // 持有锁的最长时间
    private static final int expireTime = 2;
    // 获取不到锁的休眠时间
    private static final long sleepTime = 100;
    // 锁中断状态
    private static boolean interruped = true;

    static{
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // 最大连接数
        poolConfig.setMaxTotal(1);
        // 最大空闲数
        poolConfig.setMaxIdle(1);
        // 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：
        // Could not get a resource from the pool
        poolConfig.setMaxWaitMillis(1000);
        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
        //生产
        nodes.add(new HostAndPort("192.168.6.227", 6398));
        nodes.add(new HostAndPort("192.168.6.228", 6396));
        nodes.add(new HostAndPort("192.168.6.228", 6397));
        nodes.add(new HostAndPort("192.168.6.229", 6396));
        nodes.add(new HostAndPort("192.168.6.229", 6397));
        nodes.add(new HostAndPort("192.168.6.230", 6396));
        RedisUtils.redisCluster = new JedisCluster(nodes, poolConfig);
    }

	/*
	 * 获取Jedis实例
	 *
	 * @return
	 */
	public static synchronized JedisCluster getJedis() {
		return RedisUtils.redisCluster;
	}

    /**
     * 获取redis分布式锁
     * @param lockName
     */
    public static void lock(String lockName) {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (!RedisUtils.interruped)
                throw new RuntimeException("获取锁状态被中断");
            long id = getJedis().setnx(lockName, lockName);
            logger.info("正在获取分布式锁key=" + lockName);
            if (id == 0L) {
                try {
                    Thread.sleep(RedisUtils.sleepTime);
                    if ((System.currentTimeMillis() - startTime) / 1000 > RedisUtils.expireTime) {
                        logger.info("持锁超时key=" + lockName);
                        unlock(lockName);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                getJedis().expire(lockName, RedisUtils.expireTime);
                logger.info("成功获取分布式锁key=" + lockName);
                break;
            }
        }
    }

    public static void lockInterruptibly() throws InterruptedException {
        RedisUtils.interruped = false;
    }

    /**
     * 解锁
     * @param lockName
     */
    public static void unlock(String lockName) {
        try {
            logger.info("删除获取分布式锁key=" + lockName);
            getJedis().del(lockName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


	public static void main(String[] args) {
		RedisUtils.getJedis().set("test", "hello2");
		System.err.println(RedisUtils.getJedis().get("test"));
	}
}
