package org.apache.flink.runtime.reConfig.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Title: RedisUtil
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/1/16 13:41
 * @description: Redis工具类
 */
public class RedisUtil {

	private static final String REDIS_HOST = "redis-container";
	private static final Integer REDIS_PORT = 6379;

	private static JedisPool jedisPool;

	static {
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPool = new JedisPool(jedisPoolConfig, REDIS_HOST, REDIS_PORT, 1000);
	}

	public static Jedis getJedis() {
		return jedisPool.getResource();
	}

	public static void closeJedis(Jedis jedis) {
		if (jedis != null) {
			jedis.close();
		}
	}

	public static void set(String key, String value) {
		try (Jedis jedis = getJedis()) {
			jedis.set(key, value);
		}
	}

	public static void set(byte[] key, byte[] value){
		try (Jedis jedis = getJedis()){
			jedis.set(key, value);
		}
	}

	public static void set(byte[] key, byte[] value, int second){
		try (Jedis jedis = getJedis()){
			jedis.setex(key, second, value);
		}
	}

	public static String get(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.get(key);
		}
	}

	public static byte[] get(byte[] key){
		try (Jedis jedis = getJedis()){
			return jedis.get(key);
		}
	}

	public static Long del(String key){
		try (Jedis jedis = getJedis()){
			return jedis.del(key);
		}
	}

	public static Long del(byte[] key){
		try (Jedis jedis = getJedis()){
			return jedis.del(key);
		}
	}

	public static boolean exist(byte[] key){
		try (Jedis jedis = getJedis()){
			return jedis.exists(key);
		}
	}

	public static String flushDB(){
		try (Jedis jedis = getJedis()){
			return jedis.flushDB();
		}
	}

	public static void setex(byte[] key, byte[] value){
		try (Jedis jedis = getJedis()){
			jedis.setex(key, 60, value);
		}
	}


}
