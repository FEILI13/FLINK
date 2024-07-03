package org.apache.flink.runtime.reConfig.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * @Title: RedisTest
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/1/15 16:09
 * @description: 测试Redis功能
 */
public class RedisTest {

	public static void main(String[] args) {
//		Map<String, Object> dataMap = new HashMap<>();
//		dataMap.put("name", "John");
//		dataMap.put("age", 25);
//		dataMap.put("city", "New York");
//
//		System.out.println("before :"+dataMap);
//
//		byte[] hashMapBytes = SerializerUtil.serialize(dataMap);
//		byte[] key = SerializerUtil.serialize("test");
//
//		RedisUtil.set(key, hashMapBytes);
//
//		Map<String, Object> result = (Map<String, Object>) SerializerUtil.deserialize(RedisUtil.get(key));
//		System.out.println("after :"+result);

		Map<String, Object> dataMap = new HashMap<>();
		dataMap.put("name", "John");
		dataMap.put("age", 26);
		dataMap.put("city", "New York");
		System.out.println("before:"+dataMap);

		String key = JSON.toJSONString("test");
		String before = JSON.toJSONString(dataMap);

		RedisUtil.set(key, before);

		Map<String, Object> after = JSON.parseObject(RedisUtil.get(key), new TypeReference<Map<String, Object>>() {});

		System.out.println("after:"+after);


	}

}
