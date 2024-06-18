package org.apache.flink.runtime.reConfig.utils;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

/**
 * @Title: SerializerVSJson
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/1/16 14:55
 * @description: 序列化对比JSON速度
 */
public class SerializerVSJson {

	public static void main(String[] args) {
		Map<String, Object> hm = new HashMap<>();
		for(int i=0;i<100;++i){
			hm.put(RandomStringGenerator.generateRandomString(5), RandomStringGenerator.generateRandomString(10));
		}

		//jsonTest(hm);
		serialTest(hm);

	}

	public static void jsonTest(Map<String, Object> hm){
		StringBuilder sb = new StringBuilder("test_");

		Long beforeJson = System.currentTimeMillis();
		// 测试JSON速度
		for(int i=0;i<100000;++i){
			StringBuilder newSb = new StringBuilder(sb);
			String s = newSb.append(i).toString();
			hm.put(s, s);
			String key = JSON.toJSONString(s);
			String value = JSON.toJSONString(hm);
			RedisUtil.set(key, value);
			hm.remove(s);
		}
		Long afterJson = System.currentTimeMillis();

		RedisUtil.flushDB();
		Long jsonResult = afterJson - beforeJson;
		System.out.println("json result:"+jsonResult);
	}

	public static void serialTest(Map<String, Object> hm){
		StringBuilder sb = new StringBuilder("test_");

		Long beforeSerial = System.currentTimeMillis();
		// 测试序列化速度
		for(int i=0;i<100000;++i){
			StringBuilder newSb = new StringBuilder(sb);
			String s = newSb.append(i).toString();
			hm.put(s, s);
			byte[] key = SerializerUtil.serialize(s);
			byte[] value = SerializerUtil.serialize(hm);
			RedisUtil.set(key, value);
			hm.remove(s);
		}
		Long afterSerial = System.currentTimeMillis();

		RedisUtil.flushDB();
		Long serialResult = afterSerial - beforeSerial;
		System.out.println("serial result:"+serialResult);
	}

}
