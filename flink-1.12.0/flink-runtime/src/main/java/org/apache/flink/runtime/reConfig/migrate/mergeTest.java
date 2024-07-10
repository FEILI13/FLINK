package org.apache.flink.runtime.reConfig.migrate;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.TypeReference;

import org.apache.flink.runtime.reConfig.utils.SerializerUtil;
import org.apache.flink.runtime.state.VoidNamespace;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Title: mergeTest
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.migrate
 * @Date 2024/1/17 16:07
 * @description: 合并hashmap测试
 */
public class mergeTest {

	public static void main(String[] args) {
		// 创建两个示例Map
//		Map<Integer, Map<String, String>> map1 = new HashMap<>();
//		map1.put(1, createInnerMap("A", "ValueA1"));
//		map1.put(2, createInnerMap("B", "ValueB1"));
//
//		Map<Integer, Map<String, String>> map2 = new HashMap<>();
//		map2.put(1, createInnerMap("D", "ValueA2"));
//		map2.put(3, createInnerMap("C", "ValueC2"));
//
//		// 合并两个Map
//		Map<Integer, Map<String, String>> mergedMap = mergeMaps(map1, map2);
//
//		// 打印合并后的Map
//		mergedMap.forEach((key, value) -> System.out.println(key + " -> " + value));
		VoidNamespace pre = VoidNamespace.get();
		byte[] s = SerializerUtil.serialize(pre);
		VoidNamespace tmp = (VoidNamespace) SerializerUtil.deserialize(s);
		System.out.println(tmp.equals(pre));
	}

	private static Map<String, String> createInnerMap(String key, String value) {
		Map<String, String> innerMap = new HashMap<>();
		innerMap.put(key, value);
		return innerMap;
	}

	private static Map<Integer, Map<String, String>> mergeMaps(
		Map<Integer, Map<String, String>> map1,
		Map<Integer, Map<String, String>> map2) {
		return Stream.concat(map1.entrySet().stream(), map2.entrySet().stream())
			.collect(Collectors.toMap(
				Map.Entry::getKey,
				Map.Entry::getValue,
				(innerMap1, innerMap2) -> {
					// 合并相同键的内部Map
					innerMap1.putAll(innerMap2);
					return innerMap1;
				}));
	}


}
