package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.util.MathUtils;

import java.util.Random;

/**
 * @Title: KeyGroupTest
 * @Author lxisnotlcn
 * @Package org.apache.flink.streaming.examples.wordcount
 * @Date 2024/3/14 19:28
 * @description:
 */
public class KeyGroupTest {

	public static void main(String[] args) {
		while(true) {
			String s = generateRandomString(4);
			if(MathUtils.murmurHash(s.hashCode())%128 == 17){
				System.out.println(s);
			}
		}
	}

	/*
	UM57
	pIUm
	Unpw
	HRvb
	HwHh
	Z6nR
	v4fA
	6RjC
	FLbh
	uItr
	 */

	public static String generateRandomString(int length) {
		// 定义可能出现在随机字符串中的字符
		String characterSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
		// 创建一个StringBuilder用于构建最终的随机字符串
		StringBuilder sb = new StringBuilder();
		// 创建Random实例用于生成随机数
		Random random = new Random();
		// 循环length次来构建字符串
		for (int i = 0; i < length; i++) {
			// 生成一个随机索引用于从字符集中选取字符
			int index = random.nextInt(characterSet.length());
			// 将选取的字符添加到StringBuilder中
			sb.append(characterSet.charAt(index));
		}
		// 将构建好的StringBuilder转换为String返回
		return sb.toString();
	}

	public static String generate17(int len){
		while(true) {
			String s = generateRandomString(len);
			if(MathUtils.murmurHash(s.hashCode())%32 == 17){
				return s;
			}
		}
	}

	public static String generate(int keyGroupIndex, int len){
		while(true) {
			String s = generateRandomString(len);
			if(MathUtils.murmurHash(s.hashCode())%32 == keyGroupIndex){
				return s;
			}
		}
	}

}
