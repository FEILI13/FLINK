package org.apache.flink.runtime.reConfig.utils;

import java.security.SecureRandom;

/**
 * @Title: RandomStringGenerator
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/1/16 14:52
 * @description: 随机长度字符串生成器
 */
public class RandomStringGenerator {

	private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
	private static final SecureRandom secureRandom = new SecureRandom();

	/**
	 * 生成指定长度的随机字符串
	 *
	 * @param length 需要生成的字符串长度
	 * @return 随机字符串
	 */
	public static String generateRandomString(int length) {
		if (length <= 0) {
			throw new IllegalArgumentException("Length must be greater than 0");
		}

		StringBuilder stringBuilder = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			int randomIndex = secureRandom.nextInt(CHARACTERS.length());
			char randomChar = CHARACTERS.charAt(randomIndex);
			stringBuilder.append(randomChar);
		}

		return stringBuilder.toString();
	}


}
