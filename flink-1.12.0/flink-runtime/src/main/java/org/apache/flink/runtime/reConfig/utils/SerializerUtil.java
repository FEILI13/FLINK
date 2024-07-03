package org.apache.flink.runtime.reConfig.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @Title: SerializerUtil
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/1/16 14:18
 * @description: 对象序列化反序列化器
 */
public class SerializerUtil {

	/**
	 * 序列化对象为字节数组
	 *
	 * @param object 待序列化的对象
	 * @return 字节数组
	 */
	public static byte[] serialize(Object object) {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
			 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			oos.writeObject(object);
			return bos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException("Error serializing object", e);
		}
	}

	/**
	 * 反序列化字节数组为对象
	 *
	 * @param bytes 字节数组
	 * @return 反序列化后的对象
	 */
	public static Object deserialize(byte[] bytes) {
		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			 ObjectInputStream ois = new ObjectInputStream(bis)) {
			return ois.readObject();
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException("Error deserializing object", e);
		}
	}

}
