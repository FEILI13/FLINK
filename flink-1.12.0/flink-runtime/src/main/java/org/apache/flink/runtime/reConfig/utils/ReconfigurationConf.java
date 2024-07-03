package org.apache.flink.runtime.reConfig.utils;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * @Title: ReconfigurationConf
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/1/20 16:33
 * @description: 配置文件，读取常量
 */
public class ReconfigurationConf {

	private static Properties properties = null;

	private static final String confFile = "/home/ftcl/reconfig.properties";

	public static final int SPLIT_NUM = 4;

	public ReconfigurationConf() {
	}

	public static Properties getProperties() {
		return properties;
	}

//	static {
//		try(FileInputStream input = new FileInputStream(confFile)){
//			properties = new Properties();
//			properties.load(input);
//		}catch (Exception e){
//			e.printStackTrace();
//		}
//	}
//
//	public static String getProperty(String key){
//		return properties.getProperty(key);
//	}

}
