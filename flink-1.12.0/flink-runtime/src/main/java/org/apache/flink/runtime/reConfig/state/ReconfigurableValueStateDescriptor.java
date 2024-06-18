package org.apache.flink.runtime.reConfig.state;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * @Title: ReconfigurableValueStateDescriptor
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.state
 * @Date 2024/1/12 16:17
 * @description: 可重配置 value状态 描述符
 */
public class ReconfigurableValueStateDescriptor<T> extends ValueStateDescriptor {

	public ReconfigurableValueStateDescriptor(String name, Class<T> typeClass, T defaultValue){
		super(name, typeClass, defaultValue);
	}

	/**
	 * 构造器
	 * @param name		状态名（唯一）
	 * @param typeInfo	类型信息
	 */
	public ReconfigurableValueStateDescriptor(String name, TypeInformation<T> typeInfo){
		super(name, typeInfo);
	}

}
