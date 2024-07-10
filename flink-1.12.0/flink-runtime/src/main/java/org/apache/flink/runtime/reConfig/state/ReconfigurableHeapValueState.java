package org.apache.flink.runtime.reConfig.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.AbstractHeapState;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @Title: ReconfigurableHeapValueState
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.state
 * @Date 2024/1/12 14:58
 * @description: 可重配置键值状态（堆实现）
 */
public class ReconfigurableHeapValueState<K, N, V>
	extends AbstractHeapState<K, N, V>
	implements InternalValueState<K, N, V> {

	ReconfigurableHeapValueState(
		StateTable<K, N, V> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		V defaultValue
	) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
		((ReconfigurableStateTable<?, ?, ?>) stateTable).outputKeyGroups();

	}

	/**
	 * 工厂方法构造ReconfigurableHeapValueState
	 * @param stateDesc		状态描述符
	 * @param stateTable	状态表
	 * @param keySerializer	key序列化器
	 * @return				ReconfigurableHeapValueState实例
	 * @param <K>			key类型
	 * @param <N>			namespace类型
	 * @param <SV>			state中value类型
	 * @param <S>			state类型
	 * @param <IS>			InternalState
	 */
	@SuppressWarnings("unchecked")
	public static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		StateTable<K, N, SV> stateTable,
		TypeSerializer<K> keySerializer){
		return (IS) new ReconfigurableHeapValueState<>(
			stateTable,
			keySerializer,
			stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			stateDesc.getDefaultValue()
		);
	}

	@Override
	public V value() throws IOException {
		final V result = stateTable.get(currentNamespace);

		if(result == null){
			return getDefaultValue();
		}

		return result;
	}

	@Override
	public void update(V value) throws IOException {
		if(value == null){
			clear();
			return;
		}

		stateTable.put(currentNamespace, value);
	}

	@Override
	public void migrate(long version, int keyGroupIndex, int batch, int splitNum) {
		((ReconfigurableStateTable)stateTable).migrate(version, keyGroupIndex, batch, splitNum);
	}

	@Override
	public void fetchState(long version, int keyGroupIndex, int batch, int splitNum) {
		((ReconfigurableStateTable)stateTable).fetchState(version, keyGroupIndex, batch, splitNum);
	}

	@Override
	public Map<Integer, Integer> getFrequencyWindowInfo() {
		return ((ReconfigurableStateTable)stateTable).getFrequencyWindowInfo();
	}

	@Override
	public Map<Integer, Long> getSizeWindowInfo(boolean isALL) {
		return ((ReconfigurableStateTable)stateTable).getSizeWindowInfo(isALL);
	}

	@Override
	public void cleanStateWindow() {
		((ReconfigurableStateTable)stateTable).cleanStateWindow();
	}

	@Override
	public void cleanState() {
		((ReconfigurableStateTable)stateTable).cleanState();
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}
}
