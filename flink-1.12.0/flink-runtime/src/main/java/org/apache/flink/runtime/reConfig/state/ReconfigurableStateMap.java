package org.apache.flink.runtime.reConfig.state;

import com.alibaba.fastjson2.JSON;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.runtime.state.heap.StateMapSnapshot;
import org.apache.flink.runtime.state.internal.InternalKvState;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @Title: ReconfigurableStateMap
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.state
 * @Date 2024/1/12 18:27
 * @description: 可重配置状态表 <namespace, key, value>三元组实际存储位置

 * @param <K>	Type of key
 * @param <N>	Type of namespace
 * @param <S>	Type of value
 */
public class ReconfigurableStateMap<K, N, S> extends StateMap<K, N, S> {

	private final Map<N, Map<K, S>> namespcaeMap;//状态存储位置

	private TypeSerializer<N> namespaceSerializer;

	private TypeSerializer<K> keySerializer;

	private TypeSerializer<S> stateSerializer;

	public ReconfigurableStateMap(
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<K> keySerializer,
		TypeSerializer<S> stateSerializer) {
		this.namespaceSerializer = namespaceSerializer;
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
		this.namespcaeMap = new HashMap<>();
	}

	@Override
	public int size() {
		int size = 0;
		for(Map<K, S> map:namespcaeMap.values()){
			size += map.size();
		}
		return size;
	}

	@Override
	public S get(K key, N namespace) {
		Map<K, S> keyedMap = namespcaeMap.get(namespace);
		if(keyedMap == null) {
			return null;
		}
		return keyedMap.get(key);
	}

	@Override
	public boolean containsKey(K key, N namespace) {
		return false;
	}

	@Override
	public void put(K key, N namespace, S state) {
		Map<K, S> keyedMap = namespcaeMap.computeIfAbsent(namespace, k -> new HashMap<>());
		keyedMap.put(key, state);
	}

	@Override
	public S putAndGetOld(K key, N namespace, S state) {
		return null;
	}

	@Override
	public void remove(K key, N namespace) {

	}

	@Override
	public S removeAndGetOld(K key, N namespace) {
		return null;
	}

	@Override
	public <T> void transform(
		K key,
		N namespace,
		T value,
		StateTransformationFunction<S, T> transformation) throws Exception {

	}

	@Override
	public Stream<K> getKeys(N namespace) {
		return null;
	}

	@Override
	public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		return null;
	}

	@Nonnull
	@Override
	public StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> stateSnapshot() {
		return null;
	}

	@Override
	public int sizeOfNamespace(Object namespace) {
		return 0;
	}

	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		return new StateEntryIterator();
	}

	@Override
	public void merge(Map<N, Map<K, S>> nMapMap) {
//		System.out.println("before namespace:"+JSON.toJSONString(namespcaeMap));
//		System.out.println("before set:"+namespcaeMap.keySet());
//		System.out.println("before nMapMap:"+JSON.toJSONString(nMapMap));
		nMapMap.forEach(
			(namespace, keyValuePair) -> {
				namespcaeMap.merge(namespace, keyValuePair, (oldMap, newMap) -> {
					oldMap.putAll(newMap);
					return oldMap;
				});
			}
		);
//		System.out.println("after namespace:"+JSON.toJSONString(namespcaeMap));
//		System.out.println("after set:"+namespcaeMap.keySet());
//		System.out.println("after nMapMap:"+JSON.toJSONString(nMapMap));
	}

	/**
	 * 状态表迭代器
	 */
	class StateEntryIterator implements Iterator<StateEntry<K, N, S>>{

		private Iterator<Map.Entry<N, Map<K, S>>> namespaceIterator;

		private Map.Entry<N, Map<K, S>> namespace;

		private Iterator<Map.Entry<K, S>> keyValueIterator;

		public StateEntryIterator() {
			namespaceIterator = namespcaeMap.entrySet().iterator();
			namespace = null;
			keyValueIterator = Collections.emptyIterator();
		}

		@Override
		public boolean hasNext() {
			return keyValueIterator.hasNext() || namespaceIterator.hasNext();
		}

		@Override
		public StateEntry<K, N, S> next() {
			if(!hasNext()){
				throw new RuntimeException("迭代器为空");
			}
			if(!keyValueIterator.hasNext()){
				namespace = namespaceIterator.next();
				keyValueIterator = namespace.getValue().entrySet().iterator();
			}

			Map.Entry<K, S> entry = keyValueIterator.next();

			return new StateEntry.SimpleStateEntry<>(entry.getKey(), namespace.getKey(),
				entry.getValue());
		}
	}
}
