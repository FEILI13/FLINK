package org.apache.flink.runtime.reConfig.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.runtime.state.heap.StateTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Map;

/**
 * @Title: ReconfigurableStateTable
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.state
 * @Date 2024/1/12 15:21
 * @description: 可重配置键值状态表
 */
public class ReconfigurableStateTable<K, N, S> extends StateTable<K, N, S> {

	private ReconfigurableStateManager<K, N, S> reconfigurableStateManager;//实际状态由ReconfigurableStateManager管理

	public ReconfigurableStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer) {
		super(keyContext, metaInfo, keySerializer);
		this.reconfigurableStateManager = new ReconfigurableStateManager<>(
			keySerializer,
			getMetaInfo().getNamespaceSerializer(),
			getMetaInfo().getStateSerializer()
		);
	}

	@Nonnull
	@Override
	public StateSnapshot stateSnapshot() {
		return null;
	}

	@Override
	protected StateMap<K, N, S> createStateMap() {
		return new ReconfigurableStateMap<>(getNamespaceSerializer(), keySerializer, getStateSerializer());
	}

	@Override
	public S get(N namespace) {
		return reconfigurableStateManager.get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public void put(K key, int keyGroup, N namespace, S state) {
		reconfigurableStateManager.put(key, keyGroup, namespace, state);
	}

	private StateMap<K,N,S> getMapFromManager(int keyGroupIndex, K key) {
		return reconfigurableStateManager.getMap(keyGroupIndex);
	}

	public void outputKeyGroups() {
		// TODO 输出键组信息
	}

	public void migrate(long version, int keyGroupIndex, int batch, int splitNum) {
		reconfigurableStateManager.migrateState(version, keyGroupIndex, batch, splitNum);
	}

	public void fetchState(long version, int keyGroupIndex, int batch, int splitNum) {
		reconfigurableStateManager.fetchState(version, keyGroupIndex, batch, splitNum);
	}

	public Map<Integer, Integer> getFrequencyWindowInfo(){
		return reconfigurableStateManager.getFrequencyWindowInfo();
	}

	public Map<Integer, Long> getSizeWindowInfo(boolean isALL){
		return reconfigurableStateManager.getSizeWindowInfo(isALL);
	}

	public void cleanStateWindow(){
		reconfigurableStateManager.cleanStateWindow();
	}

	public void cleanState() {
		reconfigurableStateManager.cleanState();
	}
}
