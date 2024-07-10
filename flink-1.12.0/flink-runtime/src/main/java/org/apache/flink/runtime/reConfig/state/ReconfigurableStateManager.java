package org.apache.flink.runtime.reConfig.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.reConfig.migrate.MigrateManager;
import org.apache.flink.runtime.reConfig.utils.BloomFilter;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.heap.StateMap;

import org.openjdk.jol.info.GraphLayout;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Title: ReconfigurableStateManager
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.state
 * @Date 2024/1/13 10:02
 * @description: 可重配置状态 管理器（保存处理状态）
 */
public class ReconfigurableStateManager<K, N, S> {

	/** 映射关系：键组->状态 */
	private Map<Integer, StateMap<K, N, S>> keyGroupStateMaps;

	private final TypeSerializer<K> keySerializer;

	private final TypeSerializer<N> namespaceSerializer;

	private final TypeSerializer<S> stateSerializer;

	private MigrateManager<N, K, S> migrateManager;
	/** 键组使用频率表 */
	private Map<Integer, Integer> frequencyMap;
	private int maxKeyGroup = 0;
	private int maxKeyGroupCount = 0;
	/** 迁移某个键组时，将其状态细粒度划分，每个粒度对应一个namespace状态表 */
	private Map<Integer, Map<N, Map<K, S>>> tmpMap;
	/** 当前分割数 */
	private Integer currentSplitNum;
	/** 当前迁移键组序号 */
	private Integer currentMigrateKeyGroup = -1;

	private boolean haveTrigger = false;//是否触发过状态迁移

	/** 统计每个窗口接受的状态量大小 **/
	private Map<Integer, Integer> frequenctWindow;
	/** 统计每个键组的状态大小 **/
	private Map<Integer, Long> sizeWindow;
	/** 统计出现过的key **/
	private BloomFilter<K> bloomFilter;

	public ReconfigurableStateManager(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer) {
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.stateSerializer = stateSerializer;
		this.keyGroupStateMaps = new HashMap<>();
		this.migrateManager = new MigrateManager(keySerializer, namespaceSerializer, stateSerializer);
		this.frequencyMap = new HashMap<>();
		this.tmpMap = new HashMap<>();
		this.currentSplitNum = 4;
		this.frequenctWindow = new HashMap<>();
		this.sizeWindow = new HashMap<>();
		this.bloomFilter = new BloomFilter<>(4096, s -> s.hashCode(), s -> s.hashCode() ^ (s.hashCode()<<16));
		//this.currentSplitNum = Integer.valueOf(ReconfigurationConf.getProperty("splitNum"));
//		for(int i=0;i<128;++i){
//			keyGroupStateMaps.put(i, new ReconfigurableStateMap<>(ke));
//		}
	}

	/**
	 * 获取键组对应的状态
	 * @param keyGroupIndex	键组索引
	 * @return				状态map
	 */
	public StateMap<K,N,S> getMap(int keyGroupIndex) {
		if(keyGroupStateMaps.get(keyGroupIndex) == null){
			keyGroupStateMaps.put(keyGroupIndex, new ReconfigurableStateMap<>(namespaceSerializer, keySerializer ,stateSerializer));
		}
		return keyGroupStateMaps.get(keyGroupIndex);
	}

	/**
	 * 获取状态（value值）
	 * @param key			key
	 * @param keyGroupIndex	键组
	 * @param namespace				namespace
	 * @return						value值
	 */
	public S get(K key, int keyGroupIndex, N namespace) {
		if(keyGroupIndex == currentMigrateKeyGroup){// 对应状态存储在tmpMap中
			if(tmpMap.get(key.hashCode() & (currentSplitNum - 1))==null||tmpMap.get(key.hashCode() & (currentSplitNum - 1)).get(namespace)==null) {
				return null;
			}
			return tmpMap.get(key.hashCode() & (currentSplitNum - 1)).get(namespace).get(key);
		}
		StateMap<K, N, S> stateMap = keyGroupStateMaps.get(keyGroupIndex);
		if(stateMap == null){
			return null;
		}
		return stateMap.get(key, namespace);
	}

	/**
	 * 放置状态到状态表中
	 * @param key		键
	 * @param keyGroup	键组索引
	 * @param namespace	namespace
	 * @param state		value值
	 */
	public void put(K key, int keyGroup, N namespace, S state) {
//		if(!bloomFilter.mightContain(key)){
//			bloomFilter.add(key);
//			sizeWindow.put(keyGroup, sizeWindow.getOrDefault(keyGroup, 0L)+GraphLayout.parseInstance(state).totalSize());
//		}
		frequenctWindow.put(keyGroup, frequenctWindow.getOrDefault(keyGroup, 0)+1);

		if(keyGroup == currentMigrateKeyGroup){// 对应状态存储在tmpMap中
			Map<K, S> ksMap = tmpMap
				.get(key.hashCode() & (currentSplitNum - 1))
				.computeIfAbsent(namespace, k -> new HashMap<>());
			ksMap.put(key, state);
			return;
		}
		//System.out.println("key:"+key+" keyGroup:"+keyGroup);
		StateMap<K, N, S> stateMap = getMap(keyGroup);
		stateMap.put(key, namespace, state);
		if(stateMap.size()>maxKeyGroupCount){
			maxKeyGroup = keyGroup;
			maxKeyGroupCount = stateMap.size();
		}
		//System.out.println("key:"+key+" keygroup:"+keyGroup+" batch:"+(key.hashCode()&3));
		//TODO 考虑将统计状态功能放到哪里
//		frequencyMap.put(keyGroup, frequencyMap.getOrDefault(keyGroup, 0)+1);
//		//System.out.println("current split num:"+currentSplitNum);
//
//		if(frequencyMap.get(keyGroup)==1_0000 && keyGroup<2 && !haveTrigger){
//			try {
//				haveTrigger = true;
//				long beforeTime = System.currentTimeMillis();
//				currentMigrateKeyGroup = keyGroup;
//				splitKeyGroup(keyGroup, currentSplitNum);
//				removeKeyGroup(keyGroup);
//				migrateState();
//				long totalTime = System.currentTimeMillis() - beforeTime;
//				System.out.println("total time:"+totalTime);
////				fetchState();
////				mergeState(keyGroup);
//			}catch (Exception e){
//				e.printStackTrace();
//			}
//		}
	}

	/**
	 * 切分状态表为多份，保存在tmpMap中，TODO 考虑窗口是否适用
	 * 拷贝键组索引对应的状态，将其分割到tmpMap中
	 * @param keyGroupIndex	键组索引
	 * @param splitNum		分割数(2的幂次方)
	 */
	public void splitKeyGroup(Integer keyGroupIndex, Integer splitNum){
		currentMigrateKeyGroup = keyGroupIndex;
		currentSplitNum = splitNum;
		StateMap<K, N, S> currentStateMap = keyGroupStateMaps.get(keyGroupIndex);
		tmpMap.clear();

		long sizeInBytes = GraphLayout.parseInstance(currentStateMap).totalSize();
		System.out.println("total state size:"+sizeInBytes+"B");

		for (StateEntry<K, N, S> entry : currentStateMap) {
			//logger.info("key:{} namespace:{} state:{}", entry.getKey(), entry.getNamespace(), entry.getState());
			//System.out.println(entry.getNamespace().getClass());
			int key = entry.getKey().hashCode() & (splitNum - 1);
			tmpMap.computeIfAbsent(key, k -> new HashMap<>())
				.computeIfAbsent(entry.getNamespace(), n -> new HashMap<>())
				.put(entry.getKey(), entry.getState());
		}
		keyGroupStateMaps.remove(keyGroupIndex);
		//logger.info("tmpMap:{}", tmpMap);
	}

	/**
	 * 迁移tmpMap
	 */
	public void migrateState() throws IOException {
		for(int i:tmpMap.keySet()){
			long beforeTime = System.currentTimeMillis();
			//System.out.println("redis object"+JSON.toJSONString(tmpMap.get(i)));
			migrateManager.migrate(i, tmpMap.get(i));
			long totalTime = System.currentTimeMillis() - beforeTime;
			System.out.println("batch " + i + " cost time " + totalTime);
		}
	}

	/**
	 * 分批迁移tmpMap
	 * @param batch		第batch批次
	 */
	public void migrateState(int batch) throws IOException{
		migrateManager.migrate(batch, tmpMap.get(batch));
	}

	public void migrateState(long version, int keyGroupIndex, int batch, int splitNum){
		if(tmpMap.size()==0){
			splitKeyGroup(keyGroupIndex, splitNum);
		}
		try {
			migrateManager.migrate(version, keyGroupIndex, batch, tmpMap.get(batch));
		}catch (IOException e){
			e.printStackTrace();
		}
	}

	public void fetchState() throws IOException {
		tmpMap = new HashMap<>();
		for(int i=0;i<currentSplitNum;++i){
			Map<N, Map<K, S>> fetchData = migrateManager.fetchState(i);
			if(fetchData!=null) {
				tmpMap.put(i, fetchData);
				StateMap<K, N, S> map = getMap(currentMigrateKeyGroup);

			}
			System.out.println("get state:" + tmpMap.get(i));
		}
	}

	public void fetchState(long version, int keyGroupIndex, int batch, int splitNum){
//		if(tmpMap.size()==0) {
//			currentMigrateKeyGroup = keyGroupIndex;
//			currentSplitNum = splitNum;
//			tmpMap.clear();
//		}
		try {
			Map<N, Map<K, S>> fetchData = migrateManager.fetchState(version, keyGroupIndex, batch);
			//if(fetchData!=null) {
//				tmpMap.put(batch, fetchData);
//			for(N n:fetchData.keySet()){
//				System.out.println("batch "+batch+" state count:"+fetchData.get(n).size());
//			}
			long sizeInBytes = GraphLayout.parseInstance(fetchData).totalSize();
			System.out.println("batch "+batch+" state size:"+sizeInBytes+"B");
			keyGroupStateMaps.putIfAbsent(keyGroupIndex, new ReconfigurableStateMap<>(namespaceSerializer, keySerializer ,stateSerializer));
			StateMap<K, N, S> stateMap = keyGroupStateMaps.get(keyGroupIndex);
			stateMap.merge(fetchData);
			//}
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public void mergeState(int keyGroupIndex){
		for(int i=0;i<currentSplitNum;++i){
			Map<N, Map<K, S>> nMapMap = tmpMap.get(i);
			//logger.info("ith batch fetched Map<N, Map<K, S>>:{}", nMapMap);
			if(nMapMap!=null) {
				keyGroupStateMaps.get(keyGroupIndex).merge(nMapMap);
			}
		}
	}

	public Map<Integer, Integer> getFrequencyWindowInfo(){
//		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//		for(Map.Entry<Integer, Integer> entry:frequenctWindow.entrySet()){
//			System.out.println("keyGroup:"+entry.getKey()+" fre:"+entry.getValue());
//		}
		return frequenctWindow;
	}

	public Map<Integer, Long> getSizeWindowInfo(boolean isALL){
		if(isALL){
			for(Integer keyGroupIndex:keyGroupStateMaps.keySet()){
				//System.out.println("keyGroupIndex:"+keyGroupIndex);
				sizeWindow.put(keyGroupIndex, GraphLayout.parseInstance(keyGroupStateMaps.get(keyGroupIndex)).totalSize());
			}
		} else{
			for(Integer keyGroupIndex:frequenctWindow.keySet()){
				try {
					sizeWindow.put(keyGroupIndex, GraphLayout.parseInstance(keyGroupStateMaps.get(keyGroupIndex)).totalSize());
				}catch (Exception e){
					sizeWindow.remove(keyGroupIndex);
				}
			}
		}
		return sizeWindow;
	}

	public void cleanStateWindow(){
		frequenctWindow.clear();
		sizeWindow.clear();
		bloomFilter.clear();
	}

	public void removeKeyGroup(int keyGroup){
		keyGroupStateMaps.remove(keyGroup);
	}

	public void cleanState() {
		if(tmpMap.size()!=0){
			tmpMap.clear();
		}
		currentSplitNum = -1;
		currentMigrateKeyGroup = -1;
	}
}
