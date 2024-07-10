package org.apache.flink.runtime.reConfig.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Title: LoadBanlancer
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.partition
 * @Date 2024/3/28 16:35
 * @description: 负载均衡器，接受负载信息，计算迁移结果
 */
public class LoadBanlancer {

	private Map<Integer, Long> sizeWindowInfo = new ConcurrentHashMap<>();  // 键组索引 -> 键组状态大小

	private Map<Integer, Integer> frequencyWindowInfo = new ConcurrentHashMap<>(); // 键组索引 -> 使用频次
	private Map<Integer, Integer> keyGroupMap = new ConcurrentHashMap<>(); // 键组索引 -> 实例索引
	private int taskCount; // 机器实例总数

	private final int threshHold = 10; // 访问频率小于这个值时不进行迁移

	public MigratePlan migratePlan = new MigratePlan(this);


	public void putSizeWindowInfo(Map<Integer, Long> sizeWindowInfo) {
		this.sizeWindowInfo.putAll(sizeWindowInfo);
	}

	public void putFrequencyWindowInfo(Map<Integer, Integer> frequencyWindowInfo) {
		this.frequencyWindowInfo.putAll(frequencyWindowInfo);
	}

	public void putKeyGroupMap(Map<Integer, Integer> keyGroupMap) {
		this.keyGroupMap.putAll(keyGroupMap);
	}

	public void setTaskCount(int taskCount) {
		System.out.println("taskCount:"+taskCount);
		this.taskCount = taskCount;
	}

	public MigratePlan computeMigratePlan(int splitNum){
		migratePlan.isFinish = false;
		double beforeBalance = computeLoadBalance(frequencyWindowInfo, keyGroupMap);
		Map<Integer, Integer> newKeyGroupMap = new HashMap<>(keyGroupMap);
		// 初始化机器负载信息
		Map<Integer, Integer> taskLoad = new HashMap<>();
		for (int i = 0; i < taskCount; i++) {
			taskLoad.put(i, 0); // task索引 -> 当前负载
		}

		// 按使用频次降序排序键组
		List<Map.Entry<Integer, Integer>> sortedKeyGroups = new ArrayList<>(frequencyWindowInfo.entrySet());
		sortedKeyGroups.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

		// 分配键组到机器
		for (Map.Entry<Integer, Integer> entry : sortedKeyGroups) {
			int keyGroupIndex = entry.getKey();
			int frequency = entry.getValue();
//			if(entry.getValue()<=threshHold){
//				newKeyGroupMap.put(keyGroupIndex, keyGroupMap.get(keyGroupIndex));
//				taskLoad.put(keyGroupMap.get(keyGroupIndex), taskLoad.get(keyGroupMap.get(keyGroupIndex)) + frequency);
//			}else {
				// 选择当前负载最低的机器
				int bestMachineIndex = chooseBestMachine(taskLoad, frequency);

				// 更新键组映射和机器负载
				newKeyGroupMap.put(keyGroupIndex, bestMachineIndex);
				taskLoad.put(bestMachineIndex, taskLoad.get(bestMachineIndex) + frequency);
//			}
		}
		double afterBalance = computeLoadBalance(frequencyWindowInfo, newKeyGroupMap);
		System.out.println("before load balance:"+beforeBalance+" after load balance:"+afterBalance);
		if(beforeBalance>0.9 || afterBalance - beforeBalance < 0.1){
			return migratePlan;
		}
		for(Map.Entry<Integer, Integer> newMapEntry : newKeyGroupMap.entrySet()){
			System.out.println("key:"+newMapEntry.getKey()+" value:"+newMapEntry.getValue());
			if(!Objects.equals(keyGroupMap.get(newMapEntry.getKey()), newMapEntry.getValue())){
				migratePlan.addEntry(new MigratePlanEntry(
					keyGroupMap.get(newMapEntry.getKey()),
					newMapEntry.getValue(),
					newMapEntry.getKey(),
					splitNum,
					sizeWindowInfo.get(newMapEntry.getKey())
				));
			}
		}
		return migratePlan;
	}

	public double computeLoadBalance(Map<Integer, Integer> frequencyMap, Map<Integer, Integer> keyGroupMap){
		int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
		int[] tmp = new int[taskCount];
		for(Map.Entry<Integer, Integer> frquencyEntry:frequencyMap.entrySet()){
			tmp[keyGroupMap.get(frquencyEntry.getKey())] += frquencyEntry.getValue();
		}
		for(int i=0;i<taskCount;++i){
			if(tmp[i]<min){
				min = tmp[i];
			}
			if(tmp[i]>max){
				max = tmp[i];
			}
		}
		//System.out.println("min:"+min+"max:"+max);
		return (double) min/max;
	}

	private int chooseBestMachine(Map<Integer, Integer> machineLoad, int frequency) {
		// 此处简化为选择当前负载最低的机器，实际可以根据需求调整选择逻辑
		return Collections.min(machineLoad.entrySet(), Map.Entry.comparingByValue()).getKey();
	}

	public void clear() {
		sizeWindowInfo.clear();
		frequencyWindowInfo.clear();
		keyGroupMap.clear();
		//migratePlan.clear();
	}

	public MigratePlanEntry increaseSave(int keyGroupIndex, int taskNum){
		int sp = 2;
		try {
			long stateSize = sizeWindowInfo.get(keyGroupIndex);
			while ((stateSize / sp) > MigratePlan.MAX_MIGRATE_STATE_SIZE) {
				sp *= 2;
			}
		}catch (NullPointerException e){
			//e.printStackTrace();
			throw new UnsupportedOperationException("key group is null");
		}
		return new MigratePlanEntry(keyGroupMap.get(keyGroupIndex), (keyGroupMap.get(keyGroupIndex)+1)%taskNum, keyGroupIndex, sp, sizeWindowInfo.get(keyGroupIndex));
	}

//	public static void main(String[] args) {
//		LoadBanlancer loadBanlancer = new LoadBanlancer();
//		Map<Integer, Integer> keyGroup = new HashMap<>();
//		keyGroup.put(0, 100);
//		keyGroup.put(1, 50);
//		keyGroup.put(2, 1200);
//		keyGroup.put(3, 130);
//		keyGroup.put(4, 40);
//		keyGroup.put(5, 120);
//		keyGroup.put(6, 800);
//		keyGroup.put(7, 900);
//		keyGroup.put(8, 200);
//		keyGroup.put(9, 1030);
//		keyGroup.put(10, 1800);
//		keyGroup.put(11, 200);
//		keyGroup.put(12, 1400);
//		keyGroup.put(13, 500);
//		keyGroup.put(14, 400);
//		keyGroup.put(15, 10);
//		Map<Integer, Integer> keyGroup2 = new HashMap<>();
//		keyGroup2.put(0, 0);
//		keyGroup2.put(1, 0);
//		keyGroup2.put(2, 0);
//		keyGroup2.put(3, 0);
//		keyGroup2.put(4, 1);
//		keyGroup2.put(5, 1);
//		keyGroup2.put(6, 1);
//		keyGroup2.put(7, 1);
//		keyGroup2.put(8, 2);
//		keyGroup2.put(9, 2);
//		keyGroup2.put(10, 2);
//		keyGroup2.put(11, 2);
//		keyGroup2.put(12, 3);
//		keyGroup2.put(13, 3);
//		keyGroup2.put(14, 3);
//		keyGroup2.put(15, 3);
//		loadBanlancer.setTaskCount(4);
//		loadBanlancer.setKeyGroupMap(keyGroup2);
//		loadBanlancer.setFrequencyWindowInfo(keyGroup);
//		MigratePlan migratePlan1 = loadBanlancer.computeMigratePlan(4);
//		migratePlan1.printInfo();
//	}

}
