package org.apache.flink.runtime.reConfig.partition;


import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @Title: MigratePlan
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.partition
 * @Date 2024/3/29 15:23
 * @description: 状态迁移的方案
 */
public class MigratePlan {
	LinkedList<MigratePlanEntry> migratePlanEntryList;

	boolean isFinish;

	private LoadBanlancer loadBanlancer;

	public static final int MAX_MIGRATE_STATE_SIZE = 22_000_000;// 200Mb = 26_214_400B

	public MigratePlan(LoadBanlancer loadBanlancer){
		this.loadBanlancer = loadBanlancer;
		this.migratePlanEntryList = new LinkedList<>();
		this.isFinish = true;
	}

	public boolean isFinish(){
		return isFinish;
	}

	public void addEntry(MigratePlanEntry entry){
		isFinish = false;
		migratePlanEntryList.add(entry);
	}

	public boolean haveNext(){
		return migratePlanEntryList.isEmpty();
	}

	public MigratePlanEntry getMigratePlanEntry(){
		return migratePlanEntryList.removeFirst();
	}

	public void clear(){
		this.migratePlanEntryList.clear();
		isFinish = true;
	}

	public void printInfo(){
		System.out.println("==============Migrate Plan=============");
		long currentSize = 0L;
		int cur = 0;
		MigratePlanEntry entry;
		while(cur<migratePlanEntryList.size() && currentSize<MAX_MIGRATE_STATE_SIZE){// 迁移状态量小于200Mb为一轮
			entry = migratePlanEntryList.get(cur++);
			currentSize += entry.getStateSize();
			System.out.println(entry);
		}
//		for(MigratePlanEntry entry:migratePlanEntryList){
//			System.out.println(entry);
//		}
	}

	public List<MigratePlanEntry> oneRound() {
		List<MigratePlanEntry> list = new ArrayList<>();
		long currentSize = 0L;
		while(!migratePlanEntryList.isEmpty() && currentSize<MAX_MIGRATE_STATE_SIZE){// 迁移状态量小于200Mb为一轮
			MigratePlanEntry entry = migratePlanEntryList.getFirst();
			if(currentSize + entry.getStateSize() >= MAX_MIGRATE_STATE_SIZE){
				break;
			}
			currentSize += entry.getStateSize();
			list.add(migratePlanEntryList.removeFirst());
		}
		if(currentSize == 0L && !migratePlanEntryList.isEmpty()){
			System.out.println("have one big entry");
			list.add(migratePlanEntryList.removeFirst());
			list.get(0).setBigEntry(true);
			int splitNum = list.get(0).getSplitNum();
			long size = list.get(0).getStateSize();

			do {
				splitNum *= 2;
			}while(size/splitNum>MAX_MIGRATE_STATE_SIZE);
			System.out.println("final splitNum:"+splitNum);
			list.get(0).setSplitNum(splitNum);
			return list;
		}
		if(migratePlanEntryList.isEmpty()){
			loadBanlancer.clear();
			isFinish = true;
		}else{
			System.out.println("one round cant migrate, leave another round");
		}
		return list;
	}

	public List<MigratePlanEntry> allRound(){
		List<MigratePlanEntry> list = new ArrayList<>();
		while(!migratePlanEntryList.isEmpty()){
			MigratePlanEntry entry = migratePlanEntryList.removeFirst();
			int splitNum = 1;
			while(entry.getStateSize()/splitNum>MAX_MIGRATE_STATE_SIZE){
				splitNum *= 2;
			}
			entry.setSplitNum(splitNum);
			list.add(entry);
		}
		isFinish = true;
		return list;
	}
}
