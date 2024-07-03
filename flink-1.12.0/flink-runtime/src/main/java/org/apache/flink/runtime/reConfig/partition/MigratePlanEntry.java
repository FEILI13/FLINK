package org.apache.flink.runtime.reConfig.partition;

/**
 * @Title: MigratePlanEntry
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.partition
 * @Date 2024/3/29 15:35
 * @description: 迁移方案的每个条目，代表每一次迁移
 */
public class MigratePlanEntry {

	private int sourceTask;
	private int targetTask;
	private int keyGroupIndex;
	private int splitNum;
	private long stateSize;

	private boolean bigEntry;

	private int batch;

	public MigratePlanEntry(
		int sourceTask,
		int targetTask,
		int keyGroupIndex,
		int splitNum,
		long stateSize) {
		this.sourceTask = sourceTask;
		this.targetTask = targetTask;
		this.keyGroupIndex = keyGroupIndex;
		this.splitNum = splitNum;
		this.stateSize = stateSize;
		this.bigEntry = false;
		this.batch = 0;
	}

	public boolean isBigEntry() {
		return bigEntry;
	}

	public void setBigEntry(boolean bigEntry) {
		this.bigEntry = bigEntry;
	}

	public int getSourceTask() {
		return sourceTask;
	}

	public void setSourceTask(int sourceTask) {
		this.sourceTask = sourceTask;
	}

	public int getTargetTask() {
		return targetTask;
	}

	public void setTargetTask(int targetTask) {
		this.targetTask = targetTask;
	}

	public int getKeyGroupIndex() {
		return keyGroupIndex;
	}

	public void setKeyGroupIndex(int keyGroupIndex) {
		this.keyGroupIndex = keyGroupIndex;
	}

	public int getSplitNum() {
		return splitNum;
	}

	public void setSplitNum(int splitNum) {
		this.splitNum = splitNum;
	}

	public long getStateSize() {
		return stateSize;
	}

	public void setStateSize(long stateSize) {
		this.stateSize = stateSize;
	}

	public int getBatch() {
		return batch;
	}

	public void addBatch(){
		this.batch++;
	}

	public void setBatch(int batch) {
		this.batch = batch;
	}

	@Override
	public String toString() {
		return "MigratePlanEntry{" +
			"sourceTask=" + sourceTask +
			", targetTask=" + targetTask +
			", keyGroupIndex=" + keyGroupIndex +
			", splitNum=" + splitNum +
			", stateSize=" + stateSize +
			'}';
	}
}
