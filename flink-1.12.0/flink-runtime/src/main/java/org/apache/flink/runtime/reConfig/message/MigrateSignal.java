package org.apache.flink.runtime.reConfig.message;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.io.Serializable;

/**
 * @Title: MigrateSignal
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/3/6 21:05
 * @description: 迁移指令
 */
public class MigrateSignal extends RuntimeEvent implements Serializable {

	private int sourceTask;

	private int targetTask;

	private int kegGroupIndex;

	private int batch;

	private int splitNum;

	private long version;//标识不同阶段的信号

	public MigrateSignal() {
	}

	public MigrateSignal(int sourceTask, int targetTask, int kegGroupIndex, int batch, int splitNum, long version) {
		this.sourceTask = sourceTask;
		this.targetTask = targetTask;
		this.kegGroupIndex = kegGroupIndex;
		this.batch = batch;
		this.splitNum = splitNum;
		this.version = version;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(sourceTask);
		out.writeInt(targetTask);
		out.writeInt(kegGroupIndex);
		out.writeInt(batch);
		out.writeInt(splitNum);
		out.writeLong(version);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.sourceTask = in.readInt();
		this.targetTask = in.readInt();
		this.kegGroupIndex = in.readInt();
		this.batch = in.readInt();
		this.splitNum = in.readInt();
		this.version = in.readLong();
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

	public int getKegGroupIndex() {
		return kegGroupIndex;
	}

	public void setKegGroupIndex(int kegGroupIndex) {
		this.kegGroupIndex = kegGroupIndex;
	}

	public int getBatch() {
		return batch;
	}

	public void setBatch(int batch) {
		this.batch = batch;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public int getSplitNum() {
		return splitNum;
	}

	public void setSplitNum(int splitNum) {
		this.splitNum = splitNum;
	}
}
