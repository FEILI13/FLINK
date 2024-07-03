package org.apache.flink.runtime.reConfig.utils;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.io.Serializable;

/**
 * @Title: InstanceState
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/3/1 19:18
 * @description: 节点状态信息
 */
public class InstanceState extends RuntimeEvent implements Serializable {

	private int taskIndex;
	private int recordCount;

	public InstanceState() {
	}

	public InstanceState(int taskIndex, int recordCount) {
		this.taskIndex = taskIndex;
		this.recordCount = recordCount;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(taskIndex);
		out.writeInt(recordCount);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.taskIndex = in.readInt();
		this.recordCount = in.readInt();
	}

	public int getTaskIndex() {
		return taskIndex;
	}

	public void setTaskIndex(int taskIndex) {
		this.taskIndex = taskIndex;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}
}
