package org.apache.flink.runtime.reConfig.message;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @Title: ReconfigSignal
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/2/28 21:24
 * @description: 重配置信号
 */
public class ReConfigSignal extends RuntimeEvent implements Serializable {

	public enum ReConfigSignalType{
		NONE, INIT, PERIOD_DETECT, PERIOD_RESULT, MIGRATE, MIGRATE_COMPLETE, CLEAN, CLEAN_COMPLETE, RESCALE_COMPLETE,
		INCREASE_STATE, INCREASE_RESULT, TEST;
	}

	private ReConfigSignalType type;
	private Payload payload = new Payload();

	private int messageType;//0代表询问状态

	private long version;//标识不同阶段的信号
	private int keyGroupIndex;

	private int splitNum;

	private int sourceTask;

	private int targetTask;

	private int batch;

	public ReConfigSignal() {
		this.type = ReConfigSignalType.NONE;
	}

	public ReConfigSignal(int messageType, long version) {
		this.messageType = messageType;
		this.version = version;
	}

	public ReConfigSignal(ReConfigSignalType type, long version){
		this.type = type;
		this.version = version;
	}

	public ReConfigSignal(
		int messageType,
		long version,
		int keyGroupIndex,
		int splitNum,
		int sourceTask,
		int targetTask,
		int batch) {
		this.messageType = messageType;
		this.version = version;
		this.keyGroupIndex = keyGroupIndex;
		this.splitNum = splitNum;
		this.sourceTask = sourceTask;
		this.targetTask = targetTask;
		this.batch = batch;
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(type.ordinal());
		out.writeLong(version);
		byte[] payloadData = payload.toBytes();
		out.writeInt(payloadData.length);
		out.write(payloadData);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.type = ReConfigSignalType.values()[in.readInt()];
		this.version = in.readLong();
		int n = in.readInt();
		byte[] payloadData = new byte[n];
		in.read(payloadData);
		try {
			this.payload = Payload.fromBytes(payloadData);
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public void putProperties(String key, Object value){
		payload.map.put(key, value);
	}

	public Object getProperties(String key){
		return payload.map.get(key);
	}

	public ReConfigSignalType getType() {
		return type;
	}

	public void setType(ReConfigSignalType type) {
		this.type = type;
	}

	public int getMessageType() {
		return messageType;
	}

	public long getVersion() {
		return version;
	}

	public int getKeyGroupIndex() {
		return keyGroupIndex;
	}

	public int getSplitNum() {
		return splitNum;
	}

	public int getSourceTask() {
		return sourceTask;
	}

	public int getTargetTask() {
		return targetTask;
	}

	public void setMessageType(int messageType) {
		this.messageType = messageType;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public void setKeyGroupIndex(int keyGroupIndex) {
		this.keyGroupIndex = keyGroupIndex;
	}

	public void setSplitNum(int splitNum) {
		this.splitNum = splitNum;
	}

	public void setSourceTask(int sourceTask) {
		this.sourceTask = sourceTask;
	}

	public void setTargetTask(int targetTask) {
		this.targetTask = targetTask;
	}

	public int getBatch() {
		return batch;
	}

	public void setBatch(int batch) {
		this.batch = batch;
	}

	@Override
	public String toString() {
		return "ReConfigSignal{" +
			"type=" + type +
			", payload=" + payload +
			", messageType=" + messageType +
			", version=" + version +
			", keyGroupIndex=" + keyGroupIndex +
			", splitNum=" + splitNum +
			", sourceTask=" + sourceTask +
			", targetTask=" + targetTask +
			", batch=" + batch +
			'}';
	}

	public static class Payload implements Serializable{
		Map<String, Object> map = new HashMap<>();

		public Payload(){}

		public byte[] toBytes() throws IOException{
			ByteArrayOutputStream bis = new ByteArrayOutputStream();
			ObjectOutputStream ois = new ObjectOutputStream(bis);
			ois.writeObject(this);
			ois.close();
			bis.close();
			return bis.toByteArray();
		}

		public static Payload fromBytes(byte[] data) throws IOException, ClassNotFoundException{
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInputStream ois = new ObjectInputStream(bis);
			Object obj = ois.readObject();
			ois.close();
			bis.close();
			return (Payload) obj;
		}
	}
}
