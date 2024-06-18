package org.apache.flink.runtime.reConfig.migrate;

import com.alibaba.fastjson2.JSON;

import com.alibaba.fastjson2.TypeReference;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.reConfig.utils.RedisUtil;
import org.apache.flink.runtime.reConfig.utils.SerializerUtil;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * @Title: MigrateManager
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.migrate
 * @Date 2024/1/13 13:32
 * @description: 状态迁移管理器
 */
public class MigrateManager<N, K, S> {

	private int splitNum = 4;

	private final TypeSerializer<K> keySerializer;

	private final TypeSerializer<N> namespaceSerializer;

	private final TypeSerializer<S> stateSerializer;

	public MigrateManager(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer) {
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.stateSerializer = stateSerializer;
	}

	/**
	 * 迁移key，value对
	 * @param key		键
	 * @param state		value
	 */
	public S migrate(K key, S state) {
		String serverAddress = "localhost";
		int serverPort = 8888;

		try (Socket socket = new Socket(serverAddress, serverPort)) {
			System.out.println("Connected to server: " + socket.getInetAddress());

			// Create ObjectOutputStream to send objects to the server
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

			// Send the object to the server
			oos.writeObject(key);
			oos.writeObject(state);
			//System.out.println("Sent object to server: " + myObject);

			// Create ObjectInputStream to receive modified object from the server
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

			// Read the modified object from the server
			S newValue = (S) ois.readObject();
			System.out.println("Received modified object from server: " + newValue);

			// Close the resources
			oos.close();
			ois.close();

			return newValue;

		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void migrate(int batch, Map<N, Map<K, S>> map) throws IOException {
		//RedisUtil.set("test"+batch, JSON.toJSONString(map));
		if(map == null){
			return;
		}
		byte[] key = SerializerUtil.serialize("test"+batch);
		RedisUtil.set(key, toBytes(map));
	}

	public void migrate(int keyGroupIndex, int batch, Map<N, Map<K, S>> map) throws IOException {
		//RedisUtil.set(keyGroupIndex+"-"+batch, JSON.toJSONString(map));
		//System.out.println("now migrating");
		byte[] key = SerializerUtil.serialize(keyGroupIndex+"-"+batch);
		if(map == null){
			RedisUtil.set(key, toBytes(new HashMap<>()), 5);
		}else {
			RedisUtil.set(key, toBytes(map), 5);
		}
	}

	public Map<N, Map<K,S>> fetchState(int batch) throws IOException {
		//return JSON.parseObject(RedisUtil.get("test" + i), new TypeReference<Map<N, Map<K,S>>>() {});
		byte[] key = SerializerUtil.serialize("test"+batch);
		byte[] value = RedisUtil.get(key);
		if(value == null){
			return null;
		}
		return fromBytes(value);
	}

	public Map<N, Map<K,S>> fetchState(int keyGroupIndex, int batch) throws IOException, InterruptedException {
		//return JSON.parseObject(RedisUtil.get("test" + i), new TypeReference<Map<N, Map<K,S>>>() {});
		byte[] key = SerializerUtil.serialize(keyGroupIndex+"-"+batch);
		while(!RedisUtil.exist(key)){
			Thread.sleep(2);//TODO 轮询获取状态，考虑是否可以优化
		}
		byte[] value = RedisUtil.get(key);
		if(value == null){
			return null;
		}
		return fromBytes(value);
	}

	public byte[] toBytes(Map<N, Map<K, S>> map) throws IOException {
		DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(calculateInitSize(map));

		dataOutputSerializer.writeInt(map.size());
		for(Map.Entry<N, Map<K, S>> entry : map.entrySet()){
			N namespace = entry.getKey();
			namespaceSerializer.serialize(namespace, dataOutputSerializer);
			Map<K, S> innerMap = entry.getValue();
			dataOutputSerializer.writeInt(innerMap.size());
			for(Map.Entry<K, S> innerEntry:innerMap.entrySet()){
				keySerializer.serialize(innerEntry.getKey(), dataOutputSerializer);
				stateSerializer.serialize(innerEntry.getValue(), dataOutputSerializer);
			}
		}

//		int numberOfEntry = 0;
//		for(Map<K, S> keyValue : map.values()){
//			numberOfEntry += keyValue.size();
//		}
//		dataOutputSerializer.writeInt(numberOfEntry);
//
//		for (Map.Entry<N, Map<K, S>> namespaceEntry : map.entrySet()) {
//			N namespace = namespaceEntry.getKey();
//			for (Map.Entry<K, S> entry : namespaceEntry.getValue().entrySet()) {
//				namespaceSerializer.serialize(namespace, dataOutputSerializer);
//				keySerializer.serialize(entry.getKey(), dataOutputSerializer);
//				stateSerializer.serialize(entry.getValue(), dataOutputSerializer);
//			}
//		}
		return dataOutputSerializer.getSharedBuffer();
	}

	public Map<N, Map<K, S>> fromBytes(byte[] data) throws IOException {
		Map<N, Map<K, S>> result = new HashMap<>();
		DataInputDeserializer dataInputDeserializer = new DataInputDeserializer(data);

		int mapSize = dataInputDeserializer.readInt();
		for(int i=0;i<mapSize;++i){
			N namespace = namespaceSerializer.deserialize(dataInputDeserializer);
			int innerMapSize = dataInputDeserializer.readInt();
			Map<K, S> innerMap = new HashMap<>();
			for(int j=0;j<innerMapSize;++j){
				K innerKey = keySerializer.deserialize(dataInputDeserializer);
				S innerValue = stateSerializer.deserialize(dataInputDeserializer);
				innerMap.put(innerKey, innerValue);
			}
			result.put(namespace, innerMap);
		}

//		int numberOfEntries = dataInputDeserializer.readInt();
//		for (int i = 0; i < numberOfEntries; i++) {
//			try {
//				N namespace = namespaceSerializer.deserialize(dataInputDeserializer);
//				K key = keySerializer.deserialize(dataInputDeserializer);
//				S state = stateSerializer.deserialize(dataInputDeserializer);
//				result.computeIfAbsent(namespace, k -> new HashMap<>())
//					.put(key, state);
//			} catch (Exception e) {
//				// TODO: window operator may fail here, fix it
//			}
//		}
		//System.out.println("receive result:"+result);
		return result;
	}

	private int calculateInitSize(Map<N, Map<K, S>> map) {
		// very roughly calculate the init size for Serialization buffer
		int size = 0;
		for (Map<K, S> kvMap : map.values()) {
			for (Map.Entry<K, S> ignored : kvMap.entrySet()) {
				size += 32; // assume 32
			}
		}
		return Math.max(size, 1); // at least 1 to make DataInputDeserializer happy
	}
}
