/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Partitioner selects the target channel based on the key group index.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
@Internal
public class KeyGroupStreamPartitioner<T, K> extends StreamPartitioner<T> implements ConfigurableStreamPartitioner {
	private static final long serialVersionUID = 1L;

	private final KeySelector<T, K> keySelector;

	private int maxParallelism;

	private Map<Integer, Integer> routeTable;

	private volatile int migratingKeyGroup = -1;

	private volatile int migratingBatch = -1;

	private volatile int splitNum = -1;

	private volatile int targetIndex = -1;

	public KeyGroupStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.maxParallelism = maxParallelism;
		this.routeTable = new HashMap<>();
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	/**
	 * 先检查是否是当前正在迁移的键组，根据批次路由，再检查是否是已经被修改的路由（保存在哈希表中），都不是的话再按默认路由策略
	 * @param record the record to determine the output channels for.
	 * @return 目标实例
	 */
	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		K key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
			int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
			if(migratingKeyGroup==keyGroupIndex && (key.hashCode()&(splitNum-1))<=migratingBatch){// 是否是当前迁移的
				return targetIndex;
			} else if(routeTable.containsKey(keyGroupIndex)){
				return routeTable.get(keyGroupIndex);
			}
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels);
	}

	@Override
	public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
		return SubtaskStateMapper.RANGE;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "HASH";
	}

	@Override
	public void configure(int maxParallelism) {
		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		this.maxParallelism = maxParallelism;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		final KeyGroupStreamPartitioner<?, ?> that = (KeyGroupStreamPartitioner<?, ?>) o;
		return maxParallelism == that.maxParallelism &&
			keySelector.equals(that.keySelector);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), keySelector, maxParallelism);
	}

	@Override
	public void updateControl(int keyGroupIndex, int targetIndex, int batch, int splitNum) {
		this.migratingKeyGroup = keyGroupIndex;
		this.targetIndex = targetIndex;
		this.migratingBatch = batch;
		this.splitNum = splitNum;
		System.out.println("sync update route");
	}

	@Override
	public void cleanRouting() {
		routeTable.put(migratingKeyGroup, targetIndex);
		this.targetIndex = -1;
		this.migratingKeyGroup = -1;
		this.splitNum = -1;
		this.migratingBatch = -1;
	}
}
