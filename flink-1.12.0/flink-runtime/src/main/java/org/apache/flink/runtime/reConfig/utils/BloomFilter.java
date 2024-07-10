package org.apache.flink.runtime.reConfig.utils;

import java.util.BitSet;
import java.util.function.Function;

/**
 * @Title: BloomFilter
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/3/28 14:46
 * @description: 布隆过滤器，判断key是否出现过，用于统计状态
 */
public class BloomFilter<T> {
	private final BitSet bitSet;
	private final int size;
	private final Function<T, Integer>[] hashFunctions;

	@SuppressWarnings("unchecked")
	public BloomFilter(int size, Function<T, Integer>... hashFunctions) {
		this.bitSet = new BitSet(size);
		this.size = size;
		this.hashFunctions = hashFunctions;
	}

	public void add(T element) {
		for (Function<T, Integer> hashFunction : hashFunctions) {
			int hash = Math.abs(hashFunction.apply(element)) % size;
			bitSet.set(hash, true);
		}
	}

	public boolean mightContain(T element) {
		for (Function<T, Integer> hashFunction : hashFunctions) {
			int hash = Math.abs(hashFunction.apply(element)) % size;
			if (!bitSet.get(hash)) {
				return false;
			}
		}
		return true;
	}

	public void clear() {
		bitSet.clear();
	}
}
