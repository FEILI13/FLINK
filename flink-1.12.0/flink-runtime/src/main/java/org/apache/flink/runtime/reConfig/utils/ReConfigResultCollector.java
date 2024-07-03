package org.apache.flink.runtime.reConfig.utils;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @Title: ReConfigResultCollector
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.utils
 * @Date 2024/3/21 19:26
 * @description: 异步结果收集器
 */
public class ReConfigResultCollector {
	private Set<ExecutionAttemptID> executionsToWaitFor;
	private int numExecutionsToWaitFor = -1;
	private long reConfigActionID = -1;
	private CompletableFuture<Acknowledge> future;

	public CompletableFuture<Acknowledge> startNewRescaleAction(
		Set<ExecutionAttemptID> executionsToWaitFor,
		long reConfigActionID) {
		this.executionsToWaitFor = new HashSet<>(executionsToWaitFor);
		this.numExecutionsToWaitFor = executionsToWaitFor.size();
		this.reConfigActionID = reConfigActionID;
		this.future = new CompletableFuture<>();
		return future;
	}

	public void acknowledgeRescale(ExecutionAttemptID executionAttemptID, long reConfigActionID) {
		synchronized (this) {
			if (reConfigActionID == this.reConfigActionID) {
				if (executionsToWaitFor.contains(executionAttemptID)) {
					executionsToWaitFor.remove(executionAttemptID);
					numExecutionsToWaitFor--;
//						System.out.println("numExecutionsToWaitFor" + numExecutionsToWaitFor);
					if (numExecutionsToWaitFor <= 0) {
						future.complete(Acknowledge.get());
					}
				}
			}
		}
	}
}
