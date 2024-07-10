package org.apache.flink.runtime.reConfig.utils;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.Acknowledge;

import java.util.HashSet;
import java.util.List;
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
	private Set<ExecutionAttemptID> rescalingTasks = new HashSet<>();
	CompletableFuture<Acknowledge> rescalingDeployingTasksFuture = new CompletableFuture<>();

	public CompletableFuture<Acknowledge> startNewReConfigAction(
		Set<ExecutionAttemptID> executionsToWaitFor,
		long reConfigActionID) {
		this.executionsToWaitFor = new HashSet<>(executionsToWaitFor);
		this.numExecutionsToWaitFor = executionsToWaitFor.size();
		this.reConfigActionID = reConfigActionID;
		this.future = new CompletableFuture<>();
		return future;
	}

	public void acknowledgeReConfig(ExecutionAttemptID executionAttemptID, long reConfigActionID) {
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

	public void acknowledgeDeploymentForRescaling(ExecutionAttemptID executionAttemptID) {
		notifyRescaleDeploymentCompleted(executionAttemptID);
	}

	public CompletableFuture<Acknowledge> notifyStartAsyncDeploymentForRescale(List<ExecutionVertex> executionVertices) {
		// TODO: one rescalingTasks and future for each rescaling action
		rescalingTasks.clear();
		for (ExecutionVertex executionVertex : executionVertices) {
			rescalingTasks.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
			System.out.println("waiting "+executionVertex.getCurrentExecutionAttempt().getAttemptId());
		}
		rescalingDeployingTasksFuture = new CompletableFuture<>();
		return rescalingDeployingTasksFuture;
	}

	public void notifyRescaleDeploymentCompleted(ExecutionAttemptID executionAttemptID) {
		rescalingTasks.remove(executionAttemptID);
		System.out.println(executionAttemptID + " ack");
		System.out.println(rescalingTasks.size());
		if (rescalingTasks.isEmpty()) {
			rescalingDeployingTasksFuture.complete(Acknowledge.get());
		}
	}

}
