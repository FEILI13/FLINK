/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of the {@link TaskManagerGateway} for Flink's RPC system.
 */
public class RpcTaskManagerGateway implements TaskManagerGateway {

	static final Logger LOG = LoggerFactory.getLogger(RpcTaskManagerGateway.class);
	private final TaskExecutorGateway taskExecutorGateway;

	private final JobMasterId jobMasterId;

	public RpcTaskManagerGateway(TaskExecutorGateway taskExecutorGateway, JobMasterId jobMasterId) {
		this.taskExecutorGateway = Preconditions.checkNotNull(taskExecutorGateway);
		this.jobMasterId = Preconditions.checkNotNull(jobMasterId);
	}

	@Override
	public String getAddress() {
		return taskExecutorGateway.getAddress();
	}

	@Override
	public CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(
			ExecutionAttemptID executionAttemptID,
			int requestId,
			Time timeout) {

		return taskExecutorGateway.requestTaskBackPressure(executionAttemptID, requestId, timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
		LOG.info("RpcTaskManagerGateway submitTask");
		return taskExecutorGateway.submitTask(tdd, jobMasterId, timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return taskExecutorGateway.cancelTask(executionAttemptID, timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		return taskExecutorGateway.updatePartitions(executionAttemptID, partitionInfos, timeout);
	}

	@Override
	public void releasePartitions(JobID jobId, Set<ResultPartitionID> partitionIds) {
		taskExecutorGateway.releaseOrPromotePartitions(jobId, partitionIds, Collections.emptySet());
	}

	@Override
	public void notifyCheckpointComplete(ExecutionAttemptID executionAttemptID, JobID jobId, long checkpointId, long timestamp) {
		taskExecutorGateway.confirmCheckpoint(executionAttemptID, checkpointId, timestamp);
	}

	@Override
	public void notifyCheckpointAborted(ExecutionAttemptID executionAttemptID, JobID jobId, long checkpointId, long timestamp) {
		taskExecutorGateway.abortCheckpoint(executionAttemptID, checkpointId, timestamp);
	}

	@Override
	public void triggerCheckpoint(ExecutionAttemptID executionAttemptID, JobID jobId, long checkpointId, long timestamp, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
		taskExecutorGateway.triggerCheckpoint(
			executionAttemptID,
			checkpointId,
			timestamp,
			checkpointOptions,
			advanceToEndOfEventTime);
	}

	@Override
	public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
		return taskExecutorGateway.freeSlot(
			allocationId,
			cause,
			timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> sendOperatorEventToTask(
		ExecutionAttemptID task,
		OperatorID operator,
		SerializedValue<OperatorEvent> evt) {
		return taskExecutorGateway.sendOperatorEventToTask(task, operator, evt);
	}


	/**
	 * todo 赫明萱加
	 * @param attemptId
	 * @param checkpointId
	 * @param rpcTimeout
	 * @return
	 */
	public CompletableFuture<Acknowledge> ignoreCheckpoint(ExecutionAttemptID attemptId, long checkpointId,
														   Time rpcTimeout) {
		return taskExecutorGateway.ignoreCheckpoint(attemptId, checkpointId, rpcTimeout);
	}


	public CompletableFuture<Acknowledge> switchStandbyTaskToRunning(ExecutionAttemptID executionAttemptID, Time timeout) {
		return taskExecutorGateway.switchStandbyTaskToRunning(executionAttemptID, timeout);
	}


	public CompletableFuture<Acknowledge> dispatchStateToStandbyTask(ExecutionAttemptID executionAttemptID, JobManagerTaskRestore taskRestore, Time timeout) {
		return taskExecutorGateway.dispatchStateToStandbyTask(executionAttemptID, taskRestore, timeout);
	}
}
