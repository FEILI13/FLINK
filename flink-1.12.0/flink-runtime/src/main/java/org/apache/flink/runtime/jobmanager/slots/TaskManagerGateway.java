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

package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.util.SerializedValue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Task manager gateway interface to communicate with the task manager.
 */
public interface TaskManagerGateway extends TaskExecutorOperatorEventGateway {

	/**
	 * Return the address of the task manager with which the gateway is associated.
	 *
	 * @return Address of the task manager with which this gateway is associated.
	 */
	String getAddress();

	/**
	 * Request the back pressure ratio for the given task.
	 *
	 * @param executionAttemptID identifying the task to request.
	 * @param requestId id of the request.
	 * @param timeout rpc request timeout.
	 * @return A future of the task back pressure result.
	 */
	CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(
		ExecutionAttemptID executionAttemptID,
		int requestId,
		Time timeout);

	/**
	 * Submit a task to the task manager.
	 *
	 * @param tdd describing the task to submit
	 * @param timeout of the submit operation
	 * @return Future acknowledge of the successful operation
	 */
	CompletableFuture<Acknowledge> submitTask(
		TaskDeploymentDescriptor tdd,
		Time timeout);

	/**
	 * Cancel the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the task is successfully canceled
	 */
	CompletableFuture<Acknowledge> cancelTask(
		ExecutionAttemptID executionAttemptID,
		Time timeout);

	/**
	 * Update the task where the given partitions can be found.
	 *
	 * @param executionAttemptID identifying the task
	 * @param partitionInfos telling where the partition can be retrieved from
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the partitions have been successfully updated
	 */
	CompletableFuture<Acknowledge> updatePartitions(
		ExecutionAttemptID executionAttemptID,
		Iterable<PartitionInfo> partitionInfos,
		Time timeout);

	/**
	 * Batch release intermediate result partitions.
	 *
	 * @param jobId id of the job that the partitions belong to
	 * @param partitionIds partition ids to release
	 */
	void releasePartitions(JobID jobId, Set<ResultPartitionID> partitionIds);

	/**
	 * Notify the given task about a completed checkpoint.
	 *
	 * @param executionAttemptID identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param checkpointId of the completed checkpoint
	 * @param timestamp of the completed checkpoint
	 */
	void notifyCheckpointComplete(
		ExecutionAttemptID executionAttemptID,
		JobID jobId,
		long checkpointId,
		long timestamp);

	/**
	 * Notify the given task about a aborted checkpoint.
	 *
	 * @param executionAttemptID identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param checkpointId of the subsumed checkpoint
	 * @param timestamp of the subsumed checkpoint
	 */
	void notifyCheckpointAborted(
		ExecutionAttemptID executionAttemptID,
		JobID jobId,
		long checkpointId,
		long timestamp);

	/**
	 * Trigger for the given task a checkpoint.
	 *
	 * @param executionAttemptID identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param checkpointId of the checkpoint to trigger
	 * @param timestamp of the checkpoint to trigger
	 * @param checkpointOptions of the checkpoint to trigger
	 * @param advanceToEndOfEventTime Flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 *                              to fire any registered event-time timers
	 */
	void triggerCheckpoint(
		ExecutionAttemptID executionAttemptID,
		JobID jobId,
		long checkpointId,
		long timestamp,
		CheckpointOptions checkpointOptions,
		boolean advanceToEndOfEventTime);

	/**
	 * Frees the slot with the given allocation ID.
	 *
	 * @param allocationId identifying the slot to free
	 * @param cause of the freeing operation
	 * @param timeout for the operation
	 * @return Future acknowledge which is returned once the slot has been freed
	 */
	CompletableFuture<Acknowledge> freeSlot(
		final AllocationID allocationId,
		final Throwable cause,
		@RpcTimeout final Time timeout);

	@Override
	CompletableFuture<Acknowledge> sendOperatorEventToTask(
		ExecutionAttemptID task,
		OperatorID operator,
		SerializedValue<OperatorEvent> evt);

	CompletableFuture<Acknowledge> ignoreCheckpoint(ExecutionAttemptID attemptId, long checkpointId, Time rpcTimeout);

	CompletableFuture<Acknowledge> switchStandbyTaskToRunning(
		ExecutionAttemptID executionAttemptID,
		Time timeout);

	/**
	 * Dispatch the latest checkpointed state of running task to its standby.
	 *
	 * @param executionAttemptID identifying the standby task
	 * @param taskRestore identifying the task state snapshot
	 * @param timeout for the cancel operation
	 * @return Future acknowledge if the task is successfully canceled
	 */
	CompletableFuture<Acknowledge> dispatchStateToStandbyTask(
		ExecutionAttemptID executionAttemptID,
		JobManagerTaskRestore taskRestore,
		Time timeout);
}
