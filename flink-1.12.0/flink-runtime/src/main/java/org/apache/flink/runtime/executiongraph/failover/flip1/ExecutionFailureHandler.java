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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.failover.RunStandbyTaskStrategy;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.throwable.ThrowableClassifier;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.util.IterableUtils;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This handler deals with task failures to return a {@link FailureHandlingResult} which contains tasks
 * to restart to recover from failures.
 */
public class ExecutionFailureHandler {

	private final SchedulingTopology schedulingTopology;

	/** Strategy to judge which tasks should be restarted. */
	private final FailoverStrategy failoverStrategy;

	/** Strategy to judge whether and when a restarting should be done. */
	private final RestartBackoffTimeStrategy restartBackoffTimeStrategy;

	/** Number of all restarts happened since this job is submitted. */
	private long numberOfRestarts;

	/**
	 * Creates the handler to deal with task failures.
	 *
	 * @param schedulingTopology contains the topology info for failover
	 * @param failoverStrategy helps to decide tasks to restart on task failures
	 * @param restartBackoffTimeStrategy helps to decide whether to restart failed tasks and the restarting delay
	 */
	public ExecutionFailureHandler(
			final SchedulingTopology schedulingTopology,
			final FailoverStrategy failoverStrategy,
			final RestartBackoffTimeStrategy restartBackoffTimeStrategy) {

		this.schedulingTopology = checkNotNull(schedulingTopology);
		this.failoverStrategy = checkNotNull(failoverStrategy);
		this.restartBackoffTimeStrategy = checkNotNull(restartBackoffTimeStrategy);
	}

	/**
	 * Return result of failure handling. Can be a set of task vertices to restart
	 * and a delay of the restarting. Or that the failure is not recoverable and the reason for it.
	 *
	 * @param failedTask is the ID of the failed task vertex
	 * @param cause of the task failure
	 * @return result of the failure handling
	 */
	public FailureHandlingResult getFailureHandlingResult(ExecutionVertexID failedTask, Throwable cause) {
			return handleFailure(cause, failoverStrategy.getTasksNeedingRestart(failedTask, cause), false);

	}


	public FailoverStrategy getFailoverStrategy(){
		return failoverStrategy;
	}

	/**
	 * Return result of failure handling on a global failure. Can be a set of task vertices to restart
	 * and a delay of the restarting. Or that the failure is not recoverable and the reason for it.
	 *
	 * @param cause of the task failure
	 * @return result of the failure handling
	 */
	public FailureHandlingResult getGlobalFailureHandlingResult(final Throwable cause) {
		return handleFailure(
			cause,
			IterableUtils.toStream(schedulingTopology.getVertices())
				.map(SchedulingExecutionVertex::getId)
				.collect(Collectors.toSet()),
			true);
	}

	private FailureHandlingResult handleFailure(
			final Throwable cause,
			final Set<ExecutionVertexID> verticesToRestart,
			final boolean globalFailure) {

		if (isUnrecoverableError(cause)) {
			return FailureHandlingResult.unrecoverable(
				new JobException("The failure is not recoverable", cause), globalFailure);
		}

		restartBackoffTimeStrategy.notifyFailure(cause);
		if (restartBackoffTimeStrategy.canRestart()) {
			numberOfRestarts++;

			return FailureHandlingResult.restartable(
				verticesToRestart,
				restartBackoffTimeStrategy.getBackoffTime(),
				globalFailure);
		} else {
			return FailureHandlingResult.unrecoverable(
				new JobException("Recovery is suppressed by " + restartBackoffTimeStrategy, cause),
				globalFailure);
		}
	}

	@VisibleForTesting
	static boolean isUnrecoverableError(Throwable cause) {
		Optional<Throwable> unrecoverableError = ThrowableClassifier.findThrowableOfThrowableType(
			cause, ThrowableType.NonRecoverableError);
		return unrecoverableError.isPresent();
	}

	public long getNumberOfRestarts() {
		return numberOfRestarts;
	}
}
