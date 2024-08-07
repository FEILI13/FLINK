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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Utility for creating {@link CheckpointedInputGate} based on checkpoint mode
 * for {@link StreamOneInputProcessor} and {@link StreamTwoInputProcessor}.
 */
@Internal
public class InputProcessorUtil {
	@SuppressWarnings("unchecked")
	public static CheckpointedInputGate createCheckpointedInputGate(
			AbstractInvokable toNotifyOnCheckpoint,
			StreamConfig config,
			SubtaskCheckpointCoordinator checkpointCoordinator,
			IndexedInputGate[] inputGates,
			TaskIOMetricGroup taskIOMetricGroup,
			String taskName,
			MailboxExecutor mailboxExecutor) {
		CheckpointedInputGate[] checkpointedInputGates = createCheckpointedMultipleInputGate(
			toNotifyOnCheckpoint,
			config,
			checkpointCoordinator,
			taskIOMetricGroup,
			taskName,
			mailboxExecutor,
			new List[]{ Arrays.asList(inputGates) },
			Collections.emptyList());
		return Iterables.getOnlyElement(Arrays.asList(checkpointedInputGates));
	}

	/**
	 * @return an array of {@link CheckpointedInputGate} created for corresponding
	 * {@link InputGate}s supplied as parameters.
	 */
	public static CheckpointedInputGate[] createCheckpointedMultipleInputGate(
			AbstractInvokable toNotifyOnCheckpoint,
			StreamConfig config,
			SubtaskCheckpointCoordinator checkpointCoordinator,
			TaskIOMetricGroup taskIOMetricGroup,
			String taskName,
			MailboxExecutor mailboxExecutor,
			List<IndexedInputGate>[] inputGates,
			List<StreamTaskSourceInput<?>> sourceInputs) {
		CheckpointBarrierHandler barrierHandler = createCheckpointBarrierHandler(
			toNotifyOnCheckpoint,
			config,
			checkpointCoordinator,
			taskName,
			inputGates,
			sourceInputs);
		return createCheckpointedMultipleInputGate(
			mailboxExecutor,
			inputGates,
			taskIOMetricGroup,
			barrierHandler,
			config);
	}

	public static CheckpointedInputGate[] createCheckpointedMultipleInputGate(
			MailboxExecutor mailboxExecutor,
			List<IndexedInputGate>[] inputGates,
			TaskIOMetricGroup taskIOMetricGroup,
			CheckpointBarrierHandler barrierHandler,
			StreamConfig config) {

		registerCheckpointMetrics(taskIOMetricGroup, barrierHandler);

		InputGate[] unionedInputGates = Arrays.stream(inputGates)
			.map(InputGateUtil::createInputGate)
			.toArray(InputGate[]::new);
		System.out.println("unionedInputGates.length:"+unionedInputGates.length);
		if(unionedInputGates[0]==null){
			System.out.println("unionedInputGates[0]==null");
		}
		return Arrays.stream(unionedInputGates)
			.map(unionedInputGate -> new CheckpointedInputGate(
				unionedInputGate,
				barrierHandler,
				mailboxExecutor,
				config.isGraphContainingLoops() ? UpstreamRecoveryTracker.NO_OP : UpstreamRecoveryTracker.forInputGate(unionedInputGate)))
			.toArray(CheckpointedInputGate[]::new);
	}

	public static CheckpointBarrierHandler createCheckpointBarrierHandler(
			AbstractInvokable toNotifyOnCheckpoint,
			StreamConfig config,
			SubtaskCheckpointCoordinator checkpointCoordinator,
			String taskName,
			List<IndexedInputGate>[] inputGates,
			List<StreamTaskSourceInput<?>> sourceInputs) {

		CheckpointableInput[] inputs =
			Stream.<CheckpointableInput>concat(
					Arrays.stream(inputGates).flatMap(Collection::stream),
					sourceInputs.stream())
				.sorted(Comparator.comparing(CheckpointableInput::getInputGateIndex))
				.toArray(CheckpointableInput[]::new);

		CheckpointBarrierHandler barrierHandler;

		switch (config.getCheckpointMode()) {
			case EXACTLY_ONCE:
				int numberOfChannels = (int) Arrays
						.stream(inputs)
						.flatMap(gate -> gate.getChannelInfos().stream())
						.count();
				CheckpointBarrierBehaviourController controller =
					config.isUnalignedCheckpointsEnabled() ?
						new AlternatingController(
							new AlignedController(inputs),
							new UnalignedController(checkpointCoordinator, inputs)) :
						new AlignedController(inputs);
				barrierHandler= new SingleCheckpointBarrierHandler(
						taskName,
						toNotifyOnCheckpoint,
						numberOfChannels,
						controller);
				break;
			case AT_LEAST_ONCE:
				if (config.isUnalignedCheckpointsEnabled()) {
					throw new IllegalStateException("Cannot use unaligned checkpoints with AT_LEAST_ONCE " +
						"checkpointing mode");
				}

				InputGate[] unionedInputGates = Arrays.stream(inputGates)
					.map(InputGateUtil::createInputGate)
					.toArray(InputGate[]::new);
				int numInputChannels = Arrays.stream(inputs).mapToInt(CheckpointableInput::getNumberOfInputChannels).sum();
				barrierHandler= new CheckpointBarrierTracker(numInputChannels, toNotifyOnCheckpoint);
				break;
			default:
				throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + config.getCheckpointMode());
		}



		return new CausalBufferHandler(
			toNotifyOnCheckpoint.getJobCausalLog(),
			toNotifyOnCheckpoint.getRecoveryManager(),
			barrierHandler,
			barrierHandler.getTotalNumberOfInputChannels(),
			toNotifyOnCheckpoint.getCheckpointLock(),
			toNotifyOnCheckpoint
		);
	}

	private static void registerCheckpointMetrics(TaskIOMetricGroup taskIOMetricGroup, CheckpointBarrierHandler barrierHandler) {
		taskIOMetricGroup.gauge(MetricNames.CHECKPOINT_ALIGNMENT_TIME, barrierHandler::getAlignmentDurationNanos);
		taskIOMetricGroup.gauge(MetricNames.CHECKPOINT_START_DELAY_TIME, barrierHandler::getCheckpointStartDelayNanos);
	}
}
