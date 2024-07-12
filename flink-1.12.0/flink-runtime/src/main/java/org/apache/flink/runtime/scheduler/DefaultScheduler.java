/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.RunStandbyTaskStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.StandbyFailoverStrategy;
import org.apache.flink.runtime.executiongraph.restart.ThrowingRestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.ThrowingSlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.reConfig.utils.RescaleState;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.reConfig.message.ReConfigSignal.ReConfigSignalType.CLEAN;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The future default scheduler.
 */
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

	private final Logger log;

	private final ClassLoader userCodeLoader;

	private final ExecutionSlotAllocator executionSlotAllocator;

	private final ExecutionFailureHandler executionFailureHandler;

	private final ScheduledExecutor delayExecutor;

	private SchedulingStrategy schedulingStrategy;

	private final ExecutionVertexOperations executionVertexOperations;

	private final Set<ExecutionVertexID> verticesWaitingForRestart;

	private final Consumer<ComponentMainThreadExecutor> startUpAction;

	private final SchedulingStrategyFactory schedulingStrategyFactory;
	private final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory;

	DefaultScheduler(
		final Logger log,
		final JobGraph jobGraph,
		final BackPressureStatsTracker backPressureStatsTracker,
		final Executor ioExecutor,
		final Configuration jobMasterConfiguration,
		final Consumer<ComponentMainThreadExecutor> startUpAction,
		final ScheduledExecutorService futureExecutor,
		final ScheduledExecutor delayExecutor,
		final ClassLoader userCodeLoader,
		final CheckpointRecoveryFactory checkpointRecoveryFactory,
		final Time rpcTimeout,
		final BlobWriter blobWriter,
		final JobManagerJobMetricGroup jobManagerJobMetricGroup,
		final ShuffleMaster<?> shuffleMaster,
		final JobMasterPartitionTracker partitionTracker,
		final SchedulingStrategyFactory schedulingStrategyFactory,
		final FailoverStrategy.Factory failoverStrategyFactory,
		final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
		final ExecutionVertexOperations executionVertexOperations,
		final ExecutionVertexVersioner executionVertexVersioner,
		final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
		final ExecutionDeploymentTracker executionDeploymentTracker,
		long initializationTimestamp,
		SlotProvider slotProvider) throws Exception {


		super(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutor,
			jobMasterConfiguration,
			slotProvider, // this is not used any more in the new scheduler
			futureExecutor,
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			new ThrowingRestartStrategy.ThrowingRestartStrategyFactory(),
			blobWriter,
			jobManagerJobMetricGroup,
			Time.seconds(0), // this is not used any more in the new scheduler
			shuffleMaster,
			partitionTracker,
			executionVertexVersioner,
			executionDeploymentTracker,
			true,
			initializationTimestamp,
			schedulingStrategyFactory);

		this.log = log;

		this.delayExecutor = checkNotNull(delayExecutor);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.executionVertexOperations = checkNotNull(executionVertexOperations);

		final FailoverStrategy failoverStrategy = failoverStrategyFactory.create(
			getSchedulingTopology(),
			getResultPartitionAvailabilityChecker());
		log.info("Using failover strategy {} for {} ({}).", failoverStrategy, jobGraph.getName(), jobGraph.getJobID());

		this.executionFailureHandler = new ExecutionFailureHandler(
			getSchedulingTopology(),
			failoverStrategy,
			restartBackoffTimeStrategy);
		this.schedulingStrategyFactory = schedulingStrategyFactory;
		this.executionSlotAllocatorFactory = executionSlotAllocatorFactory;

		this.schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology());
		this.executionGraph.schedulingStrategy_hmx = schedulingStrategy;

		this.executionSlotAllocator = checkNotNull(executionSlotAllocatorFactory)
			.createInstance(new DefaultExecutionSlotAllocationContext());

		//this.executionGraph.notifyStandby();

		this.verticesWaitingForRestart = new HashSet<>();
		this.startUpAction = startUpAction;
	}

	// ------------------------------------------------------------------------
	// SchedulerNG
	// ------------------------------------------------------------------------

	@Override
	public void setMainThreadExecutor(ComponentMainThreadExecutor mainThreadExecutor) {
		super.setMainThreadExecutor(mainThreadExecutor);
		startUpAction.accept(mainThreadExecutor);
	}

	@Override
	protected long getNumberOfRestarts() {
		return executionFailureHandler.getNumberOfRestarts();
	}

	@Override
	protected void startSchedulingInternal() {
		log.info("Starting scheduling with scheduling strategy [{}]", schedulingStrategy.getClass().getName());
		prepareExecutionGraphForNgScheduling();
		schedulingStrategy.startScheduling();
	}

	@Override
	protected void updateTaskExecutionStateInternal(
			final ExecutionVertexID executionVertexId,
			final TaskExecutionStateTransition taskExecutionState) {

		schedulingStrategy.onExecutionStateChange(executionVertexId, taskExecutionState.getExecutionState());
		maybeHandleTaskFailure(taskExecutionState, executionVertexId);
	}

	private void maybeHandleTaskFailure(
			final TaskExecutionStateTransition taskExecutionState,
			final ExecutionVertexID executionVertexId) {

		if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
			final Throwable error = taskExecutionState.getError(userCodeLoader);
			handleTaskFailure(executionVertexId, error);
		}
	}

	private void handleTaskFailure(final ExecutionVertexID executionVertexId, @Nullable final Throwable error) {
		setGlobalFailureCause(error);
		notifyCoordinatorsAboutTaskFailure(executionVertexId, error);
		final FailureHandlingResult failureHandlingResult = executionFailureHandler.getFailureHandlingResult(executionVertexId, error);

		if(executionFailureHandler.getFailoverStrategy() instanceof StandbyFailoverStrategy){

			log.info("executionFailureHandler.getFailoverStrategy() instanceof StandbyFailoverStrategy");
			RunStandbyTaskStrategy runStandbyTaskStrategy= ((StandbyFailoverStrategy)executionFailureHandler.getFailoverStrategy()).getRunStandbyTaskStrategy();
			runStandbyTaskStrategy.onTaskFailure(executionVertexId,error);
		}
		else{
			maybeRestartTasks(failureHandlingResult);
		}

	}

	private void notifyCoordinatorsAboutTaskFailure(final ExecutionVertexID executionVertexId, @Nullable final Throwable error) {
		final ExecutionJobVertex jobVertex = getExecutionJobVertex(executionVertexId.getJobVertexId());
		final int subtaskIndex = executionVertexId.getSubtaskIndex();

		jobVertex.getOperatorCoordinators().forEach(c -> c.subtaskFailed(subtaskIndex, error));
	}

	@Override
	public void handleGlobalFailure(final Throwable error) {
		setGlobalFailureCause(error);

		log.info("Trying to recover from a global failure.", error);
		final FailureHandlingResult failureHandlingResult = executionFailureHandler.getGlobalFailureHandlingResult(error);
		maybeRestartTasks(failureHandlingResult);
	}

	private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
		if (failureHandlingResult.canRestart()) {
			log.info("failureHandlingResult.canRestart()");
			restartTasksWithDelay(failureHandlingResult);
		} else {
			failJob(failureHandlingResult.getError());
		}
	}

	private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
		final Set<ExecutionVertexID> verticesToRestart = failureHandlingResult.getVerticesToRestart();

		final Set<ExecutionVertexVersion> executionVertexVersions =
			new HashSet<>(executionVertexVersioner.recordVertexModifications(verticesToRestart).values());
		final boolean globalRecovery = failureHandlingResult.isGlobalFailure();

		addVerticesToRestartPending(verticesToRestart);

		final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

		delayExecutor.schedule(
			() -> FutureUtils.assertNoException(
				cancelFuture.thenRunAsync(restartTasks(executionVertexVersions, globalRecovery), getMainThreadExecutor())),
			failureHandlingResult.getRestartDelayMS(),
			TimeUnit.MILLISECONDS);
	}

	private void addVerticesToRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
		verticesWaitingForRestart.addAll(verticesToRestart);
		transitionExecutionGraphState(JobStatus.RUNNING, JobStatus.RESTARTING);
	}

	private void removeVerticesFromRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
		verticesWaitingForRestart.removeAll(verticesToRestart);
		if (verticesWaitingForRestart.isEmpty()) {
			transitionExecutionGraphState(JobStatus.RESTARTING, JobStatus.RUNNING);
		}
	}

	private Runnable restartTasks(final Set<ExecutionVertexVersion> executionVertexVersions, final boolean isGlobalRecovery) {
		return () -> {
			final Set<ExecutionVertexID> verticesToRestart = executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

			removeVerticesFromRestartPending(verticesToRestart);

			resetForNewExecutions(verticesToRestart);

			try {
				restoreState(verticesToRestart, isGlobalRecovery);
			} catch (Throwable t) {
				handleGlobalFailure(t);
				return;
			}

			schedulingStrategy.restartTasks(verticesToRestart);
		};
	}

	private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
		final List<CompletableFuture<?>> cancelFutures = verticesToRestart.stream()
			.map(this::cancelExecutionVertex)
			.collect(Collectors.toList());

		return FutureUtils.combineAll(cancelFutures);
	}

	private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
		final ExecutionVertex vertex = getExecutionVertex(executionVertexId);

		notifyCoordinatorOfCancellation(vertex);

		executionSlotAllocator.cancel(executionVertexId);
		return executionVertexOperations.cancel(vertex);
	}

	@Override
	protected void scheduleOrUpdateConsumersInternal(final IntermediateResultPartitionID partitionId) {
		schedulingStrategy.onPartitionConsumable(partitionId);
	}

	// ------------------------------------------------------------------------
	// SchedulerOperations
	// ------------------------------------------------------------------------

	@Override
	public void allocateSlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		validateDeploymentOptions(executionVertexDeploymentOptions);

		log.info(" allocateSlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions)");
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex =
			groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);

		final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.collect(Collectors.toList());

		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
			executionVertexVersioner.recordVertexModifications(verticesToDeploy);


		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			allocateSlots(executionVertexDeploymentOptions);

//		for(SlotExecutionVertexAssignment deploymentHandle:slotExecutionVertexAssignments){
//			try {
//				System.out.println("deploymentHandle slot"+deploymentHandle.getLogicalSlotFuture().get().getAllocationId());
//			} catch (InterruptedException e) {
//				throw new RuntimeException(e);
//			} catch (ExecutionException e) {
//				throw new RuntimeException(e);
//			}
//		}

		final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(
			requiredVersionByVertex,
			deploymentOptionsByVertex,
			slotExecutionVertexAssignments);


		log.info(" List<DeploymentHandle> deploymentHandles.length {}",deploymentHandles.size());
		waitForAllSlotsAndDeploy(deploymentHandles);
	}


	public void allocateStandbySlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {

		log.info(" allocateStandbySlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions)");
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex =
			groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);
		log.info("deploymentOptionsByVertex.length {}",deploymentOptionsByVertex.size());

		final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.collect(Collectors.toList());

		log.info("verticesToDeploy.length {}",verticesToDeploy.size());

		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex = new HashMap<>();
		requiredVersionByVertex.put(verticesToDeploy.get(0),new ExecutionVertexVersion(verticesToDeploy.get(0),1l));


		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			allocateSlots(executionVertexDeploymentOptions);

		final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(
			requiredVersionByVertex,
			deploymentOptionsByVertex,
			slotExecutionVertexAssignments);


		log.info(" List<DeploymentHandle> deploymentHandles.length {}",deploymentHandles.size());
		waitForAllSlotsAndDeploy(deploymentHandles);
	}
	private void validateDeploymentOptions(final Collection<ExecutionVertexDeploymentOption> deploymentOptions) {
		log.info("validateDeploymentOptions");
		deploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(this::getExecutionVertex)
			.forEach(v -> checkState(
				v.getExecutionState() == ExecutionState.CREATED||v.getExecutionState() == ExecutionState.SCHEDULED,
				"expected vertex %s to be in CREATED state, was: %s", v.getID(), v.getExecutionState()));
	}

	private static Map<ExecutionVertexID, ExecutionVertexDeploymentOption> groupDeploymentOptionsByVertexId(
			final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionVertexDeploymentOptions.stream().collect(Collectors.toMap(
				ExecutionVertexDeploymentOption::getExecutionVertexId,
				Function.identity()));
	}

	private List<SlotExecutionVertexAssignment> allocateSlots(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {

		List<SlotExecutionVertexAssignment> list= executionSlotAllocator.allocateSlotsFor(executionVertexDeploymentOptions
			.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(this::getExecutionVertex)
			.map(ExecutionVertexSchedulingRequirementsMapper::from)
			.collect(Collectors.toList()));

		log.info("private List<SlotExecutionVertexAssignment> allocateSlots.length {}",list.size());
		return list;
	}

	private static List<DeploymentHandle> createDeploymentHandles(
		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex,
		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

		return slotExecutionVertexAssignments
			.stream()
			.map(slotExecutionVertexAssignment -> {
				final ExecutionVertexID executionVertexId = slotExecutionVertexAssignment.getExecutionVertexId();
				return new DeploymentHandle(
					requiredVersionByVertex.get(executionVertexId),
					deploymentOptionsByVertex.get(executionVertexId),
					slotExecutionVertexAssignment);
			})
			.collect(Collectors.toList());
	}

	private void waitForAllSlotsAndDeploy(final List<DeploymentHandle> deploymentHandles) {
		FutureUtils.assertNoException(
			assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles)));
	}

	private CompletableFuture<Void> assignAllResources(final List<DeploymentHandle> deploymentHandles) {
		log.info("assignAllResources(final List<DeploymentHandle> deploymentHandles)");
		final List<CompletableFuture<Void>> slotAssignedFutures = new ArrayList<>();
		for (DeploymentHandle deploymentHandle : deploymentHandles) {
			log.info("deploymentHandle hash {}",deploymentHandle.toString());
			final CompletableFuture<Void> slotAssigned = deploymentHandle
				.getSlotExecutionVertexAssignment()
				.getLogicalSlotFuture()
				.handle(assignResourceOrHandleError(deploymentHandle));
			slotAssignedFutures.add(slotAssigned);
		}

		log.info("assignAllResources(final List<DeploymentHandle> deploymentHandles)hou");
		return FutureUtils.waitForAll(slotAssignedFutures);
	}

	private BiFunction<Void, Throwable, Void> deployAll(final List<DeploymentHandle> deploymentHandles) {
		log.info("1111deploymentHandles.length {}",deploymentHandles.size());
		return (ignored, throwable) -> {
			propagateIfNonNull(throwable);
			for (final DeploymentHandle deploymentHandle : deploymentHandles) {
				final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();
				final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
				checkState(slotAssigned.isDone());

				log.info("for (final DeploymentHandle deploymentHandle : deploymentHandles) {}",deploymentHandle);
				FutureUtils.assertNoException(
					slotAssigned.handle(deployOrHandleError(deploymentHandle)));
			}
			return null;
		};
	}

	private static void propagateIfNonNull(final Throwable throwable) {
		if (throwable != null) {
			throw new CompletionException(throwable);
		}
	}

	private BiFunction<LogicalSlot, Throwable, Void> assignResourceOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		final ExecutionVertexID executionVertexId = deploymentHandle.getExecutionVertexId();

		return (logicalSlot, throwable) -> {
			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				log.debug("Refusing to assign slot to execution vertex {} because this deployment was " +
					"superseded by another deployment", executionVertexId);
				releaseSlotIfPresent(logicalSlot);
				return null;
			}

			if (throwable == null) {
				final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
				/*if(executionVertex!=null){
					log.info("executionVertex : {}",executionVertex.getID());
				}*/
				final boolean sendScheduleOrUpdateConsumerMessage = deploymentHandle.getDeploymentOption().sendScheduleOrUpdateConsumerMessage();

				//TODO: this may not right.check it later
				for(Execution standbyExecution : executionVertex.getStandbyExecutions()) {
					standbyExecution.registerProducedPartitions(logicalSlot.getTaskManagerLocation(), sendScheduleOrUpdateConsumerMessage);
					//log.warn("\n\n" + standbyExecution.getAttemptId());
				}
				//log.warn("\n" + executionVertex.getCurrentExecutionAttempt().getAttemptId());
				executionVertex
					.getCurrentExecutionAttempt()
					.registerProducedPartitions(logicalSlot.getTaskManagerLocation(), sendScheduleOrUpdateConsumerMessage);
				//log.warn("\n" + executionVertex.getCurrentExecutionAttempt().producedPartitions.size() + " " + executionVertex.getCurrentExecutionAttempt().isStandby);
				log.info("logicalSlot : {}",logicalSlot.getTaskManagerGateway().getAddress());
				executionVertex.tryAssignResource(logicalSlot);
			} else {
				handleTaskDeploymentFailure(executionVertexId, maybeWrapWithNoResourceAvailableException(throwable));
			}
			return null;
		};
	}

	private void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
		if (logicalSlot != null) {
			logicalSlot.releaseSlot(null);
		}
	}

	private void handleTaskDeploymentFailure(final ExecutionVertexID executionVertexId, final Throwable error) {
		executionVertexOperations.markFailed(getExecutionVertex(executionVertexId), error);
	}

	private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
		final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
		if (strippedThrowable instanceof TimeoutException) {
			return new NoResourceAvailableException("Could not allocate the required slot within slot request timeout. " +
				"Please make sure that the cluster has enough resources.", failure);
		} else {
			return failure;
		}
	}

	private BiFunction<Object, Throwable, Void> deployOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		log.info("requiredVertexVersion.getVersion() {}",requiredVertexVersion.getVersion());

//		if(requiredVertexVersion.getVersion()==1l){
//			requiredVertexVersion.add();
//		}
		final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();
		log.info("requiredVertexVersion.getExecutionVertexId() {}",requiredVertexVersion.getExecutionVertexId());

		return (ignored, throwable) -> {
			log.info("executionVertexVersioner {}",executionVertexVersioner.getExecutionVertexVersion(executionVertexId).getVersion());
			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				log.info("Refusing to deploy execution vertex {} because this deployment was " +
					"superseded by another deployment", executionVertexId);
				return null;
			}

			if (throwable == null) {
				log.info("deployTaskSafe(executionVertexId); {}",executionVertexId);
				deployTaskSafe(executionVertexId);
			} else {
				log.info("deployTaskSafe(executionVertexId);youbug");
				handleTaskDeploymentFailure(executionVertexId, throwable);
			}
			return null;
		};
	}

	private void deployTaskSafe(final ExecutionVertexID executionVertexId) {
		try {
			log.info("deployTaskSafe(final ExecutionVertexID executionVertexId) {}",executionVertexId);
			final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
			executionVertexOperations.deploy(executionVertex);
		} catch (Throwable e) {
			handleTaskDeploymentFailure(executionVertexId, e);
		}
	}

	private void notifyCoordinatorOfCancellation(ExecutionVertex vertex) {
		// this method makes a best effort to filter out duplicate notifications, meaning cases where
		// the coordinator was already notified for that specific task
		// we don't notify if the task is already FAILED, CANCELLING, or CANCELED

		final ExecutionState currentState = vertex.getExecutionState();
		if (currentState == ExecutionState.FAILED ||
				currentState == ExecutionState.CANCELING ||
				currentState == ExecutionState.CANCELED) {
			return;
		}

		for (OperatorCoordinator coordinator : vertex.getJobVertex().getOperatorCoordinators()) {
			coordinator.subtaskFailed(vertex.getParallelSubtaskIndex(), null);
		}
	}

	private class DefaultExecutionSlotAllocationContext implements ExecutionSlotAllocationContext {

		@Override
		public ResourceProfile getResourceProfile(final ExecutionVertexID executionVertexId) {
			return getExecutionVertex(executionVertexId).getResourceProfile();
		}

		@Override
		public AllocationID getPriorAllocationId(final ExecutionVertexID executionVertexId) {
			return getExecutionVertex(executionVertexId).getLatestPriorAllocation();
		}

		@Override
		public SchedulingTopology getSchedulingTopology() {
			return DefaultScheduler.this.getSchedulingTopology();
		}

		@Override
		public Set<SlotSharingGroup> getLogicalSlotSharingGroups() {
			return getJobGraph().getSlotSharingGroups();
		}

		@Override
		public Set<CoLocationGroupDesc> getCoLocationGroups() {
			return getJobGraph().getCoLocationGroupDescriptors();
		}

		@Override
		public Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(
				ExecutionVertexID executionVertexId) {
			return inputsLocationsRetriever.getConsumedResultPartitionsProducers(executionVertexId);
		}

		@Override
		public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
				ExecutionVertexID executionVertexId) {
			return inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);
		}

		@Override
		public Optional<TaskManagerLocation> getStateLocation(ExecutionVertexID executionVertexId) {
			return stateLocationRetriever.getStateLocation(executionVertexId);
		}
	}

	@Override
	public void changeTopo(String taskName, int newParallelism) {
		JobVertexID jobVertexID = this.executionGraph.changeTopo(taskName, newParallelism);
		super.schedulingTopology = executionGraph.getSchedulingTopology();
		this.schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology());
		ExecutionSlotAllocator newExecutionSlotAllocator = checkNotNull(executionSlotAllocatorFactory)
			.createInstance(new DefaultExecutionSlotAllocationContext());
		// TODO 修改assignedJobVerticesForGroups，executionSlotSharingGroupMap
		this.executionSlotAllocator.updateForRescale(newExecutionSlotAllocator);
		this.schedulingStrategy.startSchedulingForRescale();


		ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

		List<ExecutionVertex> tmp1 = new ArrayList<>();
		for(ExecutionVertex ev:jobVertex.getTaskVertices()){
//			if(ev.getTaskName().contains("upStream")){
//				tmp1.add(ev);
//			}
//			if(ev.getTaskName().contains("ReConfig")){
				tmp1.add(ev);
//			}
		}
		this.executionGraph.getReConfigController().updateReConfigVertex(tmp1.toArray(new ExecutionVertex[0]));
	}

	@Override
	public void acknowledgeDeploymentForRescaling(ExecutionAttemptID executionAttemptID) {
		assert this.executionGraph.getReConfigController().reConfigResultCollector != null;
		this.executionGraph.getReConfigController().reConfigResultCollector.acknowledgeDeploymentForRescaling(executionAttemptID);
	}

	@Override
	public void rescaleClean() {
		this.schedulingStrategy.releaseResourcesForRescale();
		this.executionSlotAllocator.clean();
		this.executionGraph.cleanRescaleStates();
	}

	@Override
	public void allocateSlotsAndDeployForRescale(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		//每个pipelineRegion内部的Vertex逐个调度
		for(ExecutionVertexDeploymentOption evdo: executionVertexDeploymentOptions){
			List<ExecutionVertexDeploymentOption> singleList = Collections.singletonList(evdo);
			if(getExecutionVertex(evdo.getExecutionVertexId()).rescaleState == RescaleState.NEW){
				System.out.println("now start new vertex deploy:"+getExecutionGraph().getJobVertex(evdo.getExecutionVertexId()
					.getJobVertexId()).getName() + "_"+evdo.getExecutionVertexId().getSubtaskIndex());
				deployNewDeploymentsAndWaitUntilDone(singleList);//部署到slot中，调用Flink原有代码
			}
			else{
				System.out.println("now start modify vertex deploy:"+getExecutionGraph().getJobVertex(evdo.getExecutionVertexId()
					.getJobVertexId()).getName() + "_"+evdo.getExecutionVertexId().getSubtaskIndex());
				updateModifiedVertices(singleList);
			}
		}
	}

	public void deployNewDeploymentsAndWaitUntilDone(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions){
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex =
			groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);

		final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.collect(Collectors.toList());

		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
			executionVertexVersioner.recordVertexModifications(verticesToDeploy);

		transitionToScheduled(verticesToDeploy);//Execution生命周期至SCHEDULED（CREATED->SCHEDULED）

		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			allocateSlots(executionVertexDeploymentOptions);

		final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(
			requiredVersionByVertex,
			deploymentOptionsByVertex,
			slotExecutionVertexAssignments);

		waitForAllSlotsAndDeployToBeDone(deploymentHandles);
	}

//	private void updateModifiedVertices(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions){
//		List<ExecutionVertex> deployingExecutionVertices = new ArrayList<>();
//
//		executionVertexDeploymentOptions.forEach(executionVertexDeploymentOption -> {
//			ExecutionVertex ev = getExecutionVertex(executionVertexDeploymentOption.getExecutionVertexId());
//			if(ev.rescaleState == RescaleState.MODIFIED){
//				getExecutionVertex(executionVertexDeploymentOption.getExecutionVertexId())
//					.getCurrentExecutionAttempt()
//					.deployForRescale();
//				deployingExecutionVertices.add(ev);
//			}
//		});
//
////		if(!deployingExecutionVertices.isEmpty()){
////			try {
////				CompletableFuture<Acknowledge> future = getExecutionGraph().getReConfigController().notifyStartAsyncDeploymentForRescale(deployingExecutionVertices);
////				future.get();
////			}catch (InterruptedException | ExecutionException e){
////				e.printStackTrace();
////			}
////		}
//	}

	private void updateModifiedVertices(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions){
		updateModifiedVertices(executionVertexDeploymentOptions, RescaleState.NONE);
	}

	private void updateModifiedVertices(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions, RescaleState rescaleState) {
		List<ExecutionVertex> deployingExecutionVertices = new ArrayList<>();

		System.out.println("call updateModifiedVertices "+rescaleState);
		executionVertexDeploymentOptions.forEach(executionVertexDeploymentOption -> {
			ExecutionVertex ev = getExecutionVertex(executionVertexDeploymentOption.getExecutionVertexId());
			System.out.println(ev+" rescaleState: "+ev.rescaleState);
			if (ev.rescaleState == RescaleState.MODIFIED) {
				getExecutionVertex(executionVertexDeploymentOption.getExecutionVertexId())
					.getCurrentExecutionAttempt()
					.deployForRescale(rescaleState);
				deployingExecutionVertices.add(ev);
			}
		});
		if (!deployingExecutionVertices.isEmpty()) {
			checkNotNull(executionGraph.getReConfigController());
			try {
				CompletableFuture<Acknowledge> future = executionGraph.getReConfigController().reConfigResultCollector.notifyStartAsyncDeploymentForRescale(deployingExecutionVertices);
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		System.out.println("update modified vertices");
	}

	private void waitForAllSlotsAndDeployToBeDone(final List<DeploymentHandle> deploymentHandles){
		CompletableFuture<Void> future = assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles));
		try {
			future.get();
		}catch (InterruptedException | ExecutionException e){
			e.printStackTrace();
		}
	}

	@Override
	public void cancelTasksAndChannelsForRescale(List<ExecutionVertexDeploymentOption> savedExecutionVertexDeploymentOptionsForRescale) {
		// cancel the removed vertices
		List<ExecutionVertex> removedExecutionVertices = this.executionGraph.removeExecutionVertices;
		int numRemovedExecutionVertices = removedExecutionVertices.size();
		CompletableFuture<?>[] cancelResultFutures = new CompletableFuture<?>[numRemovedExecutionVertices];
		for (int i = 0; i < numRemovedExecutionVertices; i++) {
			ExecutionVertex ev = removedExecutionVertices.get(i);
			CompletableFuture<Acknowledge> cancelResultFuture = new CompletableFuture<>();
			boolean sendRpcCallSuccess = ev.getCurrentExecutionAttempt().cancelAsync(cancelResultFuture);
			if (sendRpcCallSuccess) {
				System.out.println("remove vertex: " + ev + " state:"+ev.rescaleState);
				cancelResultFutures[i] = cancelResultFuture;
			} else {
				log.error("Cannot send cancel call of {}.", ev);
				log.info("Cannot send cancel call of {}.", ev);
			}
		}

		// wait for the cancels to be completed
		CompletableFuture<Void> all = cancelResultFutures.length == 0 ? CompletableFuture.completedFuture(null) : CompletableFuture.allOf(cancelResultFutures);
		all.join();
//		all.whenComplete((v, throwable) -> {
			// cancel channels
			updateModifiedVertices(savedExecutionVertexDeploymentOptionsForRescale, RescaleState.CLEAN);
//		});

	}
}
