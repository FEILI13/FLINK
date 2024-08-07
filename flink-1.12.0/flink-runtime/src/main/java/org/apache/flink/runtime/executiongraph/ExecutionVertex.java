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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.topology.VertexID;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.util.Preconditions;

import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;

/**
 * The ExecutionVertex is a parallel subtask of the execution. It may be executed once, or several times, each of
 * which time it spawns an {@link Execution}.
 */
public class ExecutionVertex implements AccessExecutionVertex, Archiveable<ArchivedExecutionVertex> {

	private static final Logger LOG = ExecutionGraph.LOG;

	public static final int MAX_DISTINCT_LOCATIONS_TO_CONSIDER = 8;

	// --------------------------------------------------------------------------------------------

	private final ExecutionJobVertex jobVertex;

	private final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitions;

	private final ExecutionEdge[][] inputEdges;

	private final int subTaskIndex;

	private final ExecutionVertexID executionVertexId;

	private final EvictingBoundedList<ArchivedExecution> priorExecutions;

	private final Time timeout;

	/** The name in the format "myTask (2/7)", cached to avoid frequent string concatenations. */
	private final String taskNameWithSubtask;

	private CoLocationConstraint locationConstraint;

	/** The current or latest execution attempt of this vertex's task. */
	private Execution currentExecution;	// this field must never be null

	private final ArrayList<InputSplit> inputSplits;

	// --------------------------------------------------------------------------------------------

	private final ArrayList<Execution> standbyExecutions;

	private final Time failSignalTimeSpan= Time.milliseconds(2000L);
	private int lastKillAttemptNumber = -1;
	int count=0;

	int standCount=0;

	int flag=0;

	/**
	 * Creates an ExecutionVertex.
	 *
	 * @param timeout
	 *            The RPC timeout to use for deploy / cancel calls
	 * @param initialGlobalModVersion
	 *            The global modification version to initialize the first Execution with.
	 * @param createTimestamp
	 *            The timestamp for the vertex creation, used to initialize the first Execution with.
	 * @param maxPriorExecutionHistoryLength
	 *            The number of prior Executions (= execution attempts) to keep.
	 */
	ExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex,
			IntermediateResult[] producedDataSets,
			Time timeout,
			long initialGlobalModVersion,
			long createTimestamp,
			int maxPriorExecutionHistoryLength) {

		this.jobVertex = jobVertex;
		this.subTaskIndex = subTaskIndex;
		this.executionVertexId = new ExecutionVertexID(jobVertex.getJobVertexId(), subTaskIndex);
		this.taskNameWithSubtask = String.format("%s (%d/%d)",
				jobVertex.getJobVertex().getName(), subTaskIndex + 1, jobVertex.getParallelism());

		this.resultPartitions = new LinkedHashMap<>(producedDataSets.length, 1);

		for (IntermediateResult result : producedDataSets) {
			IntermediateResultPartition irp = new IntermediateResultPartition(result, this, subTaskIndex);
			result.setPartition(subTaskIndex, irp);

			resultPartitions.put(irp.getPartitionId(), irp);
		}

		this.inputEdges = new ExecutionEdge[jobVertex.getJobVertex().getInputs().size()][];

		this.priorExecutions = new EvictingBoundedList<>(maxPriorExecutionHistoryLength);

		this.currentExecution = new Execution(
			getExecutionGraph().getFutureExecutor(),
			this,
			0,
			initialGlobalModVersion,
			createTimestamp,
			timeout);

		// create a co-location scheduling hint, if necessary
		CoLocationGroup clg = jobVertex.getCoLocationGroup();
		if (clg != null) {
			this.locationConstraint = clg.getLocationConstraint(subTaskIndex);
		}
		else {
			this.locationConstraint = null;
		}

		getExecutionGraph().registerExecution(currentExecution);

		this.timeout = timeout;
		this.inputSplits = new ArrayList<>();

		/************************************************************************************************/
		// We can make the size match a replication factor
		this.standbyExecutions = new ArrayList<Execution>();


		final long globalModVersion = currentExecution.getGlobalModVersion();
		final long createTimestampStandby = System.currentTimeMillis();
		final boolean isStandby = true;

//		Execution newStandbyExecution = new Execution(
//			getExecutionGraph().getFutureExecutor(),
//			this,
//			currentExecution.getAttemptNumber() + 1,
//			globalModVersion,
//			createTimestampStandby,
//			timeout,
//			isStandby);
//		standbyExecutions.add(newStandbyExecution);
//		LOG.info("Added standby execution for task");
//		// register this execution at the execution graph, to receive call backs such as task state updates.
//		getExecutionGraph().registerExecution(newStandbyExecution);
//		LOG.info("Added standby execution for task " + taskNameWithSubtask + "and registered it to running executions. Now scheduling it.");
	}


	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------

	public JobID getJobId() {
		return this.jobVertex.getJobId();
	}

	public ExecutionJobVertex getJobVertex() {
		return jobVertex;
	}

	public JobVertexID getJobvertexId() {
		return this.jobVertex.getJobVertexId();
	}

	public String getTaskName() {
		return this.jobVertex.getJobVertex().getName();
	}

	/**
	 * Creates a simple name representation in the style 'taskname (x/y)', where
	 * 'taskname' is the name as returned by {@link #getTaskName()}, 'x' is the parallel
	 * subtask index as returned by {@link #getParallelSubtaskIndex()}{@code + 1}, and 'y' is the total
	 * number of tasks, as returned by {@link #getTotalNumberOfParallelSubtasks()}.
	 *
	 * @return A simple name representation in the form 'myTask (2/7)'
	 */
	@Override
	public String getTaskNameWithSubtaskIndex() {
		return this.taskNameWithSubtask;
	}

	public int getTotalNumberOfParallelSubtasks() {
		return this.jobVertex.getParallelism();
	}

	public int getMaxParallelism() {
		return this.jobVertex.getMaxParallelism();
	}

	public ResourceProfile getResourceProfile() {
		return this.jobVertex.getResourceProfile();
	}

	@Override
	public int getParallelSubtaskIndex() {
		return this.subTaskIndex;
	}

	public ExecutionVertexID getID() {
		return executionVertexId;
	}

	public int getNumberOfInputs() {
		return this.inputEdges.length;
	}

	public ExecutionEdge[] getInputEdges(int input) {
		if (input < 0 || input >= inputEdges.length) {
			throw new IllegalArgumentException(String.format("Input %d is out of range [0..%d)", input, inputEdges.length));
		}
		return inputEdges[input];
	}

	public ExecutionEdge[][] getAllInputEdges() {
		return inputEdges;
	}

	public CoLocationConstraint getLocationConstraint() {
		return locationConstraint;
	}

	public InputSplit getNextInputSplit(String host) {
		final int taskId = getParallelSubtaskIndex();
		synchronized (inputSplits) {
			final InputSplit nextInputSplit = jobVertex.getSplitAssigner().getNextInputSplit(host, taskId);
			if (nextInputSplit != null) {
				inputSplits.add(nextInputSplit);
			}
			return nextInputSplit;
		}
	}

	@Override
	public Execution getCurrentExecutionAttempt() {
		return currentExecution;
	}

	@Override
	public ExecutionState getExecutionState() {
		return currentExecution.getState();
	}

	@Override
	public long getStateTimestamp(ExecutionState state) {
		return currentExecution.getStateTimestamp(state);
	}

	@Override
	public String getFailureCauseAsString() {
		return ExceptionUtils.stringifyException(getFailureCause());
	}

	public Throwable getFailureCause() {
		return currentExecution.getFailureCause();
	}

	public CompletableFuture<TaskManagerLocation> getCurrentTaskManagerLocationFuture() {
		return currentExecution.getTaskManagerLocationFuture();
	}

	public LogicalSlot getCurrentAssignedResource() {
		return currentExecution.getAssignedResource();
	}

	@Override
	public TaskManagerLocation getCurrentAssignedResourceLocation() {
		return currentExecution.getAssignedResourceLocation();
	}

	@Nullable
	@Override
	public ArchivedExecution getPriorExecutionAttempt(int attemptNumber) {
		synchronized (priorExecutions) {
			if (attemptNumber >= 0 && attemptNumber < priorExecutions.size()) {
				return priorExecutions.get(attemptNumber);
			} else {
				throw new IllegalArgumentException("attempt does not exist");
			}
		}
	}

	public ArchivedExecution getLatestPriorExecution() {
		synchronized (priorExecutions) {
			final int size = priorExecutions.size();
			if (size > 0) {
				return priorExecutions.get(size - 1);
			}
			else {
				return null;
			}
		}
	}

	/**
	 * Gets the location where the latest completed/canceled/failed execution of the vertex's
	 * task happened.
	 *
	 * @return The latest prior execution location, or null, if there is none, yet.
	 */
	public TaskManagerLocation getLatestPriorLocation() {
		ArchivedExecution latestPriorExecution = getLatestPriorExecution();
		return latestPriorExecution != null ? latestPriorExecution.getAssignedResourceLocation() : null;
	}

	public AllocationID getLatestPriorAllocation() {
		ArchivedExecution latestPriorExecution = getLatestPriorExecution();
		return latestPriorExecution != null ? latestPriorExecution.getAssignedAllocationID() : null;
	}

	EvictingBoundedList<ArchivedExecution> getCopyOfPriorExecutionsList() {
		synchronized (priorExecutions) {
			return new EvictingBoundedList<>(priorExecutions);
		}
	}

	public ExecutionGraph getExecutionGraph() {
		return this.jobVertex.getGraph();
	}

	public Map<IntermediateResultPartitionID, IntermediateResultPartition> getProducedPartitions() {
		return resultPartitions;
	}

	public InputDependencyConstraint getInputDependencyConstraint() {
		return getJobVertex().getInputDependencyConstraint();
	}

	// --------------------------------------------------------------------------------------------
	//  Graph building
	// --------------------------------------------------------------------------------------------

	public void connectSource(int inputNumber, IntermediateResult source, JobEdge edge, int consumerNumber) {

		final DistributionPattern pattern = edge.getDistributionPattern();
		final IntermediateResultPartition[] sourcePartitions = source.getPartitions();

		ExecutionEdge[] edges;

		switch (pattern) {
			case POINTWISE:
				edges = connectPointwise(sourcePartitions, inputNumber);
				break;

			case ALL_TO_ALL:
				edges = connectAllToAll(sourcePartitions, inputNumber);
				break;

			default:
				throw new RuntimeException("Unrecognized distribution pattern.");

		}

		inputEdges[inputNumber] = edges;

		// add the consumers to the source
		// for now (until the receiver initiated handshake is in place), we need to register the
		// edges as the execution graph
		for (ExecutionEdge ee : edges) {
			ee.getSource().addConsumer(ee, consumerNumber);
		}
	}

	private ExecutionEdge[] connectAllToAll(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		ExecutionEdge[] edges = new ExecutionEdge[sourcePartitions.length];

		for (int i = 0; i < sourcePartitions.length; i++) {
			IntermediateResultPartition irp = sourcePartitions[i];
			edges[i] = new ExecutionEdge(irp, this, inputNumber);
		}

		return edges;
	}

	private ExecutionEdge[] connectPointwise(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		final int numSources = sourcePartitions.length;
		final int parallelism = getTotalNumberOfParallelSubtasks();

		// simple case same number of sources as targets
		if (numSources == parallelism) {
			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[subTaskIndex], this, inputNumber) };
		}
		else if (numSources < parallelism) {

			int sourcePartition;

			// check if the pattern is regular or irregular
			// we use int arithmetics for regular, and floating point with rounding for irregular
			if (parallelism % numSources == 0) {
				// same number of targets per source
				int factor = parallelism / numSources;
				sourcePartition = subTaskIndex / factor;
			}
			else {
				// different number of targets per source
				float factor = ((float) parallelism) / numSources;
				sourcePartition = (int) (subTaskIndex / factor);
			}

			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[sourcePartition], this, inputNumber) };
		}
		else {
			if (numSources % parallelism == 0) {
				// same number of targets per source
				int factor = numSources / parallelism;
				int startIndex = subTaskIndex * factor;

				ExecutionEdge[] edges = new ExecutionEdge[factor];
				for (int i = 0; i < factor; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[startIndex + i], this, inputNumber);
				}
				return edges;
			}
			else {
				float factor = ((float) numSources) / parallelism;

				int start = (int) (subTaskIndex * factor);
				int end = (subTaskIndex == getTotalNumberOfParallelSubtasks() - 1) ?
						sourcePartitions.length :
						(int) ((subTaskIndex + 1) * factor);

				ExecutionEdge[] edges = new ExecutionEdge[end - start];
				for (int i = 0; i < edges.length; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[start + i], this, inputNumber);
				}

				return edges;
			}
		}
	}

	/**
	 * Gets the overall preferred execution location for this vertex's current execution.
	 * The preference is determined as follows:
	 *
	 * <ol>
	 *     <li>If the task execution has state to load (from a checkpoint), then the location preference
	 *         is the location of the previous execution (if there is a previous execution attempt).
	 *     <li>If the task execution has no state or no previous location, then the location preference
	 *         is based on the task's inputs.
	 * </ol>
	 *
	 * <p>These rules should result in the following behavior:
	 *
	 * <ul>
	 *     <li>Stateless tasks are always scheduled based on co-location with inputs.
	 *     <li>Stateful tasks are on their initial attempt executed based on co-location with inputs.
	 *     <li>Repeated executions of stateful tasks try to co-locate the execution with its state.
	 * </ul>
	 *
	 * @see #getPreferredLocationsBasedOnState()
	 * @see #getPreferredLocationsBasedOnInputs()
	 *
	 * @return The preferred execution locations for the execution attempt.
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocations() {
		Collection<CompletableFuture<TaskManagerLocation>> basedOnState = getPreferredLocationsBasedOnState();
		return basedOnState != null ? basedOnState : getPreferredLocationsBasedOnInputs();
	}

	/**
	 * Gets the preferred location to execute the current task execution attempt, based on the state
	 * that the execution attempt will resume.
	 *
	 * @return A size-one collection with the location preference, or null, if there is no
	 *         location preference based on the state.
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnState() {
		TaskManagerLocation priorLocation;
		if (currentExecution.getTaskRestore() != null && (priorLocation = getLatestPriorLocation()) != null) {
			return Collections.singleton(CompletableFuture.completedFuture(priorLocation));
		}
		else {
			return null;
		}
	}

	/**
	 * Gets the preferred location to execute the current task execution attempt, based on the state
	 * that the execution attempt will resume.
	 */
	public Optional<TaskManagerLocation> getPreferredLocationBasedOnState() {
		if (currentExecution.getTaskRestore() != null) {
			return Optional.ofNullable(getLatestPriorLocation());
		}

		return Optional.empty();
	}

	/**
	 * Gets the location preferences of the vertex's current task execution, as determined by the locations
	 * of the predecessors from which it receives input data.
	 * If there are more than MAX_DISTINCT_LOCATIONS_TO_CONSIDER different locations of source data, this
	 * method returns {@code null} to indicate no location preference.
	 *
	 * @return The preferred locations based in input streams, or an empty iterable,
	 *         if there is no input-based preference.
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnInputs() {
		// otherwise, base the preferred locations on the input connections
		if (inputEdges == null) {
			return Collections.emptySet();
		}
		else {
			Set<CompletableFuture<TaskManagerLocation>> locations = new HashSet<>(getTotalNumberOfParallelSubtasks());
			Set<CompletableFuture<TaskManagerLocation>> inputLocations = new HashSet<>(getTotalNumberOfParallelSubtasks());

			// go over all inputs
			for (int i = 0; i < inputEdges.length; i++) {
				inputLocations.clear();
				ExecutionEdge[] sources = inputEdges[i];
				if (sources != null) {
					// go over all input sources
					for (int k = 0; k < sources.length; k++) {
						// look-up assigned slot of input source
						CompletableFuture<TaskManagerLocation> locationFuture = sources[k].getSource().getProducer().getCurrentTaskManagerLocationFuture();
						// add input location
						inputLocations.add(locationFuture);
						// inputs which have too many distinct sources are not considered
						if (inputLocations.size() > MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
							inputLocations.clear();
							break;
						}
					}
				}
				// keep the locations of the input with the least preferred locations
				if (locations.isEmpty() || // nothing assigned yet
						(!inputLocations.isEmpty() && inputLocations.size() < locations.size())) {
					// current input has fewer preferred locations
					locations.clear();
					locations.addAll(inputLocations);
				}
			}

			return locations.isEmpty() ? Collections.emptyList() : locations;
		}
	}

	// --------------------------------------------------------------------------------------------
	//   Actions
	// --------------------------------------------------------------------------------------------

	/**
	 * Archives the current Execution and creates a new Execution for this vertex.
	 *
	 * <p>This method atomically checks if the ExecutionGraph is still of an expected
	 * global mod. version and replaces the execution if that is the case. If the ExecutionGraph
	 * has increased its global mod. version in the meantime, this operation fails.
	 *
	 * <p>This mechanism can be used to prevent conflicts between various concurrent recovery and
	 * reconfiguration actions in a similar way as "optimistic concurrency control".
	 *
	 * @param timestamp
	 *             The creation timestamp for the new Execution
	 * @param originatingGlobalModVersion
	 *
	 * @return Returns the new created Execution.
	 *
	 * @throws GlobalModVersionMismatch Thrown, if the execution graph has a new global mod
	 *                                  version than the one passed to this message.
	 */
	public Execution resetForNewExecution(final long timestamp, final long originatingGlobalModVersion)
			throws GlobalModVersionMismatch {
		LOG.debug("Resetting execution vertex {} for new execution.", getTaskNameWithSubtaskIndex());

		synchronized (priorExecutions) {
			// check if another global modification has been triggered since the
			// action that originally caused this reset/restart happened
			final long actualModVersion = getExecutionGraph().getGlobalModVersion();
			if (actualModVersion > originatingGlobalModVersion) {
				// global change happened since, reject this action
				throw new GlobalModVersionMismatch(originatingGlobalModVersion, actualModVersion);
			}

			return resetForNewExecutionInternal(timestamp, originatingGlobalModVersion);
		}
	}

	public void resetForNewExecution() {
		resetForNewExecutionInternal(System.currentTimeMillis(), getExecutionGraph().getGlobalModVersion());
	}

	private Execution resetForNewExecutionInternal(final long timestamp, final long originatingGlobalModVersion) {
		final Execution oldExecution = currentExecution;
		final ExecutionState oldState = oldExecution.getState();

		if (oldState.isTerminal()) {
			if (oldState == FINISHED) {
				// pipelined partitions are released in Execution#cancel(), covering both job failures and vertex resets
				// do not release pipelined partitions here to save RPC calls
				oldExecution.handlePartitionCleanup(false, true);
				getExecutionGraph().getPartitionReleaseStrategy().vertexUnfinished(executionVertexId);
			}

			priorExecutions.add(oldExecution.archive());

			final Execution newExecution = new Execution(
				getExecutionGraph().getFutureExecutor(),
				this,
				oldExecution.getAttemptNumber() + 1,
				originatingGlobalModVersion,
				timestamp,
				timeout);

			currentExecution = newExecution;

			synchronized (inputSplits) {
				InputSplitAssigner assigner = jobVertex.getSplitAssigner();
				if (assigner != null) {
					assigner.returnInputSplit(inputSplits, getParallelSubtaskIndex());
					inputSplits.clear();
				}
			}

			CoLocationGroup grp = jobVertex.getCoLocationGroup();
			if (grp != null) {
				locationConstraint = grp.getLocationConstraint(subTaskIndex);
			}

			// register this execution at the execution graph, to receive call backs
			getExecutionGraph().registerExecution(newExecution);

			// if the execution was 'FINISHED' before, tell the ExecutionGraph that
			// we take one step back on the road to reaching global FINISHED
			if (oldState == FINISHED) {
				getExecutionGraph().vertexUnFinished();
			}

			// reset the intermediate results
			for (IntermediateResultPartition resultPartition : resultPartitions.values()) {
				resultPartition.resetForNewExecution();
			}

			return newExecution;
		}
		else {
			throw new IllegalStateException("Cannot reset a vertex that is in non-terminal state " + oldState);
		}
	}

	/**
	 * Schedules the current execution of this ExecutionVertex.
	 *
	 * @param slotProviderStrategy to allocate the slots from
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @param allPreviousExecutionGraphAllocationIds set with all previous allocation ids in the job graph.
	 *                                                 Can be empty if the allocation ids are not required for scheduling.
	 * @return Future which is completed once the execution is deployed. The future
	 * can also completed exceptionally.
	 */
	public CompletableFuture<Void> scheduleForExecution(
			SlotProviderStrategy slotProviderStrategy,
			LocationPreferenceConstraint locationPreferenceConstraint,
			@Nonnull Set<AllocationID> allPreviousExecutionGraphAllocationIds) {

		LOG.info("executionVertex:scheduleForExecution(");
		return this.currentExecution.scheduleForExecution(
			slotProviderStrategy,
			locationPreferenceConstraint,
			allPreviousExecutionGraphAllocationIds);
	}

	public void tryAssignResource(LogicalSlot slot) {
//		if (!currentExecution.tryAssignResource(slot)) {
//			throw new IllegalStateException("Could not assign resource " + slot + " to current execution " +
//				currentExecution + '.');
//		}
		if(standCount==0){
			if (!currentExecution.tryAssignResource(slot)) {
				throw new IllegalStateException("Could not assign resource " + slot + " to current execution " +
					currentExecution + '.');
			}
			standCount++;
		}
		else {
			for (Execution execution:standbyExecutions){
				if (!execution.tryAssignResource(slot)) {
					throw new IllegalStateException("Could not assign resource " + slot + " to current execution " +
						currentExecution + '.');
				}
			}
		}

	}

	public void deploy() throws JobException {
		if(flag==0){
			count++;
			flag++;
			//System.out.println("我被部署第"+count+"次");
			currentExecution.deploy();
		}
		else {
			for (Execution execution:standbyExecutions){
				count++;
				//System.out.println("我被部署第"+count+"次");
				execution.deploy();
			}
		}
//		count++;
//		System.out.println("我被部署第"+count+"次");
//		currentExecution.deploy();
	}

	@VisibleForTesting
	public void deployToSlot(LogicalSlot slot) throws JobException {
		if (currentExecution.tryAssignResource(slot)) {
			currentExecution.deploy();
		} else {
			throw new IllegalStateException("Could not assign resource " + slot + " to current execution " +
				currentExecution + '.');
		}
	}

	/**
	 * Cancels this ExecutionVertex.
	 *
	 * @return A future that completes once the execution has reached its final state.
	 */
	public CompletableFuture<?> cancel() {
		// to avoid any case of mixup in the presence of concurrent calls,
		// we copy a reference to the stack to make sure both calls go to the same Execution
		final Execution exec = currentExecution;
		exec.cancel();
		return exec.getReleaseFuture();
	}

	public CompletableFuture<?> suspend() {
		return currentExecution.suspend();
	}

	public void fail(Throwable t) {
		currentExecution.fail(t);
	}

	/**
	 * This method marks the task as failed, but will make no attempt to remove task execution from the task manager.
	 * It is intended for cases where the task is known not to be deployed yet.
	 *
	 * @param t The exception that caused the task to fail.
	 */
	public void markFailed(Throwable t) {
		currentExecution.markFailed(t);
	}

	/**
	 * Schedules or updates the consumer tasks of the result partition with the given ID.
	 */
	void scheduleOrUpdateConsumers(ResultPartitionID partitionId) {

		final Execution execution = currentExecution;

		// Abort this request if there was a concurrent reset
		if (!partitionId.getProducerId().equals(execution.getAttemptId())) {
			return;
		}

		final IntermediateResultPartition partition = resultPartitions.get(partitionId.getPartitionId());

		if (partition == null) {
			throw new IllegalStateException("Unknown partition " + partitionId + ".");
		}

		partition.markDataProduced();

		if (partition.getIntermediateResult().getResultType().isPipelined()) {
			// Schedule or update receivers of this partition
			execution.scheduleOrUpdateConsumers(partition.getConsumers());
		}
		else {
			throw new IllegalArgumentException("ScheduleOrUpdateConsumers msg is only valid for" +
					"pipelined partitions.");
		}
	}

	void cachePartitionInfo(PartitionInfo partitionInfo){
		getCurrentExecutionAttempt().cachePartitionInfo(partitionInfo);
	}

	/**
	 * Returns all blocking result partitions whose receivers can be scheduled/updated.
	 */
	List<IntermediateResultPartition> finishAllBlockingPartitions() {
		List<IntermediateResultPartition> finishedBlockingPartitions = null;

		for (IntermediateResultPartition partition : resultPartitions.values()) {
			if (partition.getResultType().isBlocking() && partition.markFinished()) {
				if (finishedBlockingPartitions == null) {
					finishedBlockingPartitions = new LinkedList<IntermediateResultPartition>();
				}

				finishedBlockingPartitions.add(partition);
			}
		}

		if (finishedBlockingPartitions == null) {
			return Collections.emptyList();
		}
		else {
			return finishedBlockingPartitions;
		}
	}

	/**
	 * Check whether the InputDependencyConstraint is satisfied for this vertex.
	 *
	 * @return whether the input constraint is satisfied
	 */
	boolean checkInputDependencyConstraints() {
		if (inputEdges.length == 0) {
			return true;
		}

		final InputDependencyConstraint inputDependencyConstraint = getInputDependencyConstraint();
		switch (inputDependencyConstraint) {
			case ANY:
				return isAnyInputConsumable();
			case ALL:
				return areAllInputsConsumable();
			default:
				throw new IllegalStateException("Unknown InputDependencyConstraint " + inputDependencyConstraint);
		}
	}

	private boolean isAnyInputConsumable() {
		for (int inputNumber = 0; inputNumber < inputEdges.length; inputNumber++) {
			if (isInputConsumable(inputNumber)) {
				return true;
			}
		}
		return false;
	}

	private boolean areAllInputsConsumable() {
		for (int inputNumber = 0; inputNumber < inputEdges.length; inputNumber++) {
			if (!isInputConsumable(inputNumber)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Get whether an input of the vertex is consumable.
	 * An input is consumable when when any partition in it is consumable.
	 *
	 * <p>Note that a BLOCKING result partition is only consumable when all partitions in the result are FINISHED.
	 *
	 * @return whether the input is consumable
	 */
	boolean isInputConsumable(int inputNumber) {
		for (ExecutionEdge executionEdge : inputEdges[inputNumber]) {
			if (executionEdge.getSource().isConsumable()) {
				return true;
			}
		}
		return false;
	}

	// --------------------------------------------------------------------------------------------
	//   Notifications from the Execution Attempt
	// --------------------------------------------------------------------------------------------

	void executionFinished(Execution execution) {
		getExecutionGraph().vertexFinished();
	}

	// --------------------------------------------------------------------------------------------
	//   Miscellaneous
	// --------------------------------------------------------------------------------------------

	void notifyPendingDeployment(Execution execution) {
		// only forward this notification if the execution is still the current execution
		// otherwise we have an outdated execution
		if (isCurrentExecution(execution)) {
			getExecutionGraph().getExecutionDeploymentListener().onStartedDeployment(
				execution.getAttemptId(),
				execution.getAssignedResourceLocation().getResourceID());
		}
	}

	void notifyCompletedDeployment(Execution execution) {
		// only forward this notification if the execution is still the current execution
		// otherwise we have an outdated execution
		if (isCurrentExecution(execution)) {
			getExecutionGraph().getExecutionDeploymentListener().onCompletedDeployment(
				execution.getAttemptId()
			);
		}
	}

	/**
	 * Simply forward this notification.
	 */
	void notifyStateTransition(Execution execution, ExecutionState newState, Throwable error) {
		// only forward this notification if the execution is still the current execution
		// otherwise we have an outdated execution
		if (isCurrentExecution(execution)) {
			LOG.info("if (isCurrentExecution(execution)) {");
			getExecutionGraph().notifyExecutionChange(execution, newState, error);
		}
	}

	private boolean isCurrentExecution(Execution execution) {
		return currentExecution == execution;
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return getTaskNameWithSubtaskIndex();
	}

	@Override
	public ArchivedExecutionVertex archive() {
		return new ArchivedExecutionVertex(this);
	}

	public boolean isLegacyScheduling() {
		return getExecutionGraph().isLegacyScheduling();
	}

	/************************************************************************************************************************************/
	/*
	todo 赫明萱后加
	 */
	public ArrayList<Execution> getStandbyExecutions() {
		return new ArrayList<>(standbyExecutions);
	}

	public VertexID getVertexId() {
		return executionVertexId;
	}

	public void runStandbyExecution() throws IllegalStateException {
		if (standbyExecutions.isEmpty()) {
			throw new IllegalStateException("No standby execution to run.");
		}

		Execution firstStandbyExecution = this.standbyExecutions.remove(0);
		priorExecutions.add(currentExecution.archive());
		currentExecution = firstStandbyExecution;

		currentExecution.runStandbyExecution();  //运行这个Execution,并解除StreamTask的阻塞


		boolean updateConsumersOnFailover = true;

		//Tell consumers of failed operator to reconfigure connections 告诉故障操作员的使用者重新配置连接
		LOG.debug("Update the to-run standby task's " + currentExecution + " consumer vertices (if any).");
		// 一个分区，有多个边，每个边连接一个executionvertice
		for (IntermediateResultPartition partition : resultPartitions.values()) {
			checkPartition(partition);
			final HashSet<ExecutionVertex> consumerDeduplicator1 = new HashSet<>();
			currentExecution.scheduleOrUpdateConsumers(partition.getConsumers(),consumerDeduplicator1,
				updateConsumersOnFailover);
		}

		//Tell producers for failed operator to reconfigure connections  告诉失败操作员的生产者重新配置连接
		for (ExecutionEdge[] edges : inputEdges) {
			for (ExecutionEdge edge : edges) {
				List<List<ExecutionEdge>> currentConsumerEdge = new ArrayList<List<ExecutionEdge>>(0);
				currentConsumerEdge.add(new ArrayList<ExecutionEdge>(0));
				currentConsumerEdge.get(0).add(edge);

				checkPartition(edge.getSource());
				Execution producer = edge.getSource().getProducer().getCurrentExecutionAttempt();

				LOG.debug("Update the " + producer + " producer execution's consumer execution " + currentExecution + ".");
				final HashSet<ExecutionVertex> consumerDeduplicator2 = new HashSet<>();
				producer.scheduleOrUpdateConsumers(currentConsumerEdge,consumerDeduplicator2,
					//producer.scheduleOrUpdateConsumers(edge.getSource().getConsumers(),
					updateConsumersOnFailover);
			}
		}

	}

	/**
	 * Check intermediate result partition is pipelined and not null. 新加的
	 */
	private void checkPartition(IntermediateResultPartition partition) {
		if (partition == null) {
			throw new IllegalStateException("Unknown partition " + partition + ".");
		}

		if (!partition.getIntermediateResult().getResultType().isPipelined()) {
			throw new IllegalArgumentException("ScheduleOrUpdateConsumers msg is only valid for" +
				"pipelined partitions.");
		}
	}

	public List<ExecutionVertex> getDirectUpstreamVertexes() {
		return Arrays.stream(inputEdges).flatMap(Arrays::stream).map(ExecutionEdge::getSource)
			.map(IntermediateResultPartition::getProducer).distinct().collect(Collectors.toList());
	}

//	public CompletableFuture<Void> addStandbyExecution() {
//
//		LOG.info("addStandbyExecution()qina");
//		final long globalModVersion = currentExecution.getGlobalModVersion();
//		final long createTimestamp = System.currentTimeMillis();
//		final boolean isStandby = true;
//
//		Execution newStandbyExecution = new Execution(
//			getExecutionGraph().getFutureExecutor(),
//			this,
//			currentExecution.getAttemptNumber() + 1,
//			globalModVersion,
//			createTimestamp,
//			timeout,
//			isStandby);
//		LOG.info("addStandbyExecution()hou");
//		standbyExecutions.add(newStandbyExecution);
//		LOG.info("Added standby execution for task");
//		// register this execution at the execution graph, to receive call backs such as task state updates.
//		getExecutionGraph().registerExecution(newStandbyExecution);
//		LOG.info("Added standby execution for task " + taskNameWithSubtask + "and registered it to running executions. Now scheduling it.");
//		//todo 在这里为新增加的execution 分配slot
//		return newStandbyExecution.scheduleForExecution();
//
//	}


	public void addStandbyExecution() {

		LOG.info("addStandbyExecution()前");
		final long globalModVersion = currentExecution.getGlobalModVersion();
		final long createTimestamp = System.currentTimeMillis();
		final boolean isStandby = true;

		Execution newStandbyExecution = new Execution(
			getExecutionGraph().getFutureExecutor(),
			this,
			currentExecution.getAttemptNumber() + 1,
			globalModVersion,
			createTimestamp,
			timeout,
			isStandby);
		LOG.info("addStandbyExecution()后");
		standbyExecutions.add(newStandbyExecution);
		LOG.info("Added standby execution for task");
		// register this execution at the execution graph, to receive call backs such as task state updates.
		getExecutionGraph().registerExecution(newStandbyExecution);
		LOG.info("Added standby execution for task " + taskNameWithSubtask + "and registered it to running executions. Now scheduling it.");
		//todo 在这里为新增加的execution 分配slot
		newStandbyExecution.scheduleForExecution();
	}

	public CompletableFuture<?> cancelStandbyExecution() {
		if (!standbyExecutions.isEmpty()) {
			LOG.debug(String.format("Cancelling standby execution %s", this));
			final Execution standbyExecution = standbyExecutions.remove(0);
			standbyExecution.cancel();
			return standbyExecution.getReleaseFuture();
		}
		return null;
	}

	public List<ExecutionVertex> getDownstreamVertexes() {
		List<ExecutionVertex> toReturn = new LinkedList<>();
		Deque<ExecutionVertex> unexplored = new LinkedList<>(getDirectDownstreamVertexes());

		while (!unexplored.isEmpty()){
			ExecutionVertex toExplore = unexplored.pop();
			toReturn.add(toExplore);

			unexplored.addAll(toExplore.getDirectDownstreamVertexes());
		}
		return toReturn.stream().distinct().collect(Collectors.toList());
	}

	public List<ExecutionVertex> getDirectDownstreamVertexes() {
		return getProducedPartitions().values().stream()
			.flatMap(x -> x.getConsumers().stream().flatMap(Collection::stream))
			.map(ExecutionEdge::getTarget).distinct().collect(Collectors.toList());
	}

	public void ignoreCheckpoint(long checkpointId) {

		try {
			this.currentExecution.ignoreCheckpointRpcCall(checkpointId).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

	}

	//todo 新加的
	public boolean concurrentFailExecutionSignal() {
		final long currentTimeMillis = System.currentTimeMillis();
		final int currentAttemptNumber = currentExecution.getAttemptNumber();
		final long currentRunningTime = currentExecution.getStateTimestamp(RUNNING);

		if (currentExecution.getState() == RUNNING &&
			// The following condition states that a standby execution is running
			// at least for a minimal amount of time. This is an empirical rule
			// to try and rule out subsequent fail signals that arrive late.
			(currentAttemptNumber > 0 && currentTimeMillis - currentRunningTime >
				failSignalTimeSpan.toMilliseconds() ||
				currentAttemptNumber == 0)) {
			// Ignore subsequent signals for failing the same execution instance.
			if (currentAttemptNumber > lastKillAttemptNumber) {
				lastKillAttemptNumber = currentAttemptNumber;
				LOG.debug("Allow to fail current execution {}.", currentExecution);
				return false;
			}
		}
		LOG.debug("Ignore subsequent signal to fail current execution {} of vertex {}.", currentExecution, this);
		return true;
	}

}
