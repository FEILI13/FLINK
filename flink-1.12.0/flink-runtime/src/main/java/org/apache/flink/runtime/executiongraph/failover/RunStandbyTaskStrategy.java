package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RunStandbyTaskStrategy extends FailoverStrategy{


	private static final Logger LOG = LoggerFactory.getLogger(RunStandbyTaskStrategy.class);

	/**
	 * The execution graph to recover
	 */
	private final ExecutionGraph executionGraph;

	/**
	 * The executor for creating, connecting, scheduling, and running a STANDBY task
	 */
	private final Executor callbackExecutor;

	private final int numStandbyTasksToMaintain;
	private final int checkpointCoordinatorBackoffMultiplier;
	private final long checkpointCoordinatorBackoffBaseMs;

	private final HashSet<ResourceID> failedResources = new HashSet<>();
	private final static Object lock = new Object();


	/**
	 * Creates a new failover strategy that recovers from failures by restarting all tasks
	 * of the execution graph.
	 *
	 * @param executionGraph            The execution graph to handle.
	 * @param numStandbyTasksToMaintain
	 */
	public RunStandbyTaskStrategy(ExecutionGraph executionGraph, int numStandbyTasksToMaintain,
								  int checkpointCoordinatorBackoffMultiplier,
								  long checkpointCoordinatorBackoffBaseMs) {
		this.executionGraph = checkNotNull(executionGraph);
		this.callbackExecutor = checkNotNull(executionGraph.getFutureExecutor());
		this.numStandbyTasksToMaintain = numStandbyTasksToMaintain;
		this.checkpointCoordinatorBackoffMultiplier = checkpointCoordinatorBackoffMultiplier;
		this.checkpointCoordinatorBackoffBaseMs = checkpointCoordinatorBackoffBaseMs;
	}
	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		// trigger the restart once the task has reached its terminal state
		// Note: currently all tasks passed here are already in their terminal state,
		//       so we could actually avoid the future. We use it anyways because it is cheap and
		//       it helps to support better testing
		final ExecutionVertex vertexToRecover = taskExecution.getVertex();

		ResourceID resourceIDOfFailedTM = taskExecution.getAssignedResourceLocation().getResourceID();
		//The plan to recover the vertex is the following:
		// If a standby already exists:
		// 		Concurrently remove the failed slots and start the standby
		// else
		//		Sequentially remove the failed slots, then schedule a new standby and dispatch state to it. Doing this
		//		avoids scheduling the new standby to the failed TM. Following that, start it.
		// If an error occurs anywhere in this process, we fallback to the global restart strategy

		//It is also important to signal to other tasks to ignore any checkpoints unacknowledged by the failed task.
		// Otherwise blocking would occur.

		LOG.info("{} failover strategy is triggered for the recovery of task vertex {} in TM {}", getStrategyName(), taskExecution.getVertex().getVertexId(), resourceIDOfFailedTM);


		CompletableFuture<Void> removeSlotsFuture = removeFailedSlots(taskExecution);

		//By default, there should already be a standby ready 默认情况下，应该已经有一个备用服务器
		CompletableFuture<Void> standbyReady = CompletableFuture.completedFuture(null);

		//If there isnt, we need to wait for the remove slots to complete, before scheduling a new standby
		//This guarantees we do not reschedule to the same slot
		if (vertexToRecover.getStandbyExecutions().size() == 0)
			standbyReady = composePrepareNewStandby(vertexToRecover, removeSlotsFuture);

		//If there was a standby, this runs without waiting for the failed vertex to be removed, otherwise it performs
		//the necessary steps first
		standbyReady.thenAcceptAsync((Void) -> {
			LOG.info("Running the standby execution.");
			vertexToRecover.runStandbyExecution();
		}, callbackExecutor);

		//In case of exceptions during the whole execution, trigger full recovery
		standbyReady.exceptionally((Throwable t) -> {
			executionGraph.failGlobal(
				new Exception("Error during standby task recovery, triggering full recovery: ", t));
			return null;
		});
	}

	public void onTaskFailure(ExecutionVertexID taskExecutionID, Throwable cause) {
		// trigger the restart once the task has reached its terminal state
		// Note: currently all tasks passed here are already in their terminal state,
		//       so we could actually avoid the future. We use it anyways because it is cheap and
		//       it helps to support better testing

		Execution taskExecution = executionGraph.getJobVertex(taskExecutionID.getJobVertexId()).getTaskVertices()[taskExecutionID.getSubtaskIndex()].getCurrentExecutionAttempt();
		final ExecutionVertex vertexToRecover = taskExecution.getVertex();

		ResourceID resourceIDOfFailedTM = taskExecution.getAssignedResourceLocation().getResourceID();
		//The plan to recover the vertex is the following:
		// If a standby already exists:
		// 		Concurrently remove the failed slots and start the standby
		// else
		//		Sequentially remove the failed slots, then schedule a new standby and dispatch state to it. Doing this
		//		avoids scheduling the new standby to the failed TM. Following that, start it.
		// If an error occurs anywhere in this process, we fallback to the global restart strategy

		//It is also important to signal to other tasks to ignore any checkpoints unacknowledged by the failed task.
		// Otherwise blocking would occur.

		LOG.info("{} failover strategy is triggered for the recovery of task vertex {} in TM {}", getStrategyName(), taskExecution.getVertex().getVertexId(), resourceIDOfFailedTM);


		CompletableFuture<Void> removeSlotsFuture = removeFailedSlots(taskExecution);

		//By default, there should already be a standby ready 默认情况下，应该已经有一个备用服务器
		CompletableFuture<Void> standbyReady = CompletableFuture.completedFuture(null);

		//If there isnt, we need to wait for the remove slots to complete, before scheduling a new standby
		//This guarantees we do not reschedule to the same slot
		if (vertexToRecover.getStandbyExecutions().size() == 0)
			standbyReady = composePrepareNewStandby(vertexToRecover, removeSlotsFuture);

		//If there was a standby, this runs without waiting for the failed vertex to be removed, otherwise it performs
		//the necessary steps first
		standbyReady.thenAcceptAsync((Void) -> {
			LOG.info("Running the standby execution.");
			vertexToRecover.runStandbyExecution();
		}, callbackExecutor);

		//In case of exceptions during the whole execution, trigger full recovery
		standbyReady.exceptionally((Throwable t) -> {
			executionGraph.failGlobal(
				new Exception("Error during standby task recovery, triggering full recovery: ", t));
			return null;
		});
	}

	private CompletableFuture<Void> composePrepareNewStandby(ExecutionVertex vertexToRecover,
															 CompletableFuture<Void> releaseSlotsFuture) {
		return releaseSlotsFuture.thenComposeAsync((Void) -> {
			LOG.info("Waiting for upstreams to be deployed before adding standby");
			while (vertexToRecover.getDirectUpstreamVertexes().stream().map(ExecutionVertex::getExecutionState)
				.anyMatch(x -> x != ExecutionState.RUNNING)) ;
			LOG.info("赫明萱composePrepareNewStandby调用addStandbyExecution()");
			LOG.info("赫明萱"+vertexToRecover+"调用addStandbyExecution()");
			vertexToRecover.addStandbyExecution();
			LOG.info("Waiting for standby to be ready");
			Execution standby = vertexToRecover.getStandbyExecutions().get(0);
			while (standby.getState() != ExecutionState.STANDBY) ;
			LOG.info("Standby is ready.");
			LOG.info("Dispatching latest state.");
			try {
				LOG.info("赫明萱composePrepareNewStandby调用completePendingCheckpoint");
				executionGraph.getCheckpointCoordinator().dispatchLatestCheckpointedStateToStandbyTasks(
					Collections.singletonMap(vertexToRecover.getJobvertexId(), vertexToRecover.getJobVertex()),
					false, true);
			} catch (Exception e) {
				throw new CompletionException(e);
			}
			return CompletableFuture.completedFuture(null);
		}, callbackExecutor);
	}

	private CompletableFuture<Void> removeFailedSlots(Execution taskExecution) {
		return CompletableFuture.supplyAsync(() -> {
			ResourceID resourceIDOfFailedTM = taskExecution.getAssignedResourceLocation().getResourceID();
			LOG.info("Checking if need to remove failed slots");
			synchronized (lock) {
				if (failedResources.contains(resourceIDOfFailedTM))
					return null;
				LOG.info("Failing resource {}", resourceIDOfFailedTM);
				failedResources.add(resourceIDOfFailedTM);
				ResourceManagerGateway rmGateway =
					executionGraph.getResourceManagerConnection().getResourceManagerGateway();
				SlotPool slotPool = executionGraph.getSlotPool();
				FlinkException exception = new FlinkException("Disconnecting Task Manager");

				LOG.info("Releasing task manager slots and disconnecting");
				slotPool.releaseTaskManager(resourceIDOfFailedTM, exception);
				rmGateway.disconnectTaskManager(resourceIDOfFailedTM, exception);

				LOG.info("Discarding pending checkpoints unacknowledged by failed task and restarting checkpoint " +
					"coordinator" +
					" " +
					"with backoff");
				this.executionGraph.getCheckpointCoordinator().rpcIgnoreUnacknowledgedPendingCheckpointsFor(taskExecution.getVertex(), new Exception("Task failed and is recovering causally."));
				this.executionGraph.getCheckpointCoordinator().restartBackoffCheckpointScheduler(checkpointCoordinatorBackoffMultiplier, checkpointCoordinatorBackoffBaseMs);
			}
			return null;
		}, callbackExecutor);
	}

	@Override
//	public void notifyNewVertices(List<ExecutionJobVertex> newExecutionJobVerticesTopological) {
//		final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>();
//
//		LOG.info("standby notifyNewVertices numStandbyTasksToMaintain {}",numStandbyTasksToMaintain);
//		for (int i = 0; i < numStandbyTasksToMaintain; i++) {
//			for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
//				LOG.info("赫明萱executionJobVertex：   "+executionJobVertex);
////				try {
////					Thread.sleep(1000);
////				} catch (InterruptedException e) {
////					throw new RuntimeException(e);
////				}
//				for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
//					LOG.info("赫明萱executionVertex：   "+executionVertex);
//
//					final CompletableFuture<Void> currentExecutionFuture =
//						// TODO: Anti-affinity constraint
//						CompletableFuture.runAsync(
//							() -> waitForExecutionToReachRunningState(executionVertex));
//					currentExecutionFuture.whenComplete(
//						(Void ignored, Throwable t) -> {
//							if (t == null) {
//								// this should aalso respect the topological order
//								LOG.info("赫明萱notifyNewVertices调用addStandbyExecution()");
//								LOG.info("赫明萱executionVertex：   "+executionVertex+"调用addStandbyExecution()");
//								final CompletableFuture<Void> standbyExecutionFuture = executionVertex.addStandbyExecution();
//								schedulingFutures.add(standbyExecutionFuture);
//							} else {
//
//								LOG.info("Throwable {}",t);
//								schedulingFutures.add(
//									new CompletableFuture<>());
//								schedulingFutures.get(schedulingFutures.size() - 1)
//									.completeExceptionally(t);
//							}
//						});
//				}
//			}
//		}
//
//		final CompletableFuture<Void> allSchedulingFutures = FutureUtils.waitForAll(schedulingFutures);
//		allSchedulingFutures.whenComplete((Void ignored, Throwable t) -> {
//			if (t != null) {
//				LOG.warn("Scheduling of standby tasks in '" +
//					getStrategyName() + "' failed. Cancelling the scheduling of standby tasks.");
//				for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
//					for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
//						executionVertex.cancelStandbyExecution();
//					}
//				}
//			}
//		});
//	}

	public void notifyNewVertices(List<ExecutionJobVertex> newExecutionJobVerticesTopological) {
		final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>();

		LOG.info("standby notifyNewVertices numStandbyTasksToMaintain {}",numStandbyTasksToMaintain);
		for (int i = 0; i < numStandbyTasksToMaintain; i++) {
			for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
				LOG.info("赫明萱executionJobVertex：   "+executionJobVertex);
//				try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//					throw new RuntimeException(e);
//				}
				for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
					LOG.info("赫明萱executionVertex：   "+executionVertex);

//					waitForExecutionToReachRunningState(executionVertex);

					LOG.info("赫明萱notifyNewVertices调用addStandbyExecution()");
					LOG.info("赫明萱executionVertex：   "+executionVertex+"调用addStandbyExecution()");
					executionVertex.addStandbyExecution();

//					final CompletableFuture<Void> currentExecutionFuture =
//						// TODO: Anti-affinity constraint
//						CompletableFuture.runAsync(
//							() -> waitForExecutionToReachRunningState(executionVertex));
//					currentExecutionFuture.whenComplete(
//						(Void ignored, Throwable t) -> {
//							if (t == null) {
//								// this should aalso respect the topological order
//								LOG.info("赫明萱notifyNewVertices调用addStandbyExecution()");
//								LOG.info("赫明萱executionVertex：   "+executionVertex+"调用addStandbyExecution()");
//								executionVertex.addStandbyExecution();
//							} else {
//
//								LOG.info("Throwable {}",t);
//								schedulingFutures.add(
//									new CompletableFuture<>());
//								schedulingFutures.get(schedulingFutures.size() - 1)
//									.completeExceptionally(t);
//							}
//						});
				}
			}
		}
	}

	private void waitForExecutionToReachRunningState(ExecutionVertex executionVertex) {
		ExecutionState executionState = ExecutionState.CREATED;
		do {

			executionState = executionVertex.getExecutionState();


		} while (executionState == ExecutionState.CREATED ||
			executionState == ExecutionState.SCHEDULED ||
			executionState == ExecutionState.DEPLOYING);

		LOG.info("executionState {}",executionState);
	}

	@Override
	public String getStrategyName() {
		return "run standby task";
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the RunStandbyTaskStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		int numStandbyTasksToMaintain;
		int coordinatorBackoffMultiplier;
		long coordinatorBackoffBaseMs;

		public Factory(int numStandbyTasksToMaintain) {
			this(numStandbyTasksToMaintain, 3, 10000L);
		}

		public Factory(int numStandbyTasksToMaintain, int coordinatorBackoffMultiplier,
					   long coordinatorBackoffBaseMs) {
			this.numStandbyTasksToMaintain = numStandbyTasksToMaintain;
			this.coordinatorBackoffMultiplier = coordinatorBackoffMultiplier;
			this.coordinatorBackoffBaseMs = coordinatorBackoffBaseMs;
		}

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RunStandbyTaskStrategy(executionGraph, numStandbyTasksToMaintain, coordinatorBackoffMultiplier,
				coordinatorBackoffBaseMs);
		}
	}
}
