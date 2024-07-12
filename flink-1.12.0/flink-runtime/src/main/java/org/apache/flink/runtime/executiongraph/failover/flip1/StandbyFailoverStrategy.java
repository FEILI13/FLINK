package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.RunStandbyTaskStrategy;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class StandbyFailoverStrategy implements FailoverStrategy{

	private Logger log = LoggerFactory.getLogger(StandbyFailoverStrategy.class);


	private final RunStandbyTaskStrategy runStandbyTaskStrategy;

	private final SchedulingTopology topology;

	public StandbyFailoverStrategy(final SchedulingTopology topology) {

		final int numStandbyTasksToMaintain = JobManagerOptions.NUMBER_OF_STANDBY_TASKS_TO_MAINTAIN.defaultValue();
		final int checkpointCoordinatorBackoffMultiplier = JobManagerOptions.CC_BACKOFF_MULT.defaultValue();
		final long checkpointCoordinatorBackoffBase = JobManagerOptions.CC_BACKOFF_BASE.defaultValue();
		log.info("numStandbyTasksToMaintain {}",numStandbyTasksToMaintain);
		this.runStandbyTaskStrategy = new RunStandbyTaskStrategy(topology.getExecutionGraph(),
			numStandbyTasksToMaintain,
			checkpointCoordinatorBackoffMultiplier,
			checkpointCoordinatorBackoffBase);
		this.topology = checkNotNull(topology);


	}


	public RunStandbyTaskStrategy getRunStandbyTaskStrategy(){
		return runStandbyTaskStrategy;
	}

	@Override
	public Set<ExecutionVertexID> getTasksNeedingRestart(
		ExecutionVertexID executionVertexId,
		Throwable cause) {

		Set<ExecutionVertexID> tasksToRestart = new HashSet<>();
		tasksToRestart.add(executionVertexId);
		return tasksToRestart;
	}



	/**
	 * The factory to instantiate {@link StandbyFailoverStrategy}.
	 */
	public static class Factory  implements FailoverStrategy.Factory{



		@Override
		public FailoverStrategy create(
			SchedulingTopology topology,
			ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {
			return new StandbyFailoverStrategy(topology);
		}
	}
}
