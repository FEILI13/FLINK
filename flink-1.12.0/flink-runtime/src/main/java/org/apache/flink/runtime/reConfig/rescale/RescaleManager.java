package org.apache.flink.runtime.reConfig.rescale;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.reConfig.message.ReConfigSignal;
import org.apache.flink.runtime.reConfig.partition.PartitionManager;
import org.apache.flink.runtime.scheduler.SchedulerNG;

import java.util.concurrent.CompletableFuture;

/**
 * @Title: RescaleManager
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.rescale
 * @Date 2024/3/21 19:04
 * @description: rescale管理器
 */
public class RescaleManager {
	private PartitionManager partitionManager;
	private ExecutionVertex[] tasksToTrigger;
	private ExecutionVertex[] tasksToWaitFor;
	private ExecutionVertex[] tasksToCommitTo;
	private ExecutionVertex[] upStreamExecutionVertex;
	private SchedulerNG schedulerNG;

	public RescaleManager(PartitionManager partitionManager,
						  ExecutionVertex[] tasksToTrigger,
						  ExecutionVertex[] tasksToWaitFor,
						  ExecutionVertex[] tasksToCommitTo,
						  ExecutionVertex[] upStreamExecutionVertex){
		this.partitionManager = partitionManager;
		this.tasksToTrigger = tasksToTrigger;
		this.tasksToCommitTo = tasksToCommitTo;
		this.tasksToWaitFor = tasksToWaitFor;
		this.upStreamExecutionVertex = upStreamExecutionVertex;
	}

	public void sendSignal(ReConfigSignal signal){
		try {
			final Execution[] executions = getTriggerExecutions();
			for(Execution execution:executions){
				execution.triggerReConfig(signal);
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	private Execution[] getTriggerExecutions() throws CheckpointException {
		Execution[] executions = new Execution[tasksToTrigger.length];
		for(int i=0;i<tasksToTrigger.length;++i){
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if(ee == null){
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}else if(ee.getState() == ExecutionState.RUNNING){
				executions[i] = ee;
			}else{
				throw new CheckpointException(
					CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
		}
		return executions;
	}

	public void rescalse(String taskName, int newParallelism){
		System.out.println("rescale");
		schedulerNG.changeTopo(taskName, newParallelism);
	}


	public void setSchedulerNG(SchedulerNG schedulerNG) {
		this.schedulerNG = schedulerNG;
	}


	public void ackRescale(ReConfigSignal signal) {
		System.out.println(signal);
	}
}
