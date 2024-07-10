package org.apache.flink.runtime.reConfig.rescale;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.reConfig.message.ReConfigSignal;
import org.apache.flink.runtime.reConfig.partition.PartitionManager;
import org.apache.flink.runtime.scheduler.SchedulerNG;

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

	private Execution[] getUpStreamExecutions() throws CheckpointException {
		Execution[] executions = new Execution[upStreamExecutionVertex.length];
		for(int i=0;i<upStreamExecutionVertex.length;++i){
			Execution ee = upStreamExecutionVertex[i].getCurrentExecutionAttempt();
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

	public void rescale(String taskName, int newParallelism){
		long beforeTime = System.currentTimeMillis();
		schedulerNG.changeTopo(taskName, newParallelism);
		schedulerNG.rescaleClean();
		long cost = System.currentTimeMillis() - beforeTime;
		System.out.println("&&&&cost:"+cost);

	}

	private void notifyUpdatePartitonStrategy(){
		try {
			final Execution[] executions = getUpStreamExecutions();;
			System.out.println("executions.length:"+executions.length);
			for (Execution execution : executions) {
				System.out.println("execution:"+execution);
				execution.triggerUpdatePartitionStrategy();
			}
			Thread.sleep(10000);
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public void setSchedulerNG(SchedulerNG schedulerNG) {
		this.schedulerNG = schedulerNG;
	}


	public void ackRescale(ReConfigSignal signal) {
		System.out.println(signal);
	}
}
