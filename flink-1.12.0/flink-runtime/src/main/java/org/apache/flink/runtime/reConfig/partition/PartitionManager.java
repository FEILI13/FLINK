package org.apache.flink.runtime.reConfig.partition;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.reConfig.message.MigrateSignal;
import org.apache.flink.runtime.reConfig.message.ReConfigSignal;

/**
 * @Title: PartitionManager
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig.partition
 * @Date 2024/2/28 21:29
 * @description: 分区管理器
 */
public class PartitionManager {

	private ExecutionVertex[] tasksToTrigger;
	private ExecutionVertex[] tasksToWaitFor;
	private ExecutionVertex[] tasksToCommitTo;
	private ExecutionVertex[] upStreamExecutionVertex;

	public PartitionManager(
		ExecutionVertex[] tasksToTrigger,
		ExecutionVertex[] tasksToWaitFor,
		ExecutionVertex[] tasksToCommitTo,
		ExecutionVertex[] upStreamExecutionVertex) {
		this.tasksToTrigger = tasksToTrigger;
		this.tasksToWaitFor = tasksToWaitFor;
		this.tasksToCommitTo = tasksToCommitTo;
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

	/**
	 * 状态迁移
	 * @param sourceTask	源task index
	 * @param targetTask	目标task index
	 * @param keyGroupIndex	键组索引
	 * @param batch			批次号
	 * @param splitNum		总批次
	 * @param version		时间戳版本
	 */
	public void migrte(int sourceTask, int targetTask, int keyGroupIndex, int batch, int splitNum, long version) {
//		MigrateSignal signal = new MigrateSignal(sourceTaskIndex, targetTaskIndex, keyGroupIndex, batch, splitNum, version);
//		sendSignal(signal);
		ReConfigSignal signal = new ReConfigSignal(ReConfigSignal.ReConfigSignalType.MIGRATE, version);
		signal.putProperties("sourceTask", sourceTask);
		signal.putProperties("targetTask", targetTask);
		signal.putProperties("keyGroupIndex", keyGroupIndex);
		signal.putProperties("batch", batch);
		signal.putProperties("splitNum", splitNum);
		sendSignal(signal);
	}

	public void migrte(MigratePlanEntry entry, int batch, long version) {
		migrte(
			entry.getSourceTask(),
			entry.getTargetTask(),
			entry.getKeyGroupIndex(),
			batch,
			entry.getSplitNum(),
			version
		);
	}

	public void clean(long version) {
		ReConfigSignal signal = new ReConfigSignal(ReConfigSignal.ReConfigSignalType.CLEAN, version);
		sendSignal(signal);
	}

//	public void startAction() {
//		ScheduledExecutorServiceAdapter partitioner = new ScheduledExecutorServiceAdapter(Executors.newSingleThreadScheduledExecutor(
//			new DispatcherThreadFactory(Thread.currentThread().getThreadGroup(), "partitioner")
//		));
//		partitioner.scheduleAtFixedRate()
//
//	}
}
