package org.apache.flink.runtime.reConfig;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.reConfig.message.ReConfigSignal;
import org.apache.flink.runtime.reConfig.utils.InstanceState;
import org.apache.flink.runtime.reConfig.message.ReConfigStage;
import org.apache.flink.util.Preconditions;

/**
 * @Title: RpcReConfigResponder
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig
 * @Date 2024/2/29 19:57
 * @description:
 */
public class RpcReConfigResponder {

	private final JobMasterGateway jobMasterGateway;

	private String taskName;

	public RpcReConfigResponder(JobMasterGateway jobMasterGateway) {
		this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
	}

	public void acknowledgeReConfig(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		String taskName,
		ReConfigStage stage) {

		jobMasterGateway.acknowledgeReConfig(
			jobID,
			executionAttemptID,
			taskName,
			stage);
	}

	public void acknowledgeReConfig(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		String taskName,
		InstanceState state) {

		jobMasterGateway.acknowledgeReConfig(
			jobID,
			executionAttemptID,
			taskName,
			state);
	}

	public void acknowledgeReConfig(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		String taskName,
		ReConfigSignal signal) {

		jobMasterGateway.acknowledgeReConfig(
			jobID,
			executionAttemptID,
			taskName,
			signal);
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}
}
