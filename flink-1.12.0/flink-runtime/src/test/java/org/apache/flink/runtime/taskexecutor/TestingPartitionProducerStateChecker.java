/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.function.TriFunction;

import java.util.concurrent.CompletableFuture;

public class TestingPartitionProducerStateChecker implements PartitionProducerStateChecker {
	private final TriFunction<JobID, IntermediateDataSetID, ResultPartitionID, CompletableFuture<ExecutionState>> partitionProducerStateFunction;

	private TestingPartitionProducerStateChecker(TriFunction<JobID, IntermediateDataSetID, ResultPartitionID, CompletableFuture<ExecutionState>> partitionProducerStateFunction) {
		this.partitionProducerStateFunction = partitionProducerStateFunction;
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionProducerState(JobID jobId, IntermediateDataSetID intermediateDataSetId, ResultPartitionID resultPartitionId) {
		return partitionProducerStateFunction.apply(jobId, intermediateDataSetId, resultPartitionId);
	}

	@Override
	public CompletableFuture<Acknowledge> triggerFailProducer(
		IntermediateDataSetID intermediateDataSetId,
		ResultPartitionID resultPartitionId,
		Throwable cause) {
		return null;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static final class Builder {

		private TriFunction<JobID, IntermediateDataSetID, ResultPartitionID, CompletableFuture<ExecutionState>> partitionProducerStateFunction = (ignoredA, ignoredB, ignoredC) -> new CompletableFuture<>();

		private Builder() {}

		public Builder setPartitionProducerStateFunction(TriFunction<JobID, IntermediateDataSetID, ResultPartitionID, CompletableFuture<ExecutionState>> partitionProducerStateFunction) {
			this.partitionProducerStateFunction = partitionProducerStateFunction;
			return this;
		}

		public TestingPartitionProducerStateChecker build() {
			return new TestingPartitionProducerStateChecker(partitionProducerStateFunction);
		}
	}
}
