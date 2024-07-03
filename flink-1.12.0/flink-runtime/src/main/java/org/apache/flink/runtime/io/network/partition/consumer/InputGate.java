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

package org.apache.flink.runtime.io.network.partition.consumer;

<<<<<<< HEAD
import org.apache.flink.api.common.JobID;
=======
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
>>>>>>> 42e2f9f6d6d280525727b00f05dfb935340a61a0
import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;
import java.util.Optional;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask. As shown in the Figure, each reduce task
 * will have an input gate attached to it. This will provide its input, which will consist of one
 * subpartition from each partition of the intermediate result.
 */
public interface InputGate {

	int getNumberOfInputChannels();

	String getOwningTaskName();

	boolean isFinished();

	void requestPartitions() throws IOException, InterruptedException;

	/**
	 * Blocking call waiting for next {@link BufferOrEvent}.
	 *
	 * @return {@code Optional.empty()} if {@link #isFinished()} returns true.
	 */
	Optional<BufferOrEvent> getNextBufferOrEvent() throws IOException, InterruptedException;

	/**
	 * Poll the {@link BufferOrEvent}.
	 *
	 * @return {@code Optional.empty()} if there is no data to return or if {@link #isFinished()} returns true.
	 */
	Optional<BufferOrEvent> pollNextBufferOrEvent() throws IOException, InterruptedException;

	void sendTaskEvent(TaskEvent event) throws IOException, InterruptedException;

	void registerListener(InputGateListener listener);

	int getPageSize();

	InputChannel getInputChannel(int i);

    int getAbsoluteChannelIndex(InputGate gate, int channelIndex);

	SingleInputGate[] getInputGates();

<<<<<<< HEAD
    JobID getJobID();
=======
	public void modifyForRescale(InputGateDeploymentDescriptor inputGateDeploymentDescriptor){
		throw new UnsupportedOperationException("modifyForRescale unsupported for " + this.getClass());
	}

	public void setChannelStateWriterForNewChannels(ChannelStateWriter channelStateWriter, int previousNumChannels) {
		for (int index = previousNumChannels, numChannels = getNumberOfInputChannels(); index < numChannels; index++) {
			final InputChannel channel = getChannel(index);
			if (channel instanceof ChannelStateHolder) {
				((ChannelStateHolder) channel).setChannelStateWriter(channelStateWriter);
			}
		}
	}

	public void requestPartitionsForRescale(int previousNumChannels) {
		throw new UnsupportedOperationException("modifyForRescale unsupported for " + this.getClass());
	}

	/**
	 * Simple pojo for INPUT, DATA and moreAvailable.
	 */
	protected static class InputWithData<INPUT, DATA> {
		protected final INPUT input;
		protected final DATA data;
		protected final boolean moreAvailable;
		protected final boolean morePriorityEvents;

		InputWithData(INPUT input, DATA data, boolean moreAvailable, boolean morePriorityEvents) {
			this.input = checkNotNull(input);
			this.data = checkNotNull(data);
			this.moreAvailable = moreAvailable;
			this.morePriorityEvents = morePriorityEvents;
		}

		@Override
		public String toString() {
			return "InputWithData{" +
				"input=" + input +
				", data=" + data +
				", moreAvailable=" + moreAvailable +
				", morePriorityEvents=" + morePriorityEvents +
				'}';
		}
	}

	/**
	 * Setup gate, potentially heavy-weight, blocking operation comparing to just creation.
	 */
	public abstract void setup() throws IOException;

	public abstract void requestPartitions() throws IOException;

	public abstract CompletableFuture<Void> getStateConsumedFuture();

	public abstract void finishReadRecoveredState() throws IOException;
>>>>>>> 42e2f9f6d6d280525727b00f05dfb935340a61a0
}
