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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.reConfig.message.ReConfigSignal;

import java.io.IOException;

/**
 * The CheckpointBarrierHandler reacts to checkpoint barrier arriving from the input channels.
 * Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */
@Internal
public interface CheckpointBarrierHandler {

	/**
	 * Returns the next {@link BufferOrEvent} that the operator may consume.
	 * This call blocks until the next BufferOrEvent is available, or until the stream
	 * has been determined to be finished.
	 *
	 * @return The next BufferOrEvent, or {@code null}, if the stream is finished.
	 *
	 * @throws IOException Thrown if the network or local disk I/O fails.
	 *
	 * @throws InterruptedException Thrown if the thread is interrupted while blocking during
	 *                              waiting for the next BufferOrEvent to become available.
	 * @throws Exception Thrown in case that a checkpoint fails that is started as the result of receiving
	 *                   the last checkpoint barrier
	 */
	BufferOrEvent getNextNonBlocked() throws Exception;

	/**
	 * Registers the task be notified once all checkpoint barriers have been received for a checkpoint.
	 *
	 * @param task The task to notify
	 */
	void registerCheckpointEventHandler(AbstractInvokable task);

	/**
	 * Cleans up all internally held resources.
	 *
	 * @throws IOException Thrown if the cleanup of I/O resources failed.
	 */
	void cleanup() throws IOException;

	/**
	 * Checks if the barrier handler has buffered any data internally.
	 * @return {@code True}, if no data is buffered internally, {@code false} otherwise.
	 */
	boolean isEmpty();

	/**
	 * Gets the time that the latest alignment took, in nanoseconds.
	 * If there is currently an alignment in progress, it will return the time spent in the
	 * current alignment so far.
	 *
	 * @return The duration in nanoseconds
	 */
	long getAlignmentDurationNanos();

	void ignoreCheckpoint(long checkpointID) throws IOException;

    void unblockChannelIfBlocked(int absoluteChannelIndex);
	public abstract void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws IOException;

	public abstract void processEndOfPartition() throws IOException;

	public abstract long getLatestCheckpointId();

	public long getAlignmentDurationNanos() {
		if (isDuringAlignment()) {
			return System.nanoTime() - startOfAlignmentTimestamp;
		} else {
			return FutureUtils.getOrDefault(latestAlignmentDurationNanos, 0L);
		}
	}

	public long getCheckpointStartDelayNanos() {
		return latestCheckpointStartDelayNanos;
	}

	public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
		return CompletableFuture.completedFuture(null);
	}

	protected void notifyCheckpoint(CheckpointBarrier checkpointBarrier) throws IOException {
		CheckpointMetaData checkpointMetaData =
			new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());

		CheckpointMetricsBuilder checkpointMetrics = new CheckpointMetricsBuilder()
			.setAlignmentDurationNanos(latestAlignmentDurationNanos)
			.setBytesProcessedDuringAlignment(latestBytesProcessedDuringAlignment)
			.setCheckpointStartDelayNanos(latestCheckpointStartDelayNanos);

		toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
			checkpointMetaData,
			checkpointBarrier.getCheckpointOptions(),
			checkpointMetrics);
	}

	protected void notifyAbortOnCancellationBarrier(long checkpointId) throws IOException {
		notifyAbort(checkpointId,
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
	}

	protected void notifyAbort(long checkpointId, CheckpointException cause) throws IOException {
		resetAlignment();
		toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
	}

	protected void markAlignmentStartAndEnd(long checkpointCreationTimestamp) {
		markAlignmentStart(checkpointCreationTimestamp);
		markAlignmentEnd(0);
	}

	protected void markAlignmentStart(long checkpointCreationTimestamp) {
		latestCheckpointStartDelayNanos = 1_000_000 * Math.max(
			0,
			System.currentTimeMillis() - checkpointCreationTimestamp);

		resetAlignment();
		startOfAlignmentTimestamp = System.nanoTime();
	}

	protected void markAlignmentEnd() {
		markAlignmentEnd(System.nanoTime() - startOfAlignmentTimestamp);
	}

	protected void markAlignmentEnd(long alignmentDuration) {
		latestAlignmentDurationNanos.complete(alignmentDuration);
		latestBytesProcessedDuringAlignment.complete(bytesProcessedDuringAlignment);

		startOfAlignmentTimestamp = OUTSIDE_OF_ALIGNMENT;
		bytesProcessedDuringAlignment = 0;
	}

	private void resetAlignment() {
		markAlignmentEnd(0);
		latestAlignmentDurationNanos = new CompletableFuture<>();
		latestBytesProcessedDuringAlignment = new CompletableFuture<>();
	}

	protected abstract boolean isCheckpointPending();

	public void addProcessedBytes(int bytes) {
		if (isDuringAlignment()) {
			bytesProcessedDuringAlignment += bytes;
		}
	}

	private boolean isDuringAlignment() {
		return startOfAlignmentTimestamp > OUTSIDE_OF_ALIGNMENT;
	}

	public void block(InputChannelInfo channelInfo) {
	}

	public void block(long barrierId){
		throw new UnsupportedOperationException();
	}

	public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
		throw new UnsupportedOperationException();
	}

	public abstract void processReConfigBarrier(ReConfigSignal receivedBarrier, InputChannelInfo channelInfo) throws IOException;
}
