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

import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.runtime.causal.services.BufferOrderService;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A wrapper around a {@link CheckpointBarrierHandler} which uses a {@link CausalBufferOrderService} to ensure
 * correct ordering of buffers.
 * <p>
 * This wrapper is implemented at this component and not the
 * {@link org.apache.flink.runtime.io.network.partition.consumer.InputGate} since the barriers serve as
 * synchronization points.
 * Thus, there is no need to record them in the CausalLog.
 */
public class CausalBufferHandler extends CheckpointBarrierHandler {
	private static final Logger LOG = LoggerFactory.getLogger(CausalBufferHandler.class);

	private final Object lock;

	private  CheckpointBarrierHandler wrapped;
	private  BufferOrderService bufferOrderService;

	public CausalBufferHandler(
		JobCausalLog causalLog,
		IRecoveryManager recoveryManager,
		CheckpointBarrierHandler wrapped,
		int numInputChannels,
		Object checkpointLock,
		AbstractInvokable abstractInvokable) {
		super(abstractInvokable);
		this.wrapped = wrapped;
		this.lock = checkpointLock;
		this.bufferOrderService = new CausalBufferOrderService(causalLog, recoveryManager, wrapped,
			numInputChannels);
	}


	@Override
	public BufferOrEvent getNextNonBlocked(BufferOrEvent inputGate) {

		System.out.println("CausalBufferHandler");


		//We lock to guarantee that async events don't try to write to the causal log at the same time as the
		// order service.
		synchronized (lock) {
			return bufferOrderService.getNextBuffer(inputGate);
		}
	}


	@Override
	public long getAlignmentDurationNanos() {
		return wrapped.getAlignmentDurationNanos();
	}

	@Override
	protected boolean isCheckpointPending() {
		return wrapped.isCheckpointPending();
	}

	@Override
	public void processBarrier(
		CheckpointBarrier receivedBarrier,
		InputChannelInfo channelInfo) throws IOException {
		wrapped.processBarrier(receivedBarrier,channelInfo);
	}

	@Override
	public void processBarrierAnnouncement(
		CheckpointBarrier announcedBarrier,
		int sequenceNumber,
		InputChannelInfo channelInfo) throws IOException {

		wrapped.processBarrierAnnouncement(announcedBarrier,sequenceNumber,channelInfo);

	}

	@Override
	public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws IOException {
		wrapped.processCancellationBarrier(cancelBarrier);
	}

	@Override
	public void processEndOfPartition() throws IOException {
		wrapped.processEndOfPartition();
	}

	@Override
	public long getLatestCheckpointId() {
		return wrapped.getLatestCheckpointId();
	}

	@Override
	public void ignoreCheckpoint(long checkpointID) throws IOException {
		wrapped.ignoreCheckpoint(checkpointID);
	}

	public CheckpointBarrierHandler getWrapped(){
		return wrapped;
	}

	public int getTotalNumberOfInputChannels(){
		return wrapped.getTotalNumberOfInputChannels();
	}

}
