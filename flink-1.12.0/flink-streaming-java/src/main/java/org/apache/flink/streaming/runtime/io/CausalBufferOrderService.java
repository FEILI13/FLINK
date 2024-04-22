package org.apache.flink.streaming.runtime.io;


import org.apache.flink.runtime.causal.determinant.OrderDeterminant;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.runtime.causal.services.AbstractCausalService;
import org.apache.flink.runtime.causal.services.BufferOrderService;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.Queue;

public class CausalBufferOrderService  extends AbstractCausalService implements BufferOrderService {


		private static final Logger LOG = LoggerFactory.getLogger(CausalBufferOrderService.class);

		// The place from where we pull buffers in order to order them.
		public  CheckpointBarrierHandler bufferSource;

		// We use this to buffer buffers from the incorrect channels in order to deliver them in correct order
		private final Queue<BufferOrEvent>[] bufferedBuffersPerChannel;

		// The determinant object we reuse to avoid object creation and thus large GC
		private final OrderDeterminant reuseOrderDeterminant;

		// The number of input channels this task has. Important for buffering and exception cases such as = 1
		private final int numInputChannels;

		// If there are any buffered buffers, we need to check the queues
		private int numBufferedBuffers;

	public CausalBufferOrderService(JobCausalLog jobCausalLog, IRecoveryManager recoveryManager,
		CheckpointBarrierHandler bufferSource,
		int numInputChannels) {
		super(jobCausalLog, recoveryManager);
		this.bufferSource = bufferSource;
		this.bufferedBuffersPerChannel = new ArrayDeque[numInputChannels];
		for (int i = 0; i < numInputChannels; i++)
			bufferedBuffersPerChannel[i] = new ArrayDeque<>(100);
		this.numInputChannels = numInputChannels;
		this.numBufferedBuffers = 0;
		this.reuseOrderDeterminant = new OrderDeterminant();
	}

		@Override
		public BufferOrEvent getNextBuffer(BufferOrEvent inputGate)  {


		BufferOrEvent toReturn;
		if(LOG.isDebugEnabled())
			LOG.debug("Request next buffer");
		//Simple case, when there is only one channel we do not need to store order determinants, nor
		// do any special replay logic, because everything is deterministic.
		if (numInputChannels == 1) {
			System.out.println("numInputChannels == 1");
			return getNewBuffer(inputGate);
		}


		if (isRecovering()) {
			if(LOG.isDebugEnabled())
				LOG.debug("Get replayed buffer");
			toReturn = getNextNonBlockedReplayed(inputGate);
		} else {
			if(LOG.isDebugEnabled())
				LOG.debug("Get new buffer");
			if(numBufferedBuffers != 0) {
				if(LOG.isDebugEnabled())
					LOG.debug("Get buffered buffer");
				System.out.println("toReturn = pickBufferedUnprocessedBuffer();");
				toReturn = pickBufferedUnprocessedBuffer();
			} else {
				if(LOG.isDebugEnabled())
					LOG.debug("Get actual new buffer");
				System.out.println("toReturn = getNewBuffer();");
				toReturn = getNewBuffer(inputGate);
			}
		}

		if (toReturn != null){
			LOG.info("做元组的因果日志："+toReturn.getChannelInfo().getInputChannelIdx());
			threadCausalLog.appendDeterminant(reuseOrderDeterminant.replace((byte) toReturn.getChannelInfo().getInputChannelIdx()),
				epochTracker.getCurrentEpoch());
		}


		return toReturn;
	}

		private BufferOrEvent getNextNonBlockedReplayed(BufferOrEvent inputGate)  {
			BufferOrEvent toReturn;
			byte channel = recoveryManager.getLogReplayer().replayNextChannel();
			LOG.debug("Determinant says next channel is {}!", channel);
			if (bufferedBuffersPerChannel[channel].isEmpty()) {
				toReturn = processUntilFindBufferForChannel(channel,inputGate);
			} else {
				toReturn = bufferedBuffersPerChannel[channel].remove();
				numBufferedBuffers--;
			}
			return toReturn;
		}

		private BufferOrEvent pickBufferedUnprocessedBuffer() {
		//todo improve runtime complexity
		for (Queue<BufferOrEvent> queue : bufferedBuffersPerChannel) {
			if (!queue.isEmpty()) {
				numBufferedBuffers--;
				return queue.remove();
			}
		}
		return null;//unrecheable
	}

		private BufferOrEvent processUntilFindBufferForChannel(byte channel,BufferOrEvent inputGate) {
		LOG.debug("Found no buffered buffers for channel {}. Processing buffers until I find one", channel);
		while (true) {
			BufferOrEvent newBufferOrEvent = getNewBuffer(inputGate);
			if(newBufferOrEvent == null)
				continue;
			//If this was a BoE for the channel we were looking for, return with it
			if (newBufferOrEvent.getChannelInfo().getInputChannelIdx() == channel) {
				LOG.debug("It is from the expected channel, returning");
				return newBufferOrEvent;
			}

			LOG.debug("It is not from the expected channel,  buffering and continuing");
			//Otherwise, append it to the correct queue and try again
			bufferedBuffersPerChannel[newBufferOrEvent.getChannelInfo().getInputChannelIdx()].add(newBufferOrEvent);
			numBufferedBuffers++;
		}
	}

		private BufferOrEvent getNewBuffer(BufferOrEvent inputGate) {


//			if(inputGate == null)
//				return null;
//			LOG.debug("Got a new buffer from channel {}", inputGate.getChannelInfo().getInputChannelIdx());
//			if (inputGate.isEvent() && inputGate.getEvent().getClass() == DeterminantRequestEvent.class) {
//				LOG.debug("Buffer is DeterminantRequest, sending notification");
//				recoveryManager.notifyDeterminantRequestEvent((DeterminantRequestEvent) inputGate.getEvent(),
//					inputGate.getChannelInfo().getInputChannelIdx());
//
//				return null;
//			}
//
//			return inputGate;

			BufferOrEvent newBufferOrEvent;
			while (true) {
				System.out.println("newBufferOrEvent = bufferSource.getNextNonBlocked();");
				System.out.println(bufferSource.getClass());
				try {
					newBufferOrEvent = bufferSource.getNextNonBlocked(inputGate);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				if(newBufferOrEvent == null)
					return null;
				LOG.debug("Got a new buffer from channel {}", newBufferOrEvent.getChannelInfo().getInputChannelIdx());
				if (newBufferOrEvent.isEvent() && newBufferOrEvent.getEvent().getClass() == DeterminantRequestEvent.class) {
					LOG.debug("Buffer is DeterminantRequest, sending notification");
					recoveryManager.notifyDeterminantRequestEvent((DeterminantRequestEvent) newBufferOrEvent.getEvent(),
						newBufferOrEvent.getChannelInfo().getInputChannelIdx());
					continue;
				}
				break;
			}
		return newBufferOrEvent;
	}

	public CheckpointBarrierHandler getCheckpointBarrierHandler(){
		return bufferSource;
	}
}
