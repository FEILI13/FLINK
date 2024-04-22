/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.services.TimeService;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.causal.EpochTracker;
import org.apache.flink.runtime.causal.ProcessingTimeForceable;
import org.apache.flink.runtime.causal.determinant.ProcessingTimeCallbackID;
import org.apache.flink.runtime.causal.determinant.TimerTriggerDeterminant;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLog;
import org.apache.flink.runtime.causal.recovery.RecoveryManager;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TimerService} which assigns as current processing time the result of calling
 * {@link System#currentTimeMillis()} and registers timers using a {@link ScheduledThreadPoolExecutor}.
 */
@Internal
public class SystemProcessingTimeService implements TimerService, ProcessingTimeForceable {

	private static final Logger LOG = LoggerFactory.getLogger(SystemProcessingTimeService.class);

	private static final int STATUS_ALIVE = 0;
	private static final int STATUS_QUIESCED = 1;
	private static final int STATUS_SHUTDOWN = 2;

	// ------------------------------------------------------------------------

	/** The executor service that schedules and calls the triggers of this task. */
	private final ScheduledThreadPoolExecutor timerService;

	private final ExceptionHandler exceptionHandler;
	private final AtomicInteger status;

	private final CompletableFuture<Void> quiesceCompletedFuture;

	private final TimeService timeService;
	private final EpochTracker epochTracker;
	private final ThreadCausalLog mainThreadCausalLog;
	private final RecoveryManager recoveryManager;

	private final TimerTriggerDeterminant reuseTimerTriggerDeterminant;

	private final Map<ProcessingTimeCallbackID, PreregisteredTimer> preregisteredTimerTasks;

	private final Map<ProcessingTimeCallbackID, ProcessingTimeCallback> callbacks;

	/**
	 * The lock that timers acquire upon triggering.
	 */
	private final Object checkpointLock;

	@VisibleForTesting
	SystemProcessingTimeService(ExceptionHandler exceptionHandler) {
		this(exceptionHandler, null,null,null,null,null,null);
	}

	SystemProcessingTimeService(ExceptionHandler exceptionHandler, ThreadFactory threadFactory,
								TimeService timeService,
								EpochTracker epochTracker,
								JobCausalLog causalLog, RecoveryManager recoveryManager,
								 Object checkpointLock) {

		this.exceptionHandler = checkNotNull(exceptionHandler);
		this.status = new AtomicInteger(STATUS_ALIVE);
		this.quiesceCompletedFuture = new CompletableFuture<>();
		this.checkpointLock = checkNotNull(checkpointLock);
		this.timeService = timeService;

		this.epochTracker = epochTracker;
		this.mainThreadCausalLog =
			causalLog.getThreadCausalLog(new CausalLogID(recoveryManager.getContext().getTaskVertexID()));
		this.recoveryManager = recoveryManager;

		this.preregisteredTimerTasks = new HashMap<>();
		this.callbacks = new HashMap<>();
		this.reuseTimerTriggerDeterminant = new TimerTriggerDeterminant();

		if (threadFactory == null) {
			this.timerService = new ScheduledTaskExecutor(1);
		} else {
			this.timerService = new ScheduledTaskExecutor(1, threadFactory);
		}

		// tasks should be removed if the future is canceled
		this.timerService.setRemoveOnCancelPolicy(true);

		// make sure shutdown removes all pending tasks
		this.timerService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.timerService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
	}

	@Override
//	public long getCurrentProcessingTime() {
//		return System.currentTimeMillis();
//	}

	public long getCurrentProcessingTime() {
		return timeService.currentTimeMillis();
	}


	public long getCurrentProcessingTimeCausal() {
		return timeService.currentTimeMillis();
	}

	/**
	 * Registers a task to be executed no sooner than time {@code timestamp}, but without strong
	 * guarantees of order.
	 *
	 * @param timestamp Time when the task is to be enabled (in processing time)
	 * @param callback    The task to be executed
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 */
	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {

		long delay = ProcessingTimeServiceUtil.getProcessingTimeDelay(timestamp, getCurrentProcessingTime());

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {

			ScheduledFuture<?> future;
			ScheduledTask toRegister = new ScheduledTask(status,
				exceptionHandler,
				callback,
				timestamp,
				0,
				checkpointLock,
				mainThreadCausalLog,
				epochTracker,
				reuseTimerTriggerDeterminant);

			if (recoveryManager.isRecovering())
				future = registerTimerRecovering(toRegister, delay);
			else
				future = registerTimerRunning(toRegister, delay);

			return future;
		}
		catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(delay);
			}
			else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			}
			else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		return scheduleRepeatedly(callback, initialDelay, period, false);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(ProcessingTimeCallback callback, long initialDelay, long period) {
		return scheduleRepeatedly(callback, initialDelay, period, true);
	}

	private ScheduledFuture<?> scheduleRepeatedly(ProcessingTimeCallback callback, long initialDelay, long period, boolean fixedDelay) {
		final long nextTimestamp = getCurrentProcessingTime() + initialDelay;
		final ScheduledTask task = new ScheduledTask(status,
			exceptionHandler,
			callback,
			nextTimestamp,
			period,
			checkpointLock,
			mainThreadCausalLog,
			epochTracker,
			reuseTimerTriggerDeterminant);

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {

			ScheduledFuture<?> future;
			if (recoveryManager.isRecovering())
				future = registerAtFixedRateRecovering(initialDelay, task);
			else
				future = registerAtFixedRateRunning(initialDelay, period, task,fixedDelay);

			return future;

//			return fixedDelay
//					? timerService.scheduleWithFixedDelay(task, initialDelay, period, TimeUnit.MILLISECONDS)
//					: timerService.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(initialDelay);
			}
			else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			}
			else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	/**
	 * @return {@code true} is the status of the service
	 * is {@link #STATUS_ALIVE}, {@code false} otherwise.
	 */
	@VisibleForTesting
	boolean isAlive() {
		return status.get() == STATUS_ALIVE;
	}

	@Override
	public boolean isTerminated() {
		return status.get() == STATUS_SHUTDOWN;
	}

	@Override
	public CompletableFuture<Void> quiesce() {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_QUIESCED)) {
			timerService.shutdown();
		}

		return quiesceCompletedFuture;
	}

	@Override
	public void shutdownService() {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_SHUTDOWN) ||
				status.compareAndSet(STATUS_QUIESCED, STATUS_SHUTDOWN)) {
			timerService.shutdownNow();
		}
	}

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does wait
	 * for all timers to complete or until the time limit is exceeded. Any call to
	 * {@link #registerTimer(long, ProcessingTimeCallback)} will result in a hard exception after calling this method.
	 * @param time time to wait for termination.
	 * @param timeUnit time unit of parameter time.
	 * @return {@code true} if this timer service and all pending timers are terminated and
	 *         {@code false} if the timeout elapsed before this happened.
	 */
	@VisibleForTesting
	boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException {
		shutdownService();
		return timerService.awaitTermination(time, timeUnit);
	}

	@Override
	public boolean shutdownServiceUninterruptible(long timeoutMs) {

		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(timeoutMs));

		boolean shutdownComplete = false;
		boolean receivedInterrupt = false;

		do {
			try {
				// wait for a reasonable time for all pending timer threads to finish
				shutdownComplete = shutdownAndAwaitPending(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException iex) {
				receivedInterrupt = true;
				LOG.trace("Intercepted attempt to interrupt timer service shutdown.", iex);
			}
		} while (deadline.hasTimeLeft() && !shutdownComplete);

		if (receivedInterrupt) {
			Thread.currentThread().interrupt();
		}

		return shutdownComplete;
	}

	// safety net to destroy the thread pool
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		timerService.shutdownNow();
	}

	@VisibleForTesting
	int getNumTasksScheduled() {
		BlockingQueue<?> queue = timerService.getQueue();
		if (queue == null) {
			return 0;
		} else {
			return queue.size();
		}
	}

	// ------------------------------------------------------------------------

	private class ScheduledTaskExecutor extends ScheduledThreadPoolExecutor {

		public ScheduledTaskExecutor(int corePoolSize) {
			super(corePoolSize);
		}

		public ScheduledTaskExecutor(int corePoolSize, ThreadFactory threadFactory) {
			super(corePoolSize, threadFactory);
		}

		@Override
		protected void terminated() {
			super.terminated();
			quiesceCompletedFuture.complete(null);
		}
	}

	/**
	 * An exception handler, called when {@link ProcessingTimeCallback} throws an exception.
	 */
	interface ExceptionHandler {
		void handleException(Exception ex);
	}

//	private Runnable wrapOnTimerCallback(ProcessingTimeCallback callback, long timestamp) {
//		return new ScheduledTask(status, exceptionHandler, callback, timestamp, 0,null,null,null,null);
//	}
//
//	private Runnable wrapOnTimerCallback(ProcessingTimeCallback callback, long nextTimestamp, long period) {
//		return new ScheduledTask(status, exceptionHandler, callback, nextTimestamp, period,null,null,null,null);
//	}

	private static final class ScheduledTask implements Runnable {
		private final AtomicInteger serviceStatus;
		private final ExceptionHandler exceptionHandler;
		private final ProcessingTimeCallback callback;

		private long nextTimestamp;
		private final long period;

		private final Object lock;
		private final EpochTracker epochTracker;
		private final ThreadCausalLog causalLog;
		private final TimerTriggerDeterminant timerTriggerDeterminantToUse;

		ScheduledTask(
				AtomicInteger serviceStatus,
				ExceptionHandler exceptionHandler,
				ProcessingTimeCallback callback,
				long timestamp,
				long period,
				Object lock,
				ThreadCausalLog causalLog,
				EpochTracker epochTracker,
				TimerTriggerDeterminant toUse) {
			this.serviceStatus = serviceStatus;
			this.exceptionHandler = exceptionHandler;
			this.callback = callback;
			this.nextTimestamp = timestamp;
			this.period = period;

			this.lock = Preconditions.checkNotNull(lock);
			this.causalLog = causalLog;
			this.epochTracker = epochTracker;
			this.timerTriggerDeterminantToUse = toUse;
		}

		@Override
		public void run() {
			if (serviceStatus.get() != STATUS_ALIVE) {
				return;
			}
			try {
				runTask(this.nextTimestamp);
//				callback.onProcessingTime(nextTimestamp);
			} catch (Exception ex) {
				exceptionHandler.handleException(ex);
			}
//			nextTimestamp += period;
		}

		public void runTask(long timestamp) {
			synchronized (lock) {
				try {
					if (serviceStatus.get() == STATUS_ALIVE) {

						ProcessingTimeCallbackID processingTimeCallbackID = getID();
						causalLog.appendDeterminant(
							timerTriggerDeterminantToUse.replace(
								epochTracker.getRecordCount(),
								processingTimeCallbackID,
								timestamp),
							epochTracker.getCurrentEpoch());
						callback.onProcessingTime(timestamp);
					}

					this.nextTimestamp = timestamp + period;
				} catch (Throwable t) {
					TimerException asyncException = new TimerException(t);
					exceptionHandler.handleException(asyncException);
				}
			}
		}


		public ProcessingTimeCallback getTarget() {
			return callback;
		}
	}


	static ProcessingTimeCallbackID getID(){
			return new ProcessingTimeCallbackID(ProcessingTimeCallbackID.Type.LATENCY);

	}

	/**
	 * hmx加
	 */
	private static class PreregisteredTimer {
		Runnable task;
		long delay;

		public PreregisteredTimer(Runnable task, long delay) {
			this.task = task;
			this.delay = delay;
		}

		public Runnable getTask() {
			return task;
		}

		public long getDelay() {
			return delay;
		}

	}


	private ScheduledFuture<?> registerTimerRecovering(ScheduledTask toRegister, long delay) {
		ProcessingTimeCallbackID id = getID();
		if(LOG.isDebugEnabled())
			LOG.debug("We are recovering, differing one-shot timer registration for {}!", id);
		preregisteredTimerTasks.put(id, new PreregisteredTimer(toRegister, delay));
		return new PreregisteredCompleteableFuture<>(id);
	}

	private ScheduledFuture<?> registerTimerRunning(ScheduledTask toRegister, long delay) {
		if(LOG.isDebugEnabled())
			LOG.debug("We are running, directly registering one-shot timer!");
		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return timerService.schedule(
				toRegister, delay, TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(delay);
			} else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			} else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	private ScheduledFuture<?> registerAtFixedRateRecovering(long initialDelay,
															 ScheduledTask toRegister) {
		ProcessingTimeCallbackID id = getID();
		if(LOG.isDebugEnabled())
			LOG.debug("We are recovering, differing fixed rate timer registration for {}!", id);
		preregisteredTimerTasks.put(id, new PreregisteredTimer(toRegister, initialDelay));
		return new PreregisteredCompleteableFuture<>(id);

	}

	private ScheduledFuture<?> registerAtFixedRateRunning(long initialDelay, long period,
														  ScheduledTask toRegister,
														  boolean fixedDelay) {
		if(LOG.isDebugEnabled())
			LOG.debug("We are running, directly registering fixed rate timer!");
		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return fixedDelay
				? timerService.scheduleWithFixedDelay(toRegister, initialDelay, period, TimeUnit.MILLISECONDS)
				: timerService.scheduleAtFixedRate(toRegister, initialDelay, period, TimeUnit.MILLISECONDS);
//			return timerService.scheduleAtFixedRate(
//				toRegister,
//				initialDelay,
//				period,
//				TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(initialDelay);
			} else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			} else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}


	public void registerCallback(ProcessingTimeCallback callback) {
		this.callbacks.put(getID(), callback);
	}

	public void forceExecution(ProcessingTimeCallbackID id, long timestamp) {

		PreregisteredTimer timerTask = preregisteredTimerTasks.get(id);
		if(timerTask != null) {
			Runnable runnable = timerTask.getTask();

			if (runnable instanceof ScheduledTask) {
				preregisteredTimerTasks.remove(id);
				((ScheduledTask) runnable).runTask(timestamp);
			}
			return;
		}
		ProcessingTimeCallback callback = callbacks.get(id);

		if(callback == null)
			throw new RuntimeException("Timer "+ id+ " not found during recovery");

		try {
			callback.onProcessingTime(timestamp);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void concludeReplay() {
		LOG.info("Concluded replay, moving preregistered timers to main registry");
		for (PreregisteredTimer preregisteredTimer : preregisteredTimerTasks.values()) {
			LOG.debug("Preregistered timer: {}", preregisteredTimer);
			if (preregisteredTimer.task instanceof ScheduledTask){
				if(((ScheduledTask) preregisteredTimer.task).period==0){
					registerTimerRunning((ScheduledTask) preregisteredTimer.task, preregisteredTimer.delay);
				}
				else{
					registerAtFixedRateRunning(((ScheduledTask) preregisteredTimer.task).period,
						((ScheduledTask) preregisteredTimer.task).period,
						(ScheduledTask) preregisteredTimer.task,
					 false);
				}

			}
		}
		preregisteredTimerTasks.clear();
	}

	private class PreregisteredCompleteableFuture<T> implements ScheduledFuture<T> {

		private final ProcessingTimeCallbackID callbackID;

		public PreregisteredCompleteableFuture(ProcessingTimeCallbackID id) {
			this.callbackID = id;
		}

		@Override
		public long getDelay(TimeUnit timeUnit) {
			throw new UnsupportedOperationException("Not Implemented");
		}

		@Override
		public int compareTo(Delayed delayed) {
			throw new UnsupportedOperationException("Not Implemented");
		}

		@Override
		public boolean cancel(boolean b) {
			preregisteredTimerTasks.remove(callbackID);
			return true;
		}

		@Override
		public boolean isCancelled() {
			return preregisteredTimerTasks.containsKey(this.callbackID);
		}

		@Override
		public boolean isDone() {
			throw new UnsupportedOperationException("Not Implemented");
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			throw new UnsupportedOperationException("Not Implemented");
		}

		@Override
		public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
			throw new UnsupportedOperationException("Not Implemented");
		}
	}
}
