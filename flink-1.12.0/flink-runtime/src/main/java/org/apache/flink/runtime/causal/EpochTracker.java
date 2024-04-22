package org.apache.flink.runtime.causal;

import org.apache.flink.api.common.services.RandomService;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.runtime.state.CheckpointListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EpochTracker {
	private static final Logger LOG = LoggerFactory.getLogger(EpochTracker.class);
	public long checkpointID;
	private int recordCount;

	private int recordCountTarget;

	public RandomService randomService;

	private IRecoveryManager recoveryManager;


	private final List<EpochStartListener> epochStartListeners;
	private final List<CheckpointListener> checkpointCompleteListeners;

	public EpochTracker(){
		recordCount = 0;
		checkpointID =0;
		recordCountTarget = -1;
		epochStartListeners = new ArrayList<>(10);
		checkpointCompleteListeners = new ArrayList<>();
	}

	public void subscribeToCheckpointCompleteEvents(CheckpointListener listener) {
		this.checkpointCompleteListeners.add(listener);
	}

	public void notifyCheckpointComplete(long checkpointId) {
		LOG.info("Notified of checkpoint complete {}", checkpointId);
		for (CheckpointListener c : checkpointCompleteListeners) {
			try {
				c.notifyCheckpointComplete(checkpointId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public final int getRecordCount() {
		return recordCount;
	}

	public void setRecordCountTarget(int target) {
		this.recordCountTarget = target;
		if (LOG.isDebugEnabled())
			LOG.debug("setRecordCountTarget: current={}, target={}", recordCount, recordCountTarget);
		fireAnyAsyncEvent();
	}

	private void fireAnyAsyncEvent() {
		while (recordCountTarget == recordCount) {
			if (LOG.isDebugEnabled())
				LOG.debug("Hit target of {}!", recordCountTarget);
			recordCountTarget = -1;
			recoveryManager.getLogReplayer().triggerAsyncEvent();
		}
	}

	public void setRecoveryManager(IRecoveryManager recoveryManager) {
		this.recoveryManager = recoveryManager;
	}

	public long getCurrentEpoch() {
		return checkpointID;
	}


	public final void incRecordCount() {
		recordCount++;

		if (LOG.isDebugEnabled())
			LOG.debug("incRecordCount: current={}, target={}", recordCount, recordCountTarget);
		//Before returning control to the caller, check if we first should execute async nondeterministic event
		fireAnyAsyncEvent();
	}

	public final void startNewEpoch(long epochID) {
		recordCount = 0;
		checkpointID = epochID;
		if (LOG.isDebugEnabled())
			LOG.debug("startNewEpoch: epoch={}, current={}, target={}", epochID, recordCount, recordCountTarget);
		for (EpochStartListener listener : epochStartListeners)
			listener.notifyEpochStart(epochID);
		//check if async event is first event of the epoch
		fireAnyAsyncEvent();
	}

	public void setEpoch(long epochID) {
		this.checkpointID = epochID;
	}



	public void subscribeToEpochStartEvents(EpochStartListener listener) {
		this.epochStartListeners.add(listener);
	}


}
