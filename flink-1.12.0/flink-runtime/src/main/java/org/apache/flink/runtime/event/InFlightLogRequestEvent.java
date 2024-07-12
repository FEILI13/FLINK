package org.apache.flink.runtime.event;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;

public class InFlightLogRequestEvent extends TaskEvent {

	private IntermediateResultPartitionID intermediateResultPartitionID;
	private int subpartitionIndex;
	private long checkpointId;
	private int numberOfBuffersToSkip;

	/**
	 * Default constructor (should only be used for deserialization).
	 */
	public InFlightLogRequestEvent() {
		// default constructor implementation.
		// should only be used for deserialization
	}




	public InFlightLogRequestEvent(IntermediateResultPartitionID intermediateResultPartitionID, int consumedSubpartitionIndex, long finalRestoreStateCheckpointId) {
		this(intermediateResultPartitionID, consumedSubpartitionIndex, finalRestoreStateCheckpointId, 0);
	}

	public InFlightLogRequestEvent(IntermediateResultPartitionID intermediateResultPartitionID, int consumedSubpartitionIndex, long finalRestoreStateCheckpointId, int numberOfBuffersToSkip) {
		super();
		this.intermediateResultPartitionID = intermediateResultPartitionID;
		this.subpartitionIndex = consumedSubpartitionIndex;
		this.checkpointId = finalRestoreStateCheckpointId;
		this.numberOfBuffersToSkip = numberOfBuffersToSkip;
	}

	public IntermediateResultPartitionID getIntermediateResultPartitionID() {
		return intermediateResultPartitionID;
	}

	public int getSubpartitionIndex() {
		return subpartitionIndex;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public int getNumberOfBuffersToSkip(){
		return numberOfBuffersToSkip;
	}

	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeLong(intermediateResultPartitionID.getIntermediateResultPartitionID().getUpperPart());
		out.writeLong(intermediateResultPartitionID.getIntermediateResultPartitionID().getLowerPart());
		out.writeInt(this.subpartitionIndex);
		out.writeLong(this.checkpointId);
		out.writeInt(numberOfBuffersToSkip);
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		long upper = in.readLong();
		long lower = in.readLong();
		this.subpartitionIndex = in.readInt();
		this.intermediateResultPartitionID = new IntermediateResultPartitionID(new IntermediateDataSetID(lower,upper), subpartitionIndex);


		this.checkpointId = in.readLong();
		this.numberOfBuffersToSkip = in.readInt();
	}

	@Override
	public String toString() {
		return "InFlightLogRequestEvent{" +
			"intermediateResultPartitionID=" + intermediateResultPartitionID +
			", subpartitionIndex=" + subpartitionIndex +
			", checkpointId=" + checkpointId +
			", numberBuffersSkip=" + numberOfBuffersToSkip +
			'}';
	}

}

