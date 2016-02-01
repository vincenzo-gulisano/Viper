package topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import operator.merger.MergerEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.utils.Utils;
import core.TupleType;

public class ViperFieldsSharedChannels implements CustomStreamGrouping,
		Serializable {

	public static Logger LOG = LoggerFactory
			.getLogger(ViperFieldsSharedChannels.class);
	private static final long serialVersionUID = 3014404246770284550L;
	// int index = 0;
	List<Integer> targetTasks;
	// int counter = 0;

	private SharedChannelsScaleGate sharedChannels;
	Map<Integer, String> destinationChannelsIDs;

	Map<Integer, Boolean> scheduledSleep;
	int scheduleSleepIndex = 0;

	private boolean firstTuple = true;
	private int tsIndex;
	private int fieldIndex;

	// TODO need to do something smarter for this!
	public ViperFieldsSharedChannels(int fieldIndex) {
		destinationChannelsIDs = new HashMap<Integer, String>();
		scheduledSleep = new HashMap<Integer, Boolean>();
		this.tsIndex = 1; // The user does not specify a timestamp, so we take
							// the tuple one
		this.fieldIndex = fieldIndex + 2;
	}

	public ViperFieldsSharedChannels(int tsIndex, int fieldIndex) {
		destinationChannelsIDs = new HashMap<Integer, String>();
		scheduledSleep = new HashMap<Integer, Boolean>();
		this.tsIndex = tsIndex + 2; // The user specifies a timestamp, so we
									// take that one
		this.fieldIndex = fieldIndex + 2;
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
		sharedChannels = SharedChannelsScaleGate.factory();
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {

		if (firstTuple) {
			firstTuple = false;
			LOG.info("First tuple for shuffle assigned to task " + taskId
					+ ". Targets: " + this.targetTasks);
			for (int i : targetTasks) {
				destinationChannelsIDs.put(i,
						sharedChannels.getChannelsID("" + i, "" + taskId));
				scheduledSleep.put(i, false);
				LOG.info("Channel from " + taskId + " to " + i + " is "
						+ destinationChannelsIDs.get(i));
			}
		}

		ArrayList<Integer> result = new ArrayList<Integer>();
		TupleType type = (TupleType) values.get(0);

		if (type.equals(TupleType.SHAREDQUEUEDUMMY)) {
			// LOG.info("Forwarding SHAREDQUEUEDUMMY to all destinations (task "
			// + taskId + ")");

			// If the size of for the internal channel at the current index is
			// greater than 50000, remember to sleep next time...
			scheduledSleep.put(targetTasks.get(scheduleSleepIndex),
					sharedChannels.getSize(destinationChannelsIDs
							.get(targetTasks.get(scheduleSleepIndex))) > 50000);
			scheduleSleepIndex = (scheduleSleepIndex + 1) % targetTasks.size();

			return targetTasks;
		} else if (type.equals(TupleType.REGULAR)) {

			int index = values.get(fieldIndex).hashCode() % targetTasks.size();

			if (scheduledSleep.get(targetTasks.get(index))) {
				scheduledSleep.put(targetTasks.get(index), false);
//				LOG.info("Putting to sleep task " + taskId);
				Utils.sleep(1);
			}

			// Notice that I am assuming the timestamp is alway in position 1,
			// so it is the internal timestamp, not user defined one

			sharedChannels.addObj("" + taskId,
					destinationChannelsIDs.get(targetTasks.get(index)),
					new MergerEntry((Long) values.get(tsIndex), values));
			// index = (index + 1) % targetTasks.size();
			// counter++;
			return result;

		} else if (type.equals(TupleType.FLUSH)) {
			return targetTasks;
		}
		return null;
	}
}
