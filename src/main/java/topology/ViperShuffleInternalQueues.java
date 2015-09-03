package topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import core.TupleType;

public class ViperShuffleInternalQueues implements CustomStreamGrouping,
		Serializable {

	private static final long serialVersionUID = 3014404246770284550L;
	int index = 0;
	List<Integer> targetTasks;
	HashMap<Integer, Boolean> local;
	HashMap<Integer, Integer> maxPending;
	private final int MAXPENDING = 10000;

	// This one is wrong, we need queues for each sender/receiver pair, not just
	// the receiver!!!
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		local = new HashMap<Integer, Boolean>();
		maxPending = new HashMap<Integer, Integer>();
		for (int task : targetTasks) {
			local.put(task, context.getThisWorkerTasks().contains(task));
			maxPending.put(task, MAXPENDING);
		}
		this.targetTasks = targetTasks;
	}

	// TODO Could be improved without creating always a new Arraylist
	// TODO Also have time to reset pending
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		ArrayList<Integer> result = new ArrayList<Integer>();
		TupleType type = (TupleType) values.get(0);
		if (type.equals(TupleType.REGULAR)) {
			int task = targetTasks.get(index);
			if (local.get(task) && maxPending.get(task) > 0) {
				SharedQueues.add(taskId + ":" + task, values);
				maxPending.put(task, maxPending.get(task) - 1);
			} else {
				maxPending.put(task, MAXPENDING);
				result.add(task);
			}
			index = (index + 1) % targetTasks.size();
			return result;
		} else if (type.equals(TupleType.FLUSH)) {
			return targetTasks;
		}
		return null;
	}
}
