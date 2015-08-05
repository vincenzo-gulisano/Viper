package topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import core.TupleType;

public class ViperShuffle implements CustomStreamGrouping, Serializable {

	private static final long serialVersionUID = 3014404246770284550L;
	int index = 0;
	List<Integer> targetTasks;
	int counter = 0;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		ArrayList<Integer> result = new ArrayList<Integer>();
		TupleType type = (TupleType) values.get(0);
		if (type.equals(TupleType.REGULAR)) {
			result.add(targetTasks.get(index));
			index = (index + 1) % targetTasks.size();
			counter++;
			return result;
		} else if (type.equals(TupleType.FLUSH)) {
			return targetTasks;
		}
		return null;
	}

}
