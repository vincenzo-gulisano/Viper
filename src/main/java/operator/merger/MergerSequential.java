package operator.merger;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import backtype.storm.tuple.Tuple;

public class MergerSequential implements Merger {

	LinkedList<ArrayDeque<Tuple>> queues;
	HashMap<String, Integer> ids;

	public MergerSequential(List<String> ids) {
		queues = new LinkedList<ArrayDeque<Tuple>>();
		this.ids = new HashMap<String, Integer>();
		int index = 0;
		for (String id : ids) {
			this.ids.put(id, index);
			queues.add(index, new ArrayDeque<Tuple>());
			index++;
		}
	}

	public void add(String id, Tuple t) {
		if (!ids.containsKey(id))
			throw new RuntimeException("Unknown id " + id);
		queues.get(ids.get(id)).add(t);
	}

	public Tuple getNextReady() {

		boolean allQueuesHaveATuple = true;
		int index = 0;
		for (int thisIndex = 0; thisIndex < ids.size(); thisIndex++) {
			allQueuesHaveATuple &= !queues.get(thisIndex).isEmpty();
			if (!allQueuesHaveATuple)
				break;
			if (thisIndex == 0
					|| queues.get(thisIndex).peek().getLongByField("ts") < queues
							.get(index).peek().getLongByField("ts"))
				index = thisIndex;
		}

		if (allQueuesHaveATuple)
			return queues.get(index).poll();

		return null;

	}
}
