package operator.merger;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class MergerSequential implements Merger {

	LinkedList<ArrayDeque<MergerEntry>> queues;
	HashMap<String, Integer> ids;

	public MergerSequential(List<String> ids) {
		queues = new LinkedList<ArrayDeque<MergerEntry>>();
		this.ids = new HashMap<String, Integer>();
		int index = 0;
		for (String id : ids) {
			this.ids.put(id, index);
			queues.add(index, new ArrayDeque<MergerEntry>());
			index++;
		}
	}

	public void add(String id, MergerEntry e) {
		if (!ids.containsKey(id))
			throw new RuntimeException("Unknown id " + id);
		queues.get(ids.get(id)).add(e);
	}

	public MergerEntry getNextReady() {

		boolean allQueuesHaveATuple = true;
		int index = 0;
		for (int thisIndex = 0; thisIndex < ids.size(); thisIndex++) {
			allQueuesHaveATuple &= !queues.get(thisIndex).isEmpty();
			if (!allQueuesHaveATuple)
				break;
			if (thisIndex == 0
					|| queues.get(thisIndex).peek().getTs() < queues.get(index)
							.peek().getTs())
				index = thisIndex;
		}

		if (allQueuesHaveATuple)
			return queues.get(index).poll();

		return null;

	}
}
