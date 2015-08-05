package operator.merger;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import jline.internal.Log;

public class MergerSequential implements Merger {

	LinkedList<ArrayDeque<MergerEntry>> queues;
	LinkedList<Long> latestInputTs;
	long latestOutputTs;
	HashMap<String, Integer> ids;
	String mergerId;
	public boolean areWeFlushing = false;

	public MergerSequential(List<String> ids, String mergerId) {
		this.mergerId = mergerId;
		queues = new LinkedList<ArrayDeque<MergerEntry>>();
		latestInputTs = new LinkedList<Long>();
		this.ids = new HashMap<String, Integer>();
		int index = 0;
		for (String id : ids) {
			this.ids.put(id, index);
			queues.add(index, new ArrayDeque<MergerEntry>());
			latestInputTs.add(index, -1L);
			index++;
		}
		latestOutputTs = -1L;
	}

	public void add(String id, MergerEntry e) {
		if (!ids.containsKey(id))
			throw new RuntimeException("Unknown id " + id);
		if (latestInputTs.get(ids.get(id)) != -1
				&& latestInputTs.get(ids.get(id)) > e.getTs())
			throw new RuntimeException(mergerId + " cannot add entry " + e
					+ " from id " + id
					+ ": decreasing timestamp! (latest entry: "
					+ queues.get(ids.get(id)).peek() + ")");
		queues.get(ids.get(id)).add(e);
		latestInputTs.set(ids.get(id), e.getTs());
	}

	// TODO remove areWeFlushing
	public MergerEntry getNextReady() {

		if (areWeFlushing)
			System.out.println(mergerId + " getNextReady");
		boolean allQueuesHaveATuple = true;
		int index = 0;
		for (int thisIndex = 0; thisIndex < ids.size(); thisIndex++) {
			if (areWeFlushing)
				System.out.println(mergerId + " checking index " + thisIndex);
			allQueuesHaveATuple &= !queues.get(thisIndex).isEmpty();
			if (areWeFlushing) {
				System.out.println(mergerId + " allQueuesHaveATuple="
						+ allQueuesHaveATuple);
				System.out.println(mergerId + " at this index="
						+ queues.get(thisIndex).peek());
			}
			if (!allQueuesHaveATuple)
				break;
			if (thisIndex == 0
					|| queues.get(thisIndex).peek().getTs() < queues.get(index)
							.peek().getTs()) {
				index = thisIndex;
				if (areWeFlushing)
					System.out.println(mergerId + " chosen index=" + index);
			}
		}

		if (allQueuesHaveATuple) {
			if (latestOutputTs != -1
					&& latestOutputTs > queues.get(index).peek().getTs())
				throw new RuntimeException(mergerId
						+ "cannot return ready entry: decreasing timestamp!");
			latestOutputTs = queues.get(index).peek().getTs();
			if (areWeFlushing)
				System.out.println(mergerId + " returning "
						+ queues.get(index).peek());
			return queues.get(index).poll();
		}

		return null;

	}
}
