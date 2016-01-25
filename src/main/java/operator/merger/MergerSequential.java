package operator.merger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

public class MergerSequential implements Merger {

	public static Logger LOG = LoggerFactory.getLogger(MergerSequential.class);
	LinkedList<LinkedList<MergerEntry>> queues;
	LinkedList<Long> latestInputTs;
	long latestOutputTs;
	HashMap<String, Integer> ids;
	String mergerId;

	public MergerSequential(List<String> ids, String mergerId) {
		this.mergerId = mergerId;
		queues = new LinkedList<LinkedList<MergerEntry>>();
		latestInputTs = new LinkedList<Long>();
		this.ids = new HashMap<String, Integer>();
		int index = 0;
		for (String id : ids) {
			this.ids.put(id, index);
			queues.add(index, new LinkedList<MergerEntry>());
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

		if (queues.get(ids.get(id)).size() >= Merger.maxPendingFromStream) {
			Utils.sleep(1);
			LOG.info("Merger " + mergerId + " queue for " + id
					+ " size exceeded " + Merger.maxPendingFromStream);

			for (String key : ids.keySet()) {
				LOG.info("Queue " + mergerId + " " + key + " (number "
						+ ids.get(key) + ") has "
						+ queues.get(ids.get(key)).size() + " tuples.");
				if (!queues.get(ids.get(key)).isEmpty()) {
					LOG.info("\t" + mergerId + " First tuple is "
							+ queues.get(ids.get(key)).peekFirst().toString());
					LOG.info("\t" + mergerId + " Last tuple is "
							+ queues.get(ids.get(key)).peekLast().toString());
				}
			}

			Utils.sleep(10000);
		}

		queues.get(ids.get(id)).add(e);
		latestInputTs.set(ids.get(id), e.getTs());
	}

	public MergerEntry getNextReady() {

		boolean allQueuesHaveATuple = true;
		int index = 0;
		long thisIndexTS = 0;
		long indexTS = 0;
		for (int thisIndex = 0; thisIndex < ids.size(); thisIndex++) {
			allQueuesHaveATuple &= !queues.get(thisIndex).isEmpty();
			if (!allQueuesHaveATuple)
				break;
			thisIndexTS = queues.get(thisIndex).peek().getTs();
			indexTS = queues.get(index).peek().getTs();
			if (thisIndex == 0 || thisIndexTS < indexTS) {
				index = thisIndex;
			}
		}

		if (allQueuesHaveATuple) {
			if (latestOutputTs != -1
					&& latestOutputTs > queues.get(index).peek().getTs())
				throw new RuntimeException(mergerId
						+ "cannot return ready entry: decreasing timestamp!");
			latestOutputTs = queues.get(index).peek().getTs();
			return queues.get(index).poll();
		}

		return null;

	}
}
