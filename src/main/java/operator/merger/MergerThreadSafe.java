package operator.merger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergerThreadSafe extends Thread implements Merger {

	public static Logger LOG = LoggerFactory.getLogger(MergerThreadSafe.class);
	LinkedList<ConcurrentLinkedQueue<MergerEntry>> queues;
	LinkedList<Long> latestInputTs;
	long latestOutputTs;
	HashMap<String, Integer> ids;
	HashSet<String> idsThatHaveNotStarted;
	boolean gotOneFromEachId;
	String mergerId;

	ConcurrentLinkedQueue<MergerEntry> readyTuples;
	private boolean goOn;

	public MergerThreadSafe(String mergerId) {
		this.mergerId = mergerId;
		queues = new LinkedList<ConcurrentLinkedQueue<MergerEntry>>();
		readyTuples = new ConcurrentLinkedQueue<MergerEntry>();
		goOn = true;
		latestInputTs = new LinkedList<Long>();
		this.ids = new HashMap<String, Integer>();
		idsThatHaveNotStarted = new HashSet<String>();
		latestOutputTs = -1L;
		gotOneFromEachId = false;
	}

	public void registerInput(String id) {
		int index = this.ids.size();
		this.ids.put(id, index);
		idsThatHaveNotStarted.add(id);
		queues.add(index, new ConcurrentLinkedQueue<MergerEntry>());
		latestInputTs.add(index, -1L);
	}

	public void add(String id, MergerEntry e) {
		if (!ids.containsKey(id))
			throw new RuntimeException("Unknown id " + id);
		if (!gotOneFromEachId && idsThatHaveNotStarted.contains(id)) {
			idsThatHaveNotStarted.remove(id);
			if (idsThatHaveNotStarted.isEmpty()) {
				LOG.info(mergerId + " starting internal thread");
				this.start();
				gotOneFromEachId = true;
			}
		}
		if (latestInputTs.get(ids.get(id)) != -1
				&& latestInputTs.get(ids.get(id)) > e.getTs())
			throw new RuntimeException(mergerId + " cannot add entry " + e
					+ " from id " + id
					+ ": decreasing timestamp! (latest entry: "
					+ queues.get(ids.get(id)).peek() + ")");
		queues.get(ids.get(id)).add(e);
		latestInputTs.set(ids.get(id), e.getTs());
	}

	@Override
	public void run() {
		while (goOn) {
			if (gotOneFromEachId) {
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
							&& latestOutputTs > queues.get(index).peek()
									.getTs())
						throw new RuntimeException(
								mergerId
										+ "cannot return ready entry: decreasing timestamp!");
					latestOutputTs = queues.get(index).peek().getTs();
					readyTuples.add(queues.get(index).poll());
				}
			}
		}
	}

	public MergerEntry getNextReady() {

		return readyTuples.poll();

	}
}
