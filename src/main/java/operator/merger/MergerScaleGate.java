package operator.merger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scalegate.SGTuple;
import scalegate.ScaleGate;
import scalegate.ScaleGateAArrImpl;

public class MergerScaleGate implements Merger {

	public static Logger LOG = LoggerFactory.getLogger(MergerScaleGate.class);
	LinkedList<Long> latestInputTs;
	LinkedList<Long> individualCounts;
	HashMap<String, Integer> ids;
	String mergerId;
	ScaleGate scaleGate;

//	private long count = 0;

	// private long size = 0;

	public MergerScaleGate(List<String> ids, String mergerId) {
		this.mergerId = mergerId;
		latestInputTs = new LinkedList<Long>();
		individualCounts = new LinkedList<Long>();
		this.ids = new HashMap<String, Integer>();
		int index = 0;

		scaleGate = new ScaleGateAArrImpl(4, ids.size(), 1);
		for (String id : ids) {
			this.ids.put(id, index);
			latestInputTs.add(index, -1L);
			individualCounts.add(index, 0L);
			scaleGate.addTuple(new SGTupleContainer(), index);
			index++;
		}

	}

	public void add(String id, MergerEntry e) {
		if (!ids.containsKey(id))
			throw new RuntimeException("Unknown id " + id);
		if (latestInputTs.get(ids.get(id)) != -1
				&& latestInputTs.get(ids.get(id)) > e.getTs())
			throw new RuntimeException(mergerId + " cannot add entry " + e
					+ " from id " + id
					+ ": decreasing timestamp! (latest entry: "
					+ latestInputTs.get(ids.get(id)) + ")");

		scaleGate.addTuple(new SGTupleContainer(e), ids.get(id));
		individualCounts
				.set(ids.get(id), individualCounts.get(ids.get(id)) + 1);
		latestInputTs.set(ids.get(id), e.getTs());
		// size++;
		//
		// if (size % 1000 == 0) {
		// Utils.sleep(1);
		// }
		// if (size % 10000 == 0) {
		// LOG.info("Size of ScaleGate at " + mergerId + ": " + size);
		// for (String i : ids.keySet()) {
		// LOG.info("Size of ScaleGate at " + mergerId + " "
		// + individualCounts.get(ids.get(i)) + " from " + i
		// + " latest ts: " + latestInputTs.get(ids.get(i)));
		// }
		// }
	}

	public MergerEntry getNextReady() {

		SGTuple t = scaleGate.getNextReadyTuple(0);

		if (t != null) {
//			// size--;
//			if (((SGTupleContainer) t).getME() != null) {
//				count++;
//				if (count > 200000) {
//					int x = 0;
//					x++;
//				}
//			}
			return ((SGTupleContainer) t).getME();
		}

		return null;

	}
}
