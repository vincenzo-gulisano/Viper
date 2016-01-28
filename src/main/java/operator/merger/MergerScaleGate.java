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
	HashMap<String, Integer> ids;
	String mergerId;
	ScaleGate scaleGate;

	public MergerScaleGate(List<String> ids, String mergerId) {
		this.mergerId = mergerId;
		latestInputTs = new LinkedList<Long>();
		this.ids = new HashMap<String, Integer>();
		int index = 0;

		scaleGate = new ScaleGateAArrImpl(4, ids.size(), 1);
		for (String id : ids) {
			this.ids.put(id, index);
			latestInputTs.add(index, -1L);
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
		latestInputTs.set(ids.get(id), e.getTs());
	}

	public MergerEntry getNextReady() {

		SGTuple t = scaleGate.getNextReadyTuple(0);

		if (t != null) {
			return ((SGTupleContainer) t).getME();
		}

		return null;

	}
}
