package statelessOperator;

import java.util.concurrent.ConcurrentLinkedQueue;

import operator.merger.Merger;
import operator.merger.MergerEntry;
import operator.viperBolt.BoltFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StatelessOpInternalThread implements Runnable {

	public static Logger LOG = LoggerFactory
			.getLogger(StatelessOpInternalThread.class);

	String id;
	ConcurrentLinkedQueue<Tuple> inQueue;
	Merger merger;
	BoltFunction boltFunction;
	String tsField;

	public BoltFunction getBoltFunction() {
		return boltFunction;
	}

	boolean goOn = true;

	public StatelessOpInternalThread(String id,
			ConcurrentLinkedQueue<Tuple> inQueue, Merger merger,
			BoltFunction boltFunction, String tsField) {
		this.id = id;
		this.inQueue = inQueue;
		this.merger = merger;
		this.boltFunction = boltFunction;
		this.tsField = tsField;
	}

	public void stop() {
		goOn = false;
	}

	public void addFlushingTuple(String source) {
		merger.add(source + ":" + id, new MergerEntry(Long.MAX_VALUE, null));
	}

	@Override
	public void run() {

		while (goOn) {
			if (inQueue.isEmpty())
				Utils.sleep(1);
			Tuple t = inQueue.poll();
			if (t != null) {

				// LOG.info("Got tuple " + t + " (id: "
				// + id + ")");

				for (Values v : boltFunction.process(t)) {
					String addId = t.getSourceComponent() + ":"
							+ t.getSourceTask() + ":" + id;
					// LOG.info("Internal thread " + addId + " adding values " +
					// v);
					// System.out.println(t.getSourceComponent() + ":"
					// + t.getSourceTask() + ":" + id);
					merger.add(addId, new MergerEntry(
							t.getLongByField(tsField), v));
				}
			}
		}

	}

}
