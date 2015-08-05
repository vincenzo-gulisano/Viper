package statelessOperator;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import operator.merger.Merger;
import operator.merger.MergerEntry;
import operator.viperBolt.BoltFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.TupleType;
import core.ViperValues;
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
	List<String> sourcesIds;
	int tuplesGot = 0;
	int tuplesAdded = 0;

	public BoltFunction getBoltFunction() {
		return boltFunction;
	}

	boolean goOn = true;

	public StatelessOpInternalThread(String id,
			ConcurrentLinkedQueue<Tuple> inQueue, Merger merger,
			BoltFunction boltFunction, String tsField, List<String> sourcesIds) {
		this.id = id;
		this.inQueue = inQueue;
		this.merger = merger;
		this.boltFunction = boltFunction;
		this.tsField = tsField;
		this.sourcesIds = sourcesIds;
	}

	public void stop() {
		goOn = false;
	}

	@Override
	public void run() {

		while (goOn) {
			if (inQueue.isEmpty())
				Utils.sleep(1);
			Tuple t = inQueue.poll();
			if (t != null) {

				TupleType ttype = (TupleType) t.getValueByField("type");
				if (ttype.equals(TupleType.FLUSH)) {
					for (String sourceId : sourcesIds) {
						LOG.info("Internal thread " + id
								+ " adding tuple for source " + sourceId);
						merger.add(sourceId + ":" + id, new MergerEntry(
								Long.MAX_VALUE, null));
					}
					goOn = false;
					LOG.info("Internal thread " + id + " got:" + tuplesGot
							+ " added:" + tuplesAdded);
				} else {
					tuplesGot++;
					// LOG.info("Got tuple " + t + " (id: "
					// + id + ")");

					List<Values> result = boltFunction.process(t);
					if (result != null)
						for (Values v : result) {
							String addId = t.getSourceComponent() + ":"
									+ t.getSourceTask() + ":" + id;
							// LOG.info("Internal thread " + addId +
							// " adding values " +
							// v);
							// System.out.println(t.getSourceComponent() + ":"
							// + t.getSourceTask() + ":" + id);
							merger.add(addId,
									new MergerEntry(t.getLongByField(tsField),
											v));
							tuplesAdded++;
						}
				}

			}
		}

	}

}
