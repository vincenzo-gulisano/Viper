package statelessOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import operator.merger.Merger;
import operator.merger.MergerEntry;
import operator.merger.MergerSequential;
import operator.viperBolt.BoltFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StatelessBoltFunctionWrapper implements BoltFunction {

	private static final long serialVersionUID = 1113062442466819520L;

	public static Logger LOG = LoggerFactory
			.getLogger(StatelessBoltFunctionWrapper.class);

	private ConcurrentLinkedQueue<Tuple> internalQueue;
	private Merger merger;
	private int parallelism;
	private List<Thread> threads;
	private List<StatelessOpInternalThread> internalOps;
	private BoltFunctionFactory factory;
	private int fullQueueOccurences;
	private String tsField;
	private List<String> internalThreadsIds;
	private List<String> componentsIds;

	public StatelessBoltFunctionWrapper(BoltFunctionFactory factory,
			int parallelism, String tsField) {
		this.factory = factory;
		this.parallelism = parallelism;
		this.fullQueueOccurences = 0;
		this.tsField = tsField;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		// Find the sources
		Map<GlobalStreamId, Grouping> sources = context.getThisSources();
		if (sources.size() != 1)
			throw new RuntimeException(
					"Merger bolts require exactly one source");
		GlobalStreamId id = (GlobalStreamId) sources.keySet().toArray()[0];
		List<Integer> componentIds = context.getComponentTasks(id
				.get_componentId());
		internalThreadsIds = new ArrayList<String>();
		componentsIds = new ArrayList<String>();
		for (Integer i : componentIds) {
			componentsIds.add(id.get_componentId() + ":" + i);
			for (int j = 0; j < parallelism; j++) {
				internalThreadsIds
						.add(id.get_componentId() + ":" + i + ":" + j);
			}
		}

		internalQueue = new ConcurrentLinkedQueue<Tuple>();
		merger = new MergerSequential(internalThreadsIds);

		threads = new ArrayList<Thread>();
		internalOps = new ArrayList<StatelessOpInternalThread>();

		for (int i = 0; i < parallelism; i++) {
			internalOps.add(new StatelessOpInternalThread(String.valueOf(i),
					internalQueue, merger, factory.getBoltFunction(), tsField));
			internalOps.get(i).getBoltFunction().prepare(stormConf, context);
			threads.add(new Thread(internalOps.get(i)));
			threads.get(i).start();
		}

	}

	@Override
	public List<Values> process(Tuple t) {

		// Limit speed to avoid too large queues...
		if (internalQueue.size() >= 5)
			fullQueueOccurences++;
		else
			fullQueueOccurences = 0;
		if (fullQueueOccurences > 0)
			Utils.sleep(fullQueueOccurences);

		internalQueue.add(t);
		// LOG.info("Added tuple " + t + " to internal queue (size: "
		// + internalQueue.size() + ")");
		List<Values> result = new ArrayList<Values>();
		MergerEntry me = merger.getNextReady();
		while (me != null) {
			// LOG.info("Got values " + (Values) me.getO()
			// + " from internal queue (size: " + internalQueue.size()
			// + ")");
			result.add((Values) me.getO());
			me = merger.getNextReady();
		}

		return result;

	}

	@Override
	public List<Values> receivedFlush(Tuple t) {

		// Managing the last tuples (flushing)
		List<Values> lastValues = new ArrayList<Values>();
		MergerEntry me = merger.getNextReady();
		while (me != null) {
			lastValues.add((Values) me.getO());
			LOG.info("Adding last values "+lastValues);
			me = merger.getNextReady();
		}
		for (int i = 0; i < internalOps.size(); i++) {
			for (String id : componentsIds) {
				internalOps.get(i).addFlushingTuple(id);
			}
		}
		me = merger.getNextReady();
		while (me != null) {
			LOG.info("Adding last values "+lastValues);
			lastValues.add((Values) me.getO());
			me = merger.getNextReady();
		}

		for (StatelessOpInternalThread s : internalOps) {
			s.getBoltFunction().receivedFlush(t);
			s.stop();
		}
		for (Thread th : threads)
			try {
				th.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		return lastValues;

	}

	@Override
	public void receivedWriteLog(Tuple t) {
		// TO DO, maybe just remove this?
	}

}
