package statelessOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import operator.merger.Merger;
import operator.merger.MergerThreadSafe;
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
	private ConcurrentLinkedQueue<Values> readyTuplesQueue;
	private Merger merger;
	private int parallelism;

	private List<Thread> threads;
	private List<StatelessOpInternalThread> internalOps;

	private Thread readyThread;
	private StatelessOpReadyThread readyOp;

	private BoltFunctionFactory factory;
	private int fullQueueOccurences;
	private String tsField;
	private List<String> internalThreadsIds;
	private List<String> componentsIds;
	private Set<String> componentsIdsWaitingFlush;
	private int counter = 0;

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
		componentsIdsWaitingFlush = new HashSet<String>();
		for (Integer i : componentIds) {
			componentsIds.add(id.get_componentId() + ":" + i);
			componentsIdsWaitingFlush.add(id.get_componentId() + ":" + i);
			for (int j = 0; j < parallelism; j++) {
				internalThreadsIds
						.add(id.get_componentId() + ":" + i + ":" + j);
			}
		}

		internalQueue = new ConcurrentLinkedQueue<Tuple>();
		readyTuplesQueue = new ConcurrentLinkedQueue<Values>();
		merger = new MergerThreadSafe(internalThreadsIds,
				context.getThisComponentId() + ":" + context.getThisTaskId());

		threads = new ArrayList<Thread>();
		internalOps = new ArrayList<StatelessOpInternalThread>();

		for (int i = 0; i < parallelism; i++) {
			internalOps.add(new StatelessOpInternalThread(String.valueOf(i),
					internalQueue, merger, factory.getBoltFunction(), tsField,
					componentsIds));
			internalOps.get(i).getBoltFunction().prepare(stormConf, context);
			threads.add(new Thread(internalOps.get(i), "Internal thread " + i));
			threads.get(i).start();
		}

		readyOp = new StatelessOpReadyThread(readyTuplesQueue, merger);
		readyThread = new Thread(readyOp);
		readyThread.start();

	}

	@Override
	public List<Values> process(Tuple t) {

		// Limit speed to avoid too large queues...
		if (internalQueue.size() >= 1000)
			fullQueueOccurences++;
		else
			fullQueueOccurences = 0;
		if (fullQueueOccurences > 0)
			Utils.sleep(fullQueueOccurences);

		internalQueue.add(t);
		counter++;
		// LOG.info("Added tuple " + t + " to internal queue (size: "
		// + internalQueue.size() + ")");
		List<Values> result = new ArrayList<Values>();

		// With dedicated thread taking out ready tuples...
		Values ready = readyTuplesQueue.poll();
		while (ready != null) {
			result.add((ready));
			ready = readyTuplesQueue.poll();
		}

		// MergerEntry me = merger.getNextReady();
		// while (me != null) {
		// // LOG.info("Got values " + (Values) me.getO()
		// // + " from internal queue (size: " + internalQueue.size()
		// // + ")");
		// result.add((Values) me.getO());
		// me = merger.getNextReady();
		// }

		return result;

	}

	@Override
	public List<Values> receivedFlush(Tuple t) {

		// Managing the last tuples (flushing)

		// First we put one flush tuple for each internal thread
		// When getting a flush tuple, the internal thread adds a tuple for each
		// source in the merger and then exists, so we only have to wait...
		if (!componentsIdsWaitingFlush.contains(t.getSourceComponent() + ":"
				+ t.getSourceTask()))
			throw new RuntimeException("Why the fuck do I receive an ack from "
					+ t.getSourceComponent() + ":" + t.getSourceTask() + "?");

		componentsIdsWaitingFlush.remove(t.getSourceComponent() + ":"
				+ t.getSourceTask());
		if (componentsIdsWaitingFlush.isEmpty()) {

			LOG.info("Adding flush tuples");
			for (int i = 0; i < internalOps.size(); i++) {
				internalQueue.add(t);
			}

			LOG.info("Waiting for threads to complete");
			for (Thread th : threads)
				try {
					LOG.info("Waiting for thread " + th.getName());
					th.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			LOG.info("Waiting for ready thread to complete");
			try {
				readyThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			List<Values> lastValues = new ArrayList<Values>();
			Values ready = readyTuplesQueue.poll();
			while (ready != null) {
				lastValues.add((ready));
				ready = readyTuplesQueue.poll();
			}
			
			// THIS IS NOW MANAGED BY DEDICATED THREAD
//			MergerEntry me = merger.getNextReady();
//			while (me != null && me.getO() != null) {
//				lastValues.add((Values) me.getO());
//				// LOG.info("Adding last values " + me.getO());
//				me = merger.getNextReady();
//			}

			LOG.info("Total tuples added " + counter);
			// And now we can return the values
			return lastValues;
		}

		return null;

	}

	// @Override
	// public void receivedWriteLog(Tuple t) {
	// // TO DO, maybe just remove this?
	// }

}
