package statelessOperator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import operator.merger.Merger;
import operator.merger.MergerEntry;
import operator.viperBolt.BoltFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.ViperValues;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StatelessBoltFunctionWrapper implements BoltFunction {

	private static final long serialVersionUID = 1113062442466819520L;

	public static Logger LOG = LoggerFactory
			.getLogger(StatelessBoltFunctionWrapper.class);

	private Merger merger;
	private BoltFunction f;
	private String id;
	private String thisTupleId;
	private String tsField;
	private String mergerId;
	private List<String> ids;

	public StatelessBoltFunctionWrapper(BoltFunction f, String tsField,
			String mergerId) {
		this.f = f;
		// this.fullQueueOccurences = 0;
		this.tsField = tsField;
		this.mergerId = mergerId;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		id = context.getThisComponentId() + ":" + context.getThisTaskId();

		// TODO Should check whether there's only 1 source!!!
		GlobalStreamId sourceId = (GlobalStreamId) context.getThisSources()
				.keySet().toArray()[0];
		List<Integer> componentIds = context.getComponentTasks(sourceId
				.get_componentId());
		ids = new LinkedList<String>();
		for (Integer i : componentIds)
			ids.add(id + ":" + sourceId.get_componentId() + ":" + i);

		merger = MergerFactory.boltFactory(mergerId, id, ids);

	}

	@Override
	public List<Values> process(Tuple t) {

		thisTupleId = id + ":" + t.getSourceComponent() + ":"
				+ t.getSourceTask();

		List<Values> result = f.process(t);
		if (result != null)
			for (Values v : result) {
				merger.add(thisTupleId,
						new MergerEntry(t.getLongByField(tsField), v));
				// LOG.info(id + " added values " + v);
			}

		List<Values> ready = new ArrayList<Values>();
		MergerEntry mergerEntry = merger.getNextReady();
		while (mergerEntry != null) {
			ready.add((Values) mergerEntry.getO());
			// LOG.info(id + " forwarding values " + (Values)
			// mergerEntry.getO());
			mergerEntry = merger.getNextReady();
		}
		return ready;

	}

	@Override
	public List<Values> receivedFlush(Tuple t) {

		// The ViperBolt is informing you that you received a flush
		// You want to add it to the merger for sure
		// You want to get all the remaining tuples in the merger
		// Once you are done, you want to disable the internal thread of the merger 
		// Then you can return the results...
		
		thisTupleId = id + ":" + t.getSourceComponent() + ":"
				+ t.getSourceTask();

		if (ids.contains(thisTupleId)) {
			LOG.info(id + " adding flush for " + thisTupleId);
			merger.add(thisTupleId, new MergerEntry(Long.MAX_VALUE, t));
			ids.remove(thisTupleId);
		}

		if (ids.isEmpty())
			return new ArrayList<Values>();

		return null;
	}

	@Override
	public List<Values> process(List<Object> v) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Values> receivedFlush(List<Object> v) {
		// TODO Auto-generated method stub
		return null;
	}

}
