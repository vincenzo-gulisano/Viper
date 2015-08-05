package operator.merger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import operator.viperBolt.BoltFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import core.ViperUtils;
import core.ViperValues;

public class ViperMergerFunction implements BoltFunction {

	public static Logger LOG = LoggerFactory
			.getLogger(ViperMergerFunction.class);
	private static final long serialVersionUID = -3710608737079122065L;
	private Merger merger;
	private String tsField;
	// List<Values> flushedResults;
	private String id;
	List<String> ids;
	HashSet<String> idsFlushed;

	public ViperMergerFunction(String tsField) {
		this.tsField = tsField;
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		id = context.getThisComponentId() + ":" + context.getThisTaskIndex();

		// TODO Should check whether there's only 1 source!!!
		GlobalStreamId id = (GlobalStreamId) context.getThisSources().keySet()
				.toArray()[0];
		List<Integer> componentIds = context.getComponentTasks(id
				.get_componentId());

		ids = new LinkedList<String>();
		idsFlushed = new HashSet<String>();
		LOG.info("Merger " + context.getThisComponentId() + "."
				+ context.getThisTaskIndex() + " is fed by:");
		for (Integer i : componentIds) {
			LOG.info(id.get_componentId() + ":" + i);
			ids.add(id.get_componentId() + ":" + i);
			idsFlushed.add(id.get_componentId() + ":" + i);
		}

		merger = new MergerSequential(ids, this.id);
		// flushedResults = new LinkedList<Values>();

	}

	@Override
	public List<Values> process(Tuple t) {
		List<Values> result = new LinkedList<Values>();
		merger.add(t.getSourceComponent() + ":" + t.getSourceTask(),
				new MergerEntry(t.getLongByField(tsField), t));
		MergerEntry me = merger.getNextReady();
		if (me != null)
			result.add(new ViperValues((Tuple) me.getO()));
		return result;
	}

	@Override
	public List<Values> receivedFlush(Tuple t) {
		((MergerSequential) merger).areWeFlushing = false;
		merger.add(t.getSourceComponent() + ":" + t.getSourceTask(),
				new MergerEntry(Long.MAX_VALUE, t));
		// LOG.info(id + " adding flush from " + t.getSourceComponent() + ":"
		//		+ t.getSourceTask());
		idsFlushed.remove(t.getSourceComponent() + ":" + t.getSourceTask());
		if (idsFlushed.isEmpty()) {
			List<Values> flushedResults = new ArrayList<Values>();
			MergerEntry me = merger.getNextReady();
			while (me != null) {
				Tuple outTuple = (Tuple) me.getO();
				if (ViperUtils.isFlushTuple(outTuple))
					return flushedResults;
				else
					flushedResults.add(new ViperValues(outTuple));
				me = merger.getNextReady();
			}
		}
		return null;
	}
	//
	// @Override
	// public void receivedWriteLog(Tuple t) {
	// }

}
