package operator.merger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import operator.viperBolt.BoltFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import core.ViperValues;

public class ViperMergerFunction implements BoltFunction {

	public static Logger LOG = LoggerFactory
			.getLogger(ViperMergerFunction.class);
	private static final long serialVersionUID = -3710608737079122065L;
	private Merger merger;
	private String tsField;

	public ViperMergerFunction(String tsField) {
		this.tsField = tsField;
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		List<String> ids = new LinkedList<String>();
		Map<GlobalStreamId, Grouping> sources = context.getThisSources();
		for (GlobalStreamId source : sources.keySet()) {
			int parallelism_hint = context.getComponentCommon(
					source.get_componentId()).get_parallelism_hint();
			LOG.info("Merger " + context.getThisComponentId() + "."
					+ context.getThisTaskIndex() + " is fed by:");
			for (int i = 0; i < parallelism_hint; i++) {
				String thisId = source.get_componentId() + "." + i;
				LOG.info(thisId);
				ids.add(thisId);
			}
		}

		merger = new MergerSequential(ids);

	}

	@Override
	public List<Values> process(Tuple t) {
		List<Values> result = new LinkedList<Values>();
		merger.add(t.getStringByField("sourceID"),
				new MergerEntry(t.getLongByField(tsField), t));
		Tuple nReady = (Tuple) merger.getNextReady().getO();
		if (nReady != null)
			result.add(new ViperValues(nReady));
		return result;
	}

	@Override
	public List<Values> receivedFlush(Tuple t) {
		// TODO
		return null;
	}

	@Override
	public void receivedWriteLog(Tuple t) {
	}

}
