package operator.merger;

import java.util.Map;

import operator.viperBolt.ViperBolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ViperMerger extends ViperBolt {

	public static Logger LOG = LoggerFactory.getLogger(ViperMerger.class);
	private static final long serialVersionUID = -3556782352568567327L;
	private int nextBoltTaskIndex;

	public ViperMerger(Fields outFields) {
		super(outFields, new ViperMergerFunction());
	}

	@SuppressWarnings("rawtypes")
	protected void childPrepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

//		Map<GlobalStreamId, Grouping> sources = context.getThisSources();
//		if (sources.size()!=1)
//			throw new RuntimeException("Merger bolts require exactly one source");
//		GlobalStreamId id = (GlobalStreamId) sources.keySet().toArray()[0];
//		System.out.println(context.getComponentOutputFields(id));
		
		nextBoltTaskIndex = context.getComponentTasks("mul").get(thisTaskIndex);

		System.out.println(stormConf.get("log.statistics.path"));

	}

	protected void emit(Tuple input, Values t) {
		t.set(2, id);
//		LOG.info("I am merger " + id + " and I am sending tuple " + t
//				+ " to task index " + nextBoltTaskIndex);
		collector.emitDirect(nextBoltTaskIndex, t);
	}
	
	protected void emitFlush(Tuple input) {
		collector.emitDirect(nextBoltTaskIndex, input.getValues());
	}
	
	protected void emitWriteLog(Tuple input) {
		collector.emitDirect(nextBoltTaskIndex, input.getValues());
	}

}
