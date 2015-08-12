package statelessOperator;

import java.util.List;
import java.util.Map;

import operator.merger.Merger;
import operator.merger.MergerEntry;
import operator.viperSpout.SpoutFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import core.ViperValues;

public class StatelessSpoutFunctionWrapper implements SpoutFunction {

	private static final long serialVersionUID = -8439873470686600500L;
	public static Logger LOG = LoggerFactory
			.getLogger(StatelessSpoutFunctionWrapper.class);
	private boolean receivedFlush;
	private Merger merger;
	private String mergerId;
	private String id;

	public StatelessSpoutFunctionWrapper(String mergerId) {
		this.mergerId = mergerId;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		id = context.getThisComponentId() + ":" + context.getThisTaskId();
		receivedFlush = false;
		merger = MergerFactory.spoutFactory(mergerId,
				context.getThisComponentId() + ":" + context.getThisTaskId());

	}

	@Override
	public Values getTuple() {
		MergerEntry entry = merger.getNextReady();
		if (entry != null) {
			if (entry.getTs() == Long.MAX_VALUE) {
				LOG.info(id + " received FLUSH tuple");
				receivedFlush = true;
				return null;
			} else {
				return (Values) entry.getO();
			}
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		return !receivedFlush;
	}

}
