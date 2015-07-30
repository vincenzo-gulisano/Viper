package operator.viperSpout;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

public interface SpoutFunction extends Serializable {
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context);

	Values getTuple();

	boolean hasNext();
}
