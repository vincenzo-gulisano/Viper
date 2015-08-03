package operator.viperBolt;

import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public abstract class BoltFunctionBase implements BoltFunction {

	private static final long serialVersionUID = 222933822284602305L;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public abstract List<Values> process(Tuple t);

	@Override
	public List<Values> receivedFlush(Tuple t) {
		return null;
	}

	@Override
	public void receivedWriteLog(Tuple t) {

	}

}
