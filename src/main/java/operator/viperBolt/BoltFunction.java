package operator.viperBolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public interface BoltFunction extends Serializable {

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context);
	
	public List<Values> process(Tuple t);
	
	public List<Values> receivedFlush(Tuple t);
	
	public void receivedWriteLog(Tuple t);
	
}
