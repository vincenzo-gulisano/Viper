package operator.viperBolt;

import java.io.Serializable;
import java.util.List;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public interface BoltFunction extends Serializable {

	public void prepare(TopologyContext context);
	
	public List<Values> process(Tuple t);
	
	public void receivedFlush(Tuple t);
	
	public void receivedWriteLog(Tuple t);
	
}
