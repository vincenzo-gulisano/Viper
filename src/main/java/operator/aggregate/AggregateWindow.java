package operator.aggregate;

import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public interface AggregateWindow {

	public AggregateWindow factory();

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context);
	
	public void update(Tuple t);

	public List<Object> getAggregatedResult();

	
}
