package operator.merger;

import backtype.storm.tuple.Tuple;

public interface Merger {

	public void add(String id,Tuple t);
	
	public Tuple getNextReady();
	
}
