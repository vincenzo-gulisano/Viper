package operators;

import backtype.storm.tuple.Tuple;

public interface Merger {

	public void add(String id,long timestamp,Tuple t);
	
	public Tuple getNextReady();
	
}
