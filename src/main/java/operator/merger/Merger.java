package operator.merger;

import backtype.storm.tuple.Tuple;

public interface Merger {

	public void add(String id,MergerEntry e);
	
	public MergerEntry getNextReady();
	
}
