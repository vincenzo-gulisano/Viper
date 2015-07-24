package operators;

import backtype.storm.tuple.Tuple;

public class MergerSequential implements Merger {

	public MergerSequential() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(String id, long timestamp, Tuple t) {
		// TODO Auto-generated method stub

	}

	@Override
	public Tuple getNextReady() {
		// TODO Auto-generated method stub
		return null;
	}

}
