package operator.merger;


public interface Merger {
	
	static final long maxPendingFromStream = 10000L;
	static final long maxConsecutiveReadyTuples = 1000L;
	
	public void add(String id,MergerEntry e);
	
	public MergerEntry getNextReady();
	
}
