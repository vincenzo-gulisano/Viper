package operator.merger;


public interface Merger {
	
	static final long maxPendingFromStream = 50000L;
	
	public void add(String id,MergerEntry e);
	
	public MergerEntry getNextReady();
	
}
