package operator.merger;

public class MergerEntry {

	private long ts;
	private int taskId;
	private String StreamId;
	private Object o;

	public MergerEntry(long ts, int taskId, String streamId, Object o) {
		this.ts = ts;
		this.taskId = taskId;
		this.StreamId = streamId;
		this.o = o;
	}

	public long getTs() {
		return ts;
	}

	public int getTaskId() {
		return taskId;
	}

	public String getStreamId() {
		return StreamId;
	}

	public Object getO() {
		return o;
	}

	public String toString() {
		return ts + "," + o;
	}

}
