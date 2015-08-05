package operator.merger;

public class MergerEntry {

	private long ts;

	private Object o;

	public MergerEntry(long ts, Object o) {
		this.ts = ts;
		this.o = o;
	}

	public long getTs() {
		return ts;
	}

	public Object getO() {
		return o;
	}

	public String toString() {
		return ts + "," + o;
	}

}
