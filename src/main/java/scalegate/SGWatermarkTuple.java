/**
 * 
 */
package scalegate;

/**
 * @author johnnik
 *Class for Watermark Tuples, used in the integrated flow-control mechanism of SG
 */
public class SGWatermarkTuple implements SGTuple {

	private long ts;
	private long watermark;
	
	public SGWatermarkTuple(long ts, long wm) {
		this.ts = ts;
		this.watermark = wm;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(SGTuple o) {
		if (getTS() == o.getTS()) {
			return 0;
		} else {
			return getTS() > o.getTS() ? 1 : -1;
		}
	}

	/* (non-Javadoc)
	 * @see scalegate.SGTuple#getTS()
	 */
	@Override
	public long getTS() {
		return ts;
	}

	@Override
	public boolean isWM() {
		return true;
	}

	@Override
	public long getWM() {
		return watermark;
	}

}
