package scalegate;

import operator.merger.MergerEntry;

public class SGTupleContainer implements SGTuple {

	private MergerEntry me;
	private boolean isFake;

	public SGTupleContainer(MergerEntry me) {
		this.me = me;
		isFake = false;
	}

	public SGTupleContainer() {
		me = null;
		isFake = true;
	}

	@Override
	public int compareTo(SGTuple o) {
		if (getTS() == o.getTS()) {
			return 0;
		} else {
			return getTS() > o.getTS() ? 1 : -1;
		}
	}

	@Override
	public long getTS() {
		if (isFake)
			return 0;
		return me.getTs();
	}

	public MergerEntry getME() {
		return me;
	}

	public boolean isFake() {
		return isFake;
	}

	@Override
	public boolean isWM() {
		return false;
	}

	@Override
	public long getWM() {
		return -1;
	}
}
