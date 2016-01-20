package usecases.linearroad;

import java.util.ArrayList;
import java.util.HashMap;

class DetectNewVehicles {

	class SegmentInfo {
		public SegmentInfo(String segment, Long lastTimeUpdate) {
			this.segment = segment;
			this.lastTimeUpdate = lastTimeUpdate;
		}

		public String segment;
		public Long lastTimeUpdate;
	}

	public DetectNewVehicles() {
		previousSegments = new HashMap<Integer, DetectNewVehicles.SegmentInfo>();
	}

	private long previousTimestamp = -1;
	private HashMap<Integer, SegmentInfo> previousSegments;
	private static int count = 0;

	public boolean isThisANewVehicle(long time, int xway, int seg, int vid) {

		if (previousTimestamp != -1 && time < previousTimestamp) {
			throw new RuntimeException(
					"Incoming tuple timestamp has decreased!!!");
		}
		previousTimestamp = time;

		String csegment = "" + xway + "_" + seg;
		boolean isnew = false;
		if (previousSegments.containsKey(vid)) {
			if (!previousSegments.get(vid).segment.equals(csegment)) {
				isnew = true;
			}
		} else {
			isnew = true;
		}
		previousSegments.put(vid, new SegmentInfo(csegment, time));

		if (count == 50) {
			count = 0;
			ArrayList<Integer> toRemove = new ArrayList<Integer>();
			for (Integer vt : this.previousSegments.keySet()) {
				if (time - this.previousSegments.get(vt).lastTimeUpdate > 31) {
					toRemove.add(vt);
				}
			}
			for (Integer vt : toRemove)
				previousSegments.remove(vt);
		}
		return isnew;
	}

}