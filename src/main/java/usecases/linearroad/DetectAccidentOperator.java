package usecases.linearroad;

import java.io.Serializable;
import java.util.HashMap;

public class DetectAccidentOperator implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1447389627327330715L;
	
	private HashMap<Integer, Vehicle> vehicles;
	private HashMap<Integer, Vehicle> stoppedVehicles;
	private HashMap<Integer, HashMap<Integer, Vehicle>> possibleAccidentsPositions;

	public DetectAccidentOperator() {
		vehicles = new HashMap<Integer, Vehicle>();
		stoppedVehicles = new HashMap<Integer, Vehicle>();
		possibleAccidentsPositions = new HashMap<Integer, HashMap<Integer, Vehicle>>();
	}

	public class UpdateSegmentAnswer {
		public boolean cleared;
		public boolean newacc;

		public UpdateSegmentAnswer(boolean cleared, boolean newacc) {
			this.cleared = cleared;
			this.newacc = newacc;
		}
	}

	public UpdateSegmentAnswer run(int xway, int segment, long time, int vid,
			int speed, int lane, int position) {

		boolean newAcc = false;
		boolean accCleared = false;

		Vehicle v;
		int prevPos = -1;
		if (vehicles.get(vid) == null) {
			v = new Vehicle(vid, time, xway, lane, segment, position);
			vehicles.put(vid, v);
		} else {
			v = vehicles.get(vid);
			prevPos = v.pos;
			v.update(position, xway, segment, lane, speed, time);
		}

		if (v.stopped) {
			//System.out.println("Vehicle " + v + " is stopped!");
			stoppedVehicles.put(v.id, v);

			if (!possibleAccidentsPositions.containsKey(v.pos)) {
				possibleAccidentsPositions.put(v.pos,
						new HashMap<Integer, Vehicle>());
				possibleAccidentsPositions.get(v.pos).put(v.id, v);
			} else if (possibleAccidentsPositions.containsKey(v.pos)
					&& !possibleAccidentsPositions.get(v.pos).containsKey(v.id)) {
				possibleAccidentsPositions.get(v.pos).put(v.id, v);
				//System.out.println("Accident at position " + v.pos);
				newAcc = true;
			}

		} else if (!v.stopped && stoppedVehicles.containsKey(v.id)) {
//			System.out.println("Vehicle " + v + " is no longer stopped!");
			stoppedVehicles.remove(v.id);
			possibleAccidentsPositions.get(prevPos).remove(v.id);
			if (possibleAccidentsPositions.get(prevPos).size() == 1) {
//				System.out.println("Accident at position " + prevPos
//						+ " cleared!");
				accCleared = true;
			}
			if (possibleAccidentsPositions.get(prevPos).size() == 0) {
//				System.out.println("No stopped cars at position " + prevPos);
				possibleAccidentsPositions.remove(prevPos);
			}
		}

		return new UpdateSegmentAnswer(accCleared, newAcc);

	}
}