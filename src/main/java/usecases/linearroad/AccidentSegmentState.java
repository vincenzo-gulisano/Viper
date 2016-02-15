package usecases.linearroad;

import java.util.ArrayList;
import java.util.HashMap;

public class AccidentSegmentState {

	// state for one segment with information about accidents...
	private boolean hasAccident;
	private boolean isNewAccident;
	private boolean isCleared;
	private long timeNew;
	private long timeCleared;
	// private boolean isNewCleared;
	private HashMap<Integer, Vehicle> vehicles;
	private ArrayList<Vehicle> stopped;
	private HashMap<Integer, ArrayList<Vehicle>> accidents;

	public AccidentSegmentState() {
		vehicles = new HashMap<Integer, Vehicle>();
		stopped = new ArrayList<Vehicle>();
		accidents = new HashMap<Integer, ArrayList<Vehicle>>();
	}

	@Override
	public String toString() {
		String res = "accident:" + this.hasAccident + " is new:"
				+ this.isNewAccident + " is cleared:" + this.isCleared
				+ " timeNew:" + this.timeNew + " timeCleared:"
				+ this.timeCleared;
		for (Vehicle v : stopped) {
			res += " " + v.id;
		}
		res += " all vehicles: " + vehicles.size();
		return res;
	}

	public Vehicle addVehicle(int vid, long time, int xway, int lane,
			int segment, int position, int speed) {
		Vehicle v;
		if (vehicles.get(vid) == null) {
			v = new Vehicle(vid, time, xway, lane, segment, position);
			vehicles.put(vid, v);
		} else {
			v = vehicles.get(vid);
			v.update(position, xway, segment, lane, speed, time);
		}
		return v;

	}

	public class UpdateSegmentAnswer {
		public boolean cleared;
		public boolean newacc;

		public UpdateSegmentAnswer(boolean cleared, boolean newacc) {
			this.cleared = cleared;
			this.newacc = newacc;
		}
	}

	public UpdateSegmentAnswer updateSegmentState(int vid, long time, int xway,
			int lane, int segment, int position, int speed) {
		Vehicle v = this.addVehicle(vid, time, xway, lane, segment, position,
				speed);
		boolean cleared = false;
		boolean newacc = false;
		if (v.stopped) {
			// check if it is stopped in the same position as the other vehicles
			System.out.println("Vehicle " + v + " is stopped!");
			newacc = addStoppedVehicle(v);
		} else {
			cleared = addRunningVehicle(v);
		}
		return new UpdateSegmentAnswer(cleared, newacc);
	}

	private boolean addRunningVehicle(Vehicle v) {
		boolean cleared = false;
		if (stopped.size() == 0) {
			return cleared;
		}
		if (stopped.contains(v)) {
			// vehicle was stopped and is not anymore
			System.out.println("Removing vehicle " + v + " from stopped");
			stopped.remove(v);
			// we have at least one accident
			System.out.println("this.accidents.size(): "
					+ this.accidents.size());
			if (this.accidents.size() != 0) {
				System.out.println("this.accidents.containsKey(v.pos): "
						+ this.accidents.containsKey(v.pos));
				if (this.accidents.containsKey(v.pos)) {
					ArrayList<Vehicle> vlist = this.accidents.get(v.pos);
					vlist.remove(v);
					if (vlist.size() <= 1) {
						this.accidents.remove(v.pos);
						if (this.accidents.size() == 0) {
							this.isCleared = true;
							cleared = true;
							this.timeCleared = v.time;
							this.timeNew = -1;
						}
					}
				}
			}
		}
		return cleared;
	}

	private boolean addStoppedVehicle(Vehicle v) {

		if (!this.stopped.contains(v) && this.stopped.size() == 0) {
			this.stopped.add(v);
			return false;
		}

		boolean newacc = false;
		for (Vehicle elem : this.stopped) {
			if (elem.id == v.id)
				continue;
			if (v.pos == elem.pos && v.lane == elem.lane
					&& (v.time - elem.time) <= 120 && v.xway == elem.xway) {
				// if accident position is not in array of accidents
				if (this.accidents.size() == 0) {
					newacc = true;
					this.isNewAccident = true;
					this.timeNew = v.time;
				}
				if (this.accidents.containsKey(v.pos)) {
					// add this vehicle as involved in accident?
					this.accidents.get(v.pos).add(v);
				} else {
					ArrayList<Vehicle> vids = new ArrayList<Vehicle>();
					vids.add(v);
					vids.add(elem);
					System.out.println("Adding accident for position " + v.pos);
					this.accidents.put(v.pos, vids);
				}
			}
			break;
		}
		if (!stopped.contains(v))
			stopped.add(v);

		return newacc;
	}

}