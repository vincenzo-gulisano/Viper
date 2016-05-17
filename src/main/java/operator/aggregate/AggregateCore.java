package operator.aggregate;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import operator.viperBolt.BoltFunction;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import core.ViperValues;

public class AggregateCore<T extends AggregateWindow> implements BoltFunction {

	// TODO Check this
	/**
	 * This class implements the time-based sliding window Aggregate. The
	 * following assumption holds,
	 * 
	 * Input tuples (TridentTuple) contain a field <timestampFieldID> of type
	 * long (timestamp)
	 * 
	 * Input tuples (TridentTuple) contain a field <groupbyFieldID> of type
	 * String (if groupbyFieldID is not "")
	 * 
	 * The group-by parameter of the aggregate operator is defined by exactly
	 * one field
	 * 
	 * The windowSize and windowAdvance parameters have the same time units of
	 * the timestamp field
	 * 
	 * Input tuples' timestamps are non-decreasing!
	 */

	// USER PARAMETERS
	private String timestampFieldID;
	private String groupbyFieldID;
	private long windowSize;
	private long windowAdvance;
	private T aggregateWindow;

	// OTHERS
	private long earliestTimestamp;
	private long latestTimestamp;
	private boolean firstTuple = true;
	private HashMap<Long, HashMap<String, T>> windows;

	private static final long serialVersionUID = 5553102377724551120L;

	@SuppressWarnings("rawtypes")
	Map stormConf;
	TopologyContext context;

	public AggregateCore(String timestampFieldID, String groupbyFieldID,
			long windowSize, long windowAdvance, T aggregateWindow) {
		windows = new HashMap<Long, HashMap<String, T>>();
		this.timestampFieldID = timestampFieldID;
		this.groupbyFieldID = groupbyFieldID;
		this.windowSize = windowSize;
		this.windowAdvance = windowAdvance;
		this.aggregateWindow = aggregateWindow;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		this.stormConf = stormConf;
		this.context = context;
	}

	public static List<Long> getWindowsStartTimestamps(long timestamp,
			long windowSize, long windowAdvance) {

		LinkedList<Long> result = new LinkedList<Long>();

		long windowStart = (timestamp / windowAdvance) * windowAdvance;
		result.add(windowStart);
		while (windowStart - windowAdvance + windowSize > timestamp
				&& windowStart - windowAdvance >= 0) {
			windowStart -= windowAdvance;
			result.addFirst(windowStart);
		}

		return result;

	}

	@Override
	public List<Values> process(Tuple t) {

		List<Values> result = new LinkedList<Values>();

		// Take timestamp and make sure it has not decreased
		long timestamp = t.getLongByField(timestampFieldID);
		if (firstTuple) {
			firstTuple = false;
		} else {
			if (timestamp < latestTimestamp) {
				throw new RuntimeException("Input tuple's timestamp decreased!");
			}
		}
		latestTimestamp = timestamp;

		// Take the group-by
		String groupby = "";
		if (!groupbyFieldID.equals(""))
			groupby = String.valueOf(t.getValueByField(groupbyFieldID));

		// Purge stale windows
		List<Long> windowsStart = getWindowsStartTimestamps(timestamp,
				this.windowSize, this.windowAdvance);

		long firstWindowStart = windowsStart.get(0);

		while (earliestTimestamp < firstWindowStart) {
			if (windows.containsKey(earliestTimestamp)) {
				for (Entry<String, T> entry : windows.get(earliestTimestamp)
						.entrySet()) {
					String entryGroupby = entry.getKey();
					T window = entry.getValue();
					// TODO should add here entryGroupby and timestamp before
					// the
					// values
					result.add(new ViperValues(window.getAggregatedResult(
							earliestTimestamp, entryGroupby)));
				}
				windows.remove(earliestTimestamp);
			}
			earliestTimestamp += windowAdvance;
		}

		// Update active windows
		for (Long windowStart : windowsStart) {
			if (!windows.containsKey(windowStart)) {
				windows.put(windowStart, new HashMap<String, T>());
			}
			if (!windows.get(windowStart).containsKey(groupby)) {
				@SuppressWarnings("unchecked")
				T window = (T) aggregateWindow.factory();
				window.prepare(stormConf, context);
				windows.get(windowStart).put(groupby, window);
			}
			windows.get(windowStart).get(groupby).update(t);
		}

		// Done...
		return result;

	}

	@Override
	public List<Values> receivedFlush(Tuple t) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Values> process(List<Object> v) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Values> receivedFlush(List<Object> v) {
		// TODO Auto-generated method stub
		return null;
	}

}
