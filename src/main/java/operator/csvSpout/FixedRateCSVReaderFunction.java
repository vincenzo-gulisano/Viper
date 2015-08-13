package operator.csvSpout;

import java.util.Map;

import operator.viperSpout.SpoutFunction;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FixedRateCSVReaderFunction implements SpoutFunction {

	private static final long serialVersionUID = 2612121254607940790L;
	private CSVFileReader reader;
	private String filePath;

	boolean firstInvocation = true;
	long firstTimestamp;
	long prevTimestamp;
	long desiredRate;
	long checkRateThreshold = 1000;
	long counter = 0;
	long sleepPeriod;
	long maxDuration;
	boolean hasNext;

	public FixedRateCSVReaderFunction(CSVFileReader reader, long desiredRate,
			long maxDuration) {
		this.reader = reader;
		this.desiredRate = desiredRate;
		this.maxDuration = maxDuration;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		filePath = (String) stormConf.get(context.getThisComponentId() + "."
				+ context.getThisTaskIndex() + ".filepath");
		reader.setup(filePath);
		hasNext = reader.hasNext();
		sleepPeriod = 1000 / (desiredRate / checkRateThreshold);
	}

	@Override
	public Values getTuple() {

		if (firstInvocation) {
			firstInvocation = false;
			firstTimestamp = System.currentTimeMillis();
			prevTimestamp = System.currentTimeMillis();
		}

		if (counter == checkRateThreshold) {
			Utils.sleep(sleepPeriod
					- (System.currentTimeMillis() - prevTimestamp));
			prevTimestamp = System.currentTimeMillis();
			counter = 0;
		}
		counter++;

		hasNext = reader.hasNext();
		if (hasNext
				&& (System.currentTimeMillis() - firstTimestamp) > maxDuration * 1000) {
			hasNext = false;
		}

		return reader.getNextTuple();
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

}
