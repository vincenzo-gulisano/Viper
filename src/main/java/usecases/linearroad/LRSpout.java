package usecases.linearroad;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import operator.viperSpout.SpoutFunction;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

public class LRSpout implements SpoutFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7442216449985515402L;

	private long startTimestamp;
	private ArrayList<LRTuple> input_tuples;
	int index = 0;

	// Force time to increase even if we are looping on input tuples.
	long repetition = 0;
	long timeStep = 60 * 60 * 3;

	String input_data;
	int spout_parallelism;
	long duration;

	public LRSpout(String input_data, int spout_parallelism, long duration) {
		this.input_data = input_data;
		this.spout_parallelism = spout_parallelism;
		this.duration = duration;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		startTimestamp = System.currentTimeMillis();
		input_tuples = new ArrayList<LRTuple>();

		int taskIndex = context.getThisTaskIndex();

		// Read input data
		try {
			// Open the file
			FileInputStream fstream = new FileInputStream(input_data);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fstream));

			String strLine;

			// Read File Line By Line
			int lineNumber = 0;
			while ((strLine = br.readLine()) != null) {
				if (lineNumber % spout_parallelism == taskIndex) {
					LRTuple t = new LRTuple(strLine);
					input_tuples.add(t);
				}
				lineNumber++;
			}

			// Close the input stream
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public boolean hasNext() {
		return (System.currentTimeMillis() - startTimestamp) < duration * 1000;
	}

	@Override
	public Values getTuple() {

		LRTuple t = input_tuples.get(index);

		Values result = new Values(t.type, t.time + repetition * timeStep,
				t.vid, t.speed, t.xway, t.lane, t.dir, t.seg, t.pos);
		index = (index + 1) % input_tuples.size();

		// Force time to increase even if we are looping on input
		// tuples.
		if (index == 0)
			repetition++;

		return result;
	}

}
