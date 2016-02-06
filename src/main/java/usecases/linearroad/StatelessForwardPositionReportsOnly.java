package usecases.linearroad;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import operator.merger.ViperMerger;
import operator.sink.Sink;
import operator.viperBolt.BoltFunction;
import operator.viperBolt.ViperBolt;
import operator.viperSpout.SpoutFunction;
import operator.viperSpout.ViperSpout;
import topology.ViperFieldsSharedChannels;
import topology.ViperShuffleSharedChannels;
import topology.ViperTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StatelessForwardPositionReportsOnly {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		boolean local = Boolean.valueOf(args[0]);
		boolean logStats = Boolean.valueOf(args[1]);
		String statsPath = args[2];
		String topologyName = args[3];
		final long duration = Long.valueOf(args[4]);
		final int spout_parallelism = Integer.valueOf(args[5]);
		final int op_parallelism = Integer.valueOf(args[6]);
		final int sink_parallelism = Integer.valueOf(args[7]);
		final String input_data = args[8];

		boolean useOptimizedQueues = Boolean.valueOf(args[9]);
		final int workers = Integer.valueOf(args[10]);

		boolean logOut = false; // Boolean.valueOf(args[11]);

		ViperTopologyBuilder builder = new ViperTopologyBuilder();

		// //////////////// SPOUT //////////////////////////

		builder.setSpout("spout", new ViperSpout(new SpoutFunction() {

			private long startTimestamp;
			private ArrayList<LRTuple> input_tuples;
			int index = 0;
			// long counter = 0;

			// Force time to increase even if we are looping on input tuples.
			long repetition = 0;
			long timeStep = 60 * 60 * 3;

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
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fstream));

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

				Values result = new Values(t.type, t.time + repetition
						* timeStep, t.vid, t.speed, t.xway, t.lane, t.dir,
						t.seg, t.pos);
				index = (index + 1) % input_tuples.size();

				// Force time to increase even if we are looping on input
				// tuples.
				if (index == 0)
					repetition++;

				return result;
			}

		}, new Fields("lr_type", "lr_time", "lr_vid", "lr_speed", "lr_xway",
				"lr_lane", "lr_dir", "lr_seg", "lr_pos")), spout_parallelism);

		// //////////////// STATEFUL OPERATOR //////////////////////////

		class CheckNewSegment implements BoltFunction {

			public List<Values> process(Tuple arg0) {
				List<Values> results = new ArrayList<Values>();
				if (arg0.getIntegerByField("lr_type") == 0) {
					results.add(new Values(arg0.getIntegerByField("lr_type"),
							arg0.getLongByField("lr_time"), arg0
									.getIntegerByField("lr_vid"), arg0
									.getIntegerByField("lr_speed"), arg0
									.getIntegerByField("lr_xway"), arg0
									.getIntegerByField("lr_lane"), arg0
									.getIntegerByField("lr_dir"), arg0
									.getIntegerByField("lr_seg"), arg0
									.getIntegerByField("lr_pos")));
				}
				return results;
			}

			@SuppressWarnings("rawtypes")
			public void prepare(Map arg0, TopologyContext arg1) {
			}

			public List<Values> receivedFlush(Tuple arg0) {
				return new ArrayList<Values>();
			}

		}

		if (useOptimizedQueues) {

			builder.setBolt(
					"op",
					new ViperBolt(new Fields("lr_type", "lr_time", "lr_vid",
							"lr_speed", "lr_xway", "lr_lane", "lr_dir",
							"lr_seg", "lr_pos"), new CheckNewSegment()),
					op_parallelism).customGrouping(
					"spout",
					new ViperFieldsSharedChannels(logStats, statsPath,
							topologyName, 1, 2));

		} else {

			if (spout_parallelism == 1) {

				// In this case, no need for merger.
				builder.setBolt(
						"op",
						new ViperBolt(new Fields("lr_type", "lr_time",
								"lr_vid", "lr_speed", "lr_xway", "lr_lane",
								"lr_dir", "lr_seg", "lr_pos"),
								new CheckNewSegment()), op_parallelism)
						.fieldsGrouping("spout", new Fields("lr_vid"));

			} else if (spout_parallelism > 1) {

				builder.setBolt(
						"op_merger",
						new ViperMerger(new Fields("lr_type", "lr_time",
								"lr_vid", "lr_speed", "lr_xway", "lr_lane",
								"lr_dir", "lr_seg", "lr_pos"), "lr_time"),
						op_parallelism).fieldsGrouping("spout",
						new Fields("lr_vid"));

				builder.setBolt(
						"op",
						new ViperBolt(new Fields("lr_type", "lr_time",
								"lr_vid", "lr_speed", "lr_xway", "lr_lane",
								"lr_dir", "lr_seg", "lr_pos"),
								new CheckNewSegment()), op_parallelism)
						.directGrouping("op_merger");

			}

		}

		// //////////////// SINK //////////////////////////

		/*
		 * Now the tricky part First of all, check if the previous operator has
		 * at least two instances, otherwise no need for merger
		 */

		if (useOptimizedQueues) {

			if (op_parallelism == 1) {

				builder.setBolt("sink", new Sink(), sink_parallelism)
						.shuffleGrouping("op");

			} else if (op_parallelism > 1) {

				builder.setBolt("sink", new Sink(), sink_parallelism)
						.customGrouping(
								"op",
								new ViperShuffleSharedChannels(logStats,
										statsPath, topologyName, 1));

			}

		} else {

			if (op_parallelism == 1) {

				builder.setBolt("sink", new Sink(), sink_parallelism)
						.shuffleGrouping("op");

			} else if (op_parallelism > 1) {

				builder.setBolt(
						"sink_merger",
						new ViperMerger(new Fields("lr_type", "lr_time",
								"lr_vid", "lr_speed", "lr_xway", "lr_lane",
								"lr_dir", "lr_seg", "lr_pos"), "lr_time"),
						sink_parallelism).shuffleGrouping("op");

				builder.setBolt("sink", new Sink(), sink_parallelism)
						.directGrouping("sink_merger");

			}
		}

		// //////////////// CONFIGURATION //////////////////////////

		Config conf = new Config();
		conf.setDebug(false);

		conf.put("log.statistics", logStats);
		conf.put("log.statistics.path", statsPath);
		conf.put("merger.type", "MergerScaleGate");

		if (logOut) {
			for (int i = 0; i < sink_parallelism; i++) {
				conf.put("sink." + i + ".filepath", statsPath + File.separator
						+ topologyName + "_out" + i + ".csv");
			}
		}

		conf.put("internal.queues", useOptimizedQueues);

		if (!local) {
			conf.setNumWorkers(workers);
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());

			Thread.sleep(600000);

			cluster.shutdown();
		}

	}
}
