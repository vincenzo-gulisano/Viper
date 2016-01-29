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

import operator.csvSink.CSVFileWriter;
import operator.csvSink.CSVSink;
import operator.merger.ViperMerger;
import operator.sink.Sink;
import operator.viperBolt.BoltFunction;
import operator.viperBolt.ViperBolt;
import operator.viperSpout.SpoutFunction;
import operator.viperSpout.ViperSpout;
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

public class StatefulVehicleEnteringNewSegment {

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
//			long counter = 0;

			// Force time to increase even if we are looping on input tuples.
			long repetition = 0;
			long timeStep = 60 * 60 * 3;

			@SuppressWarnings("rawtypes")
			@Override
			public void prepare(Map stormConf, TopologyContext context) {

				startTimestamp = System.currentTimeMillis();
				input_tuples = new ArrayList<LRTuple>();

				int taskIndex = context.getThisTaskIndex();
				// System.out.println(taskIndex);

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

				// Force time to increase even if we are looping on input
				// tuples.
				// t.time += repetition * timeStep;

				Values result = new Values(t.type, t.time + repetition
						* timeStep, t.vid, t.speed, t.xway, t.lane, t.dir,
						t.seg, t.pos);
				index = (index + 1) % input_tuples.size();

				// Force time to increase even if we are looping on input
				// tuples.
				if (index == 0)
					repetition++;
				// System.out.println("Spout " + index + " adding " + result);
				// Utils.sleep(100);
//				if (counter % 100 == 0)
//					Utils.sleep(1);

//				counter++;
				return result;
			}

		}, new Fields("lr_type", "lr_time", "lr_vid", "lr_speed", "lr_xway",
				"lr_lane", "lr_dir", "lr_seg", "lr_pos")), spout_parallelism);

		// //////////////// STATEFUL OPERATOR //////////////////////////

		class CheckNewSegment implements BoltFunction {

			private DetectNewVehicles detectNewVehicles;

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
									.getIntegerByField("lr_pos"),
							detectNewVehicles.isThisANewVehicle(
									arg0.getLongByField("lr_time"),
									arg0.getIntegerByField("lr_xway"),
									arg0.getIntegerByField("lr_seg"),
									arg0.getIntegerByField("lr_vid"))));
					// System.out.println(results);

				}
				return results;
			}

			@SuppressWarnings("rawtypes")
			public void prepare(Map arg0, TopologyContext arg1) {
				detectNewVehicles = new DetectNewVehicles();
			}

			public List<Values> receivedFlush(Tuple arg0) {
				return new ArrayList<Values>();
			}

		}

		/*
		 * Now the tricky part First of all, check if the previous operator has
		 * at least two instances, otherwise no need for merger
		 */

		if (useOptimizedQueues) {
			throw new RuntimeException(
					"Optimized queues for stateless not ready yet!");
		}

		if (spout_parallelism == 1) {

			// In this case, no need for merger.
			builder.setBolt(
					"op",
					new ViperBolt(new Fields("lr_type", "lr_time", "lr_vid",
							"lr_speed", "lr_xway", "lr_lane", "lr_dir",
							"lr_seg", "lr_pos", "new_seg"),
							new CheckNewSegment()), op_parallelism)
					.fieldsGrouping("spout", new Fields("lr_vid"));

		} else if (spout_parallelism > 1) {

			builder.setBolt(
					"op_merger",
					new ViperMerger(new Fields("lr_type", "lr_time", "lr_vid",
							"lr_speed", "lr_xway", "lr_lane", "lr_dir",
							"lr_seg", "lr_pos"), "lr_time"), op_parallelism)
					.fieldsGrouping("spout", new Fields("lr_vid"));

			builder.setBolt(
					"op",
					new ViperBolt(new Fields("lr_type", "lr_time", "lr_vid",
							"lr_speed", "lr_xway", "lr_lane", "lr_dir",
							"lr_seg", "lr_pos", "new_seg"),
							new CheckNewSegment()), op_parallelism)
					.directGrouping("op_merger");

		} else {
			throw new RuntimeException(
					"Spout parallelism seems to be negative...");
		}

		// //////////////// SINK //////////////////////////

		/*
		 * Now the tricky part First of all, check if the previous operator has
		 * at least two instances, otherwise no need for merger
		 */

		if (useOptimizedQueues) {
			throw new RuntimeException(
					"Optimized queues for stateless not ready yet!");
		}

		if (op_parallelism == 1) {

			// In this case, no need for merger.

			if (logOut) {
				builder.setBolt("sink", new CSVSink(new CSVFileWriter() {

					@Override
					protected String convertTupleToLine(Tuple t) {
						return t.getIntegerByField("lr_type") + ";"
								+ t.getLongByField("lr_time") + ";"
								+ t.getIntegerByField("lr_vid") + ";"
								+ t.getIntegerByField("lr_speed") + ";"
								+ t.getIntegerByField("lr_xway") + ";"
								+ t.getIntegerByField("lr_lane") + ";"
								+ t.getIntegerByField("lr_dir") + ";"
								+ t.getIntegerByField("lr_seg") + ";"
								+ t.getIntegerByField("lr_pos") + ";"
								+ t.getBooleanByField("new_seg");
					}

				}), sink_parallelism).shuffleGrouping("op");
			} else {
				builder.setBolt("sink", new Sink(), sink_parallelism)
						.shuffleGrouping("op");
			}

		} else if (op_parallelism > 1) {

			builder.setBolt(
					"sink_merger",
					new ViperMerger(new Fields("lr_type", "lr_time", "lr_vid",
							"lr_speed", "lr_xway", "lr_lane", "lr_dir",
							"lr_seg", "lr_pos", "new_seg"), "lr_time"),
					sink_parallelism).shuffleGrouping("op");

			if (logOut) {
				builder.setBolt("sink", new CSVSink(new CSVFileWriter() {

					@Override
					protected String convertTupleToLine(Tuple t) {
						return t.getIntegerByField("lr_type") + ";"
								+ t.getLongByField("lr_time") + ";"
								+ t.getIntegerByField("lr_vid") + ";"
								+ t.getIntegerByField("lr_speed") + ";"
								+ t.getIntegerByField("lr_xway") + ";"
								+ t.getIntegerByField("lr_lane") + ";"
								+ t.getIntegerByField("lr_dir") + ";"
								+ t.getIntegerByField("lr_seg") + ";"
								+ t.getIntegerByField("lr_pos") + ";"
								+ t.getBooleanByField("new_seg");
					}

				}), sink_parallelism).directGrouping("sink_merger");
			} else {
				builder.setBolt("sink", new Sink(), sink_parallelism)
						.directGrouping("sink_merger");
			}

		} else {
			throw new RuntimeException(
					"Operator parallelism seems to be negative...");
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
