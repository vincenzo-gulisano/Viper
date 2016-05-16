package usecases.linearroad;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import operator.merger.ViperMerger;
import operator.sink.Sink;
import operator.viperBolt.BoltFunction;
import operator.viperBolt.ViperBolt;
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
import backtype.storm.topology.BoltDeclarer;
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

		ViperTopologyBuilder builder = new ViperTopologyBuilder();

		// //////////////// SPOUT //////////////////////////

		builder.setSpout("spout", new ViperSpout(new LRSpout(input_data,
				spout_parallelism, duration), new Fields("lr_type", "lr_time",
				"lr_vid", "lr_speed", "lr_xway", "lr_lane", "lr_dir", "lr_seg",
				"lr_pos")), spout_parallelism);

		// //////////////// STATEFUL OPERATOR //////////////////////////

		class CheckNewSegment implements BoltFunction {

			private DetectNewVehicles detectNewVehicles;

			public List<Values> process(Tuple arg0) {
				List<Values> results = new ArrayList<Values>();
				if (arg0.getIntegerByField("lr_type") == 0) {
					boolean newVehicle = detectNewVehicles.isThisANewVehicle(
							arg0.getLongByField("lr_time"),
							arg0.getIntegerByField("lr_xway"),
							arg0.getIntegerByField("lr_seg"),
							arg0.getIntegerByField("lr_vid"));
					if (newVehicle)
						results.add(new Values(arg0
								.getIntegerByField("lr_type"), arg0
								.getLongByField("lr_time"), arg0
								.getIntegerByField("lr_vid"), arg0
								.getIntegerByField("lr_speed"), arg0
								.getIntegerByField("lr_xway"), arg0
								.getIntegerByField("lr_lane"), arg0
								.getIntegerByField("lr_dir"), arg0
								.getIntegerByField("lr_seg"), arg0
								.getIntegerByField("lr_pos"), newVehicle));
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

			@Override
			public List<Values> process(List<Object> v) {
				throw new RuntimeException("METHOD NOT IMPLEMENTED");
			}

			@Override
			public List<Values> receivedFlush(List<Object> v) {
				throw new RuntimeException("METHOD NOT IMPLEMENTED");
			}

		}

		BoltDeclarer op = builder.setBolt("op", new ViperBolt(new Fields(
				"lr_type", "lr_time", "lr_vid", "lr_speed", "lr_xway",
				"lr_lane", "lr_dir", "lr_seg", "lr_pos", "new_seg"),
				new CheckNewSegment()), op_parallelism);

		if (useOptimizedQueues) {

			op.customGrouping("spout", new ViperFieldsSharedChannels(logStats,
					statsPath, topologyName, 1, 2));

		} else {

			if (spout_parallelism == 1) {

				// In this case, no need for merger.
				op.fieldsGrouping("spout", new Fields("lr_vid"));

			} else {

				builder.setBolt(
						"op_merger",
						new ViperMerger(new Fields("lr_type", "lr_time",
								"lr_vid", "lr_speed", "lr_xway", "lr_lane",
								"lr_dir", "lr_seg", "lr_pos"), "lr_time"),
						op_parallelism).fieldsGrouping("spout",
						new Fields("lr_vid"));

				op.directGrouping("op_merger");

			}
		}

		// //////////////// SINK //////////////////////////

		if (useOptimizedQueues) {

			builder.setBolt("sink", new Sink(), sink_parallelism)
					.customGrouping(
							"op",
							new ViperShuffleSharedChannels(logStats, statsPath,
									topologyName, 1));

		} else {

			if (op_parallelism == 1) {

				// In this case, no need for merger.
				builder.setBolt("sink", new Sink(), sink_parallelism)
						.shuffleGrouping("op");

			} else {

				builder.setBolt(
						"sink_merger",
						new ViperMerger(new Fields("lr_type", "lr_time",
								"lr_vid", "lr_speed", "lr_xway", "lr_lane",
								"lr_dir", "lr_seg", "lr_pos", "new_seg"),
								"lr_time"), sink_parallelism).shuffleGrouping(
						"op");

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
