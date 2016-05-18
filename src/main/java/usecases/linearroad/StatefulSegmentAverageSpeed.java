package usecases.linearroad;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import operator.aggregate.AggregateWindow;
import operator.aggregate.ViperAggregate;
import operator.merger.ViperMerger;
import operator.sink.Sink;
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

public class StatefulSegmentAverageSpeed {

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
				spout_parallelism, duration, true), new Fields("lr_type",
				"lr_time", "lr_vid", "lr_speed", "lr_xway", "lr_lane",
				"lr_dir", "lr_seg", "lr_pos")), spout_parallelism);

		// //////////////// STATEFUL OPERATOR //////////////////////////

		class SegmentAverageSpeed implements AggregateWindow, Serializable {

			double speedSum = 0;
			double speedCount = 0;

			@Override
			public AggregateWindow factory() {
				return new SegmentAverageSpeed();
			}

			@SuppressWarnings("rawtypes")
			@Override
			public void prepare(Map stormConf, TopologyContext context) {

			}

			@Override
			public void update(Tuple t) {
				speedCount++;
				speedSum += t.getIntegerByField("lr_speed");
			}

			@Override
			public List<Object> getAggregatedResult(long ts, String key) {
				double avg = speedCount > 0 ? speedSum / speedCount : -1;
				return new Values(ts, key, avg);
			}

		}

		BoltDeclarer op = builder.setBolt("op",
				new ViperAggregate<SegmentAverageSpeed>(new Fields("lr_time",
						"lr_seg", "lr_lav"), "lr_time", "lr_seg", 300, 60,
						new SegmentAverageSpeed()), op_parallelism);

		if (useOptimizedQueues) {

			op.customGrouping("spout", new ViperFieldsSharedChannels(logStats,
					statsPath, topologyName, 1, 7));

		} else {

			if (spout_parallelism == 1) {

				// In this case, no need for merger.
				op.fieldsGrouping("spout", new Fields("lr_seg"));

			} else {

				builder.setBolt(
						"op_merger",
						new ViperMerger(new Fields("lr_type", "lr_time",
								"lr_vid", "lr_speed", "lr_xway", "lr_lane",
								"lr_dir", "lr_seg", "lr_pos"), "lr_time"),
						op_parallelism).fieldsGrouping("spout",
						new Fields("lr_seg"));

				op.directGrouping("op_merger");

			}
		}

		// //////////////// SINK //////////////////////////

		if (useOptimizedQueues) {

			builder.setBolt("sink", new Sink(), sink_parallelism)
					.customGrouping(
							"op",
							new ViperShuffleSharedChannels(logStats, statsPath,
									topologyName, 0));

		} else {

			if (op_parallelism == 1) {

				// In this case, no need for merger.
				builder.setBolt("sink", new Sink(), sink_parallelism)
						.shuffleGrouping("op");

			} else {

				builder.setBolt(
						"sink_merger",
						new ViperMerger(new Fields("lr_time", "lr_seg",
								"lr_lav"), "lr_time"), sink_parallelism)
						.shuffleGrouping("op");

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
