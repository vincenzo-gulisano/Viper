package usecases.linearroad;

import java.util.ArrayList;
import java.util.List;

import operator.sink.SinkFunction;
import operator.viperBolt.BoltFunctionBase;
import operator.viperSpout.InternalQueuesShuffle;
import operator.viperSpout.ViperSpoutBoltWrapper;
import operator.viperSpout.ViperSpoutInternalOnly;
import topology.ViperTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PrototypeStatelessForwardStoppedCarsOnly {

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

		ViperTopologyBuilder builder = new ViperTopologyBuilder();

		// //////////////// SPOUT //////////////////////////

		class SpoutShuffle extends InternalQueuesShuffle {

			int index = 0;
			String id_suffix = "";

			public SpoutShuffle(boolean keepStats, String statsPath,
					String topologyName) {
				super(keepStats, statsPath, topologyName);
				for (int i = 0; i < spout_parallelism; i++) {
					id_suffix += "_spout." + i;
				}
			}

			@Override
			public long getTS(List<Object> v) {
				return (Long) v.get(1);
			}

			@Override
			public String getChannelID(List<Object> v) {
				String result = "op." + index + id_suffix;
				index = (index + 1) % op_parallelism;
				return result;
			}

		}

		builder.setSpout("spout", new ViperSpoutInternalOnly(new LRSpout(
				input_data, spout_parallelism, duration), new Fields("lr_type",
				"lr_time", "lr_vid", "lr_speed", "lr_xway", "lr_lane",
				"lr_dir", "lr_seg", "lr_pos"), new SpoutShuffle(logStats,
				statsPath, topologyName)), spout_parallelism);

		// //////////////// OPERATOR //////////////////////////

		class Filter extends BoltFunctionBase {

			@Override
			public List<Values> process(Tuple arg0) {
				throw new RuntimeException("METHOD NOT IMPLEMENTED");

			}

			@Override
			// TODO This is extremely ugly!!!
			public List<Values> process(List<Object> v) {
				List<Values> results = new ArrayList<Values>();
				if ((Integer) v.get(2) == 0 && (Integer) v.get(5) == 0)
					results.add(new Values(v.subList(2, v.size())));
				return results;
			}

			@Override
			public List<Values> receivedFlush(List<Object> v) {
				throw new RuntimeException("METHOD NOT IMPLEMENTED");
			}

		}

		class OpShuffle extends InternalQueuesShuffle {

			int index = 0;
			String id_suffix = "";

			public OpShuffle(boolean keepStats, String statsPath,
					String topologyName) {
				super(keepStats, statsPath, topologyName);
				for (int i = 0; i < op_parallelism; i++) {
					id_suffix += "_op." + i;
				}
			}

			@Override
			public long getTS(List<Object> v) {
				return (Long) v.get(1);
			}

			@Override
			public String getChannelID(List<Object> v) {
				String result = "sink." + index + id_suffix;
				index = (index + 1) % sink_parallelism;
				return result;
			}

		}
		builder.setSpout("op", new ViperSpoutBoltWrapper(new Fields("lr_type",
				"lr_time", "lr_vid", "lr_speed", "lr_xway", "lr_lane",
				"lr_dir", "lr_seg", "lr_pos"), new Filter(), new OpShuffle(
				logStats, statsPath, topologyName), "spout"), op_parallelism);

		// //////////////// SINK //////////////////////////

		builder.setSpout("sink", new ViperSpoutBoltWrapper(new Fields(),
				new SinkFunction(), null, "op"), sink_parallelism);

		// //////////////// CONFIGURATION //////////////////////////

		Config conf = new Config();
		conf.setDebug(false);

		conf.put("log.statistics", logStats);
		conf.put("log.statistics.path", statsPath);
		conf.put("merger.type", "MergerScaleGate");
		conf.put("topology.spout.wait.strategy",
				"operator.viperSpout.NoSleepSpoutWaitStrategy");

		if (!local) {
			conf.setNumWorkers(1);
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
