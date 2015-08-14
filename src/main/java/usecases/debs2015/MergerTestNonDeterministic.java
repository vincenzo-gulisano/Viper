package usecases.debs2015;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import operator.sink.Sink;
import operator.viperBolt.BoltFunctionBase;
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

public class MergerTestNonDeterministic {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		boolean local = Boolean.valueOf(args[0]);
		boolean logStats = Boolean.valueOf(args[1]);
		String statsPath = args[2];
		final int op_parallelism = Integer.valueOf(args[3]);
		String topologyName = args[4];
		final long duration = Long.valueOf(args[5]);
		final double selectivity = Double.valueOf(args[6]);

		ViperTopologyBuilder builder = new ViperTopologyBuilder();

		builder.setSpout("spout", new ViperSpout(new SpoutFunction() {

			private long startTimestamp;
			private Random rand;

			@SuppressWarnings("rawtypes")
			@Override
			public void prepare(Map stormConf, TopologyContext context) {
				startTimestamp = System.currentTimeMillis();
				rand = new Random();
			}

			@Override
			public boolean hasNext() {
				return (System.currentTimeMillis() - startTimestamp) < duration * 1000;
			}

			@Override
			public Values getTuple() {
				return new Values(rand.nextInt(), rand.nextInt(), rand
						.nextInt());
			}
		}, new Fields("x", "y", "z")), 1);

		builder.setBolt(
				"op",
				new ViperBolt(new Fields("x", "y", "z"),
						new BoltFunctionBase() {

							private Random rand;

							@SuppressWarnings("rawtypes")
							@Override
							public void prepare(Map stormConf,
									TopologyContext context) {
								rand = new Random();
								super.prepare(stormConf, context);
							}

							@Override
							public List<Values> process(Tuple t) {
								List<Values> result = new ArrayList<Values>();
								if (rand.nextDouble() < selectivity) {
									result.add(new Values(t
											.getIntegerByField("x"), t
											.getIntegerByField("y"), t
											.getIntegerByField("z")));
								}
								return result;
							}
						}), op_parallelism).shuffleGrouping("spout");

		builder.setBolt("sink", new Sink(), 1).shuffleGrouping("op");

		Config conf = new Config();
		conf.setDebug(false);

		conf.put("log.statistics", logStats);
		conf.put("log.statistics.path", statsPath);

		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

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
