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
import topology.ViperShuffle;
import topology.ViperTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class DummyStormNondeterministic {

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
		final double selectivity = Double.valueOf(args[8]);

		final int workers = Integer.valueOf(args[9]);

		ViperTopologyBuilder builder = new ViperTopologyBuilder();

		builder.setSpout("spout", new ViperSpout(new SpoutFunction() {

			private long startTimestamp;
			private Random rand;
			private long counter = 0;

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
				counter++;
				if (counter%1000==0)
					Utils.sleep(1);
				return new Values(rand.nextInt(), rand.nextInt(), rand
						.nextInt());
			}
		}, new Fields("x", "y", "z")), spout_parallelism);

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

		builder.setBolt("sink", new Sink(), sink_parallelism).shuffleGrouping("op");

		Config conf = new Config();
		conf.setDebug(false);
		
		conf.put("log.statistics", logStats);
		conf.put("log.statistics.path", statsPath);

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
