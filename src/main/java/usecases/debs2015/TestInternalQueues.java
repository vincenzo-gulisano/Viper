package usecases.debs2015;

import java.util.List;
import java.util.Map;
import java.util.Random;

import operator.sink.Sink;
import operator.viperBolt.BoltFunctionBase;
import operator.viperBolt.ViperBolt;
import operator.viperSpout.SpoutFunction;
import operator.viperSpout.ViperSpout;
import topology.ViperShuffle;
import topology.ViperShuffleInternalQueues;
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
import backtype.storm.utils.Utils;

public class TestInternalQueues {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		boolean local = Boolean.valueOf(args[0]);
		boolean logStats = Boolean.valueOf(args[1]);
		String statsPath = args[2];
		final int spout_parallelism = Integer.valueOf(args[3]);
		String topologyName = args[4];
		final long duration = Long.valueOf(args[5]);

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
				int a = rand.nextInt();
				int b = rand.nextInt();
				int c = rand.nextInt();
				Utils.sleep(1000);
				System.out.println("Spout sending [" + a + "," + b + "," + c
						+ "]");
				return new Values(a, b, c);
			}
		}, new Fields("x", "y", "z")), 1);

		builder.setBolt("sink",
				new ViperBolt(new Fields(), new BoltFunctionBase() {

					@Override
					public List<Values> process(Tuple t) {
						System.out.println("bolt received " + t);
						return null;
					}
				}), 1)
				.customGrouping("spout", new ViperShuffleInternalQueues());

		Config conf = new Config();
		conf.setDebug(false);

		conf.put("log.statistics", logStats);
		conf.put("log.statistics.path", statsPath);

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
