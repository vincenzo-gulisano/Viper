package basic;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import operators.BaseBolt.BoltFunction;
import operators.BaseBolt.ViperBolt;
import operators.baseSpout.SpoutFunction;
import operators.baseSpout.ViperSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ViperSpoutAndBolt {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		boolean local = Boolean.valueOf(args[0]);
		String statsPath = args[1];

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new ViperSpout(new SpoutFunction() {

			private static final long serialVersionUID = 1L;
			private int counter = 100;
			private Random r = new Random();

			public boolean hasNext() {
				return counter > 0;
			}

			public Values getTuple() {
				counter--;
				return new Values(r.nextInt());
			}

		}, new Fields("x"), true, statsPath));

		builder.setBolt(
				"mul",
				new ViperBolt(new Fields("2x"), true, statsPath,
						new BoltFunction() {

							private static final long serialVersionUID = 1L;

							public void receivedWriteLog(Tuple t) {
							}

							public void receivedFlush(Tuple t) {
							}

							public List<Values> process(Tuple t) {
								List<Values> result = new ArrayList<Values>();
								result.add(new Values(2 * t
										.getIntegerByField("x")));
								return result;
							}

						}), 1).shuffleGrouping("spout");

		Config conf = new Config();
		conf.setDebug(false);

		if (!local) {
			conf.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar("ViperSpoutAndBolt",
					conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(1);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("ViperSpoutAndBolt", conf,
					builder.createTopology());

			Thread.sleep(30000);

			cluster.shutdown();
		}

	}
}
