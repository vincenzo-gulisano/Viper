package examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import operator.sink.Sink;
import operator.viperBolt.BoltFunction;
import operator.viperBolt.ViperBolt;
import operator.viperSpout.SpoutFunction;
import operator.viperSpout.ViperSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ViperSpoutAndBolt {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		boolean local = Boolean.valueOf(args[0]);
		String statsPath = args[1];

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new ViperSpout(new SpoutFunction() {

			private static final long serialVersionUID = 1L;
			private int counter = 20000;
			private Random r = new Random();

			public boolean hasNext() {
				return counter > 0;
			}

			public Values getTuple() {
				counter--;
				return new Values(r.nextInt());
			}

			@Override
			public void prepare(Map stormConf, TopologyContext context) {
				// TODO Auto-generated method stub
				
			}

		}, new Fields("x")));

		builder.setBolt("mul",
				new ViperBolt(new Fields("2x"), new BoltFunction() {

					private static final long serialVersionUID = 1L;

					public void receivedWriteLog(Tuple t) {
					}

					public void receivedFlush(Tuple t) {
					}

					public List<Values> process(Tuple t) {
						Utils.sleep(1);
						List<Values> result = new ArrayList<Values>();
						result.add(new Values(2 * t.getIntegerByField("x")));
						return result;
					}

					@SuppressWarnings("rawtypes")
					@Override
					public void prepare(Map stormConf, TopologyContext context) {
					}

				}), 1).shuffleGrouping("spout");

		builder.setBolt("sink", new Sink(), 1).shuffleGrouping("mul");

		Config conf = new Config();
		conf.setDebug(false);

		conf.put("log.statistics", true);
		conf.put("log.statistics.path", statsPath);

		if (!local) {
			conf.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar("ViperSpoutAndBolt",
					conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(1);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("ViperSpoutAndBolt", conf,
					builder.createTopology());

			Thread.sleep(120000);

			cluster.shutdown();
		}

	}
}
