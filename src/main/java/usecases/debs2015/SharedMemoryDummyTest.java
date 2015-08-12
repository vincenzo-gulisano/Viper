package usecases.debs2015;

import java.util.ArrayList;
import java.util.List;

import operator.csvSink.CSVFileWriter;
import operator.csvSink.CSVSink;
import operator.csvSpout.CSVFileReader;
import operator.csvSpout.CSVReaderSpout;
import operator.sink.Sink;
import operator.viperBolt.BoltFunctionBase;
import operator.viperBolt.ViperBolt;
import topology.ViperShuffle;
import topology.ViperTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import core.ViperUtils;
import core.ViperValues;

public class SharedMemoryDummyTest {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		boolean local = Boolean.valueOf(args[0]);
		boolean logStats = Boolean.valueOf(args[1]);
		String statsPath = args[2];
		final int parallelism = Integer.valueOf(args[3]);
		final int stateless_parallelism = Integer.valueOf(args[4]);
		String topologyName = args[5];
		String inputFilePrefix = args[6];
		boolean logOut = Boolean.valueOf(args[7]);
		String outputFilePrefix = args[8];

		ViperTopologyBuilder builder = new ViperTopologyBuilder();

		builder.setSpout("spout", new CSVReaderSpout(new CSVFileReader() {

			@Override
			protected Values convertLineToTuple(String line) {
				return new Values(ViperUtils.getTsFromText(
						"yyyy-MM-dd HH:mm:ss", line.split(",")[3]), line);
			}

		}, new Fields("tuple_ts", "line")), parallelism);

		Fields outFields = new Fields("tuple_ts", "line");
		builder.setBolt("convert",
				new ViperBolt(outFields, new BoltFunctionBase() {

					@Override
					public List<Values> process(Tuple t) {
						ArrayList<Values> result = new ArrayList<Values>();
						result.add(new ViperValues(
								t.getLongByField("tuple_ts"), t
										.getStringByField("line")));
						return result;
					}
				}), stateless_parallelism).customGrouping("spout",
				new ViperShuffle());

		if (logOut) {
			builder.setBolt("sink", new CSVSink(new CSVFileWriter() {

				@Override
				protected String convertTupleToLine(Tuple t) {
					return t.getLongByField("tuple_ts") + ";"
							+ t.getStringByField("line");
				}

			}), stateless_parallelism).customGrouping("convert",
					new ViperShuffle());
		} else {
			builder.setBolt("sink", new Sink(), stateless_parallelism)
					.customGrouping("convert", new ViperShuffle());
		}

		Config conf = new Config();
		conf.setDebug(false);

		conf.put("log.statistics", logStats);
		conf.put("log.statistics.path", statsPath);
		for (int i = 0; i < parallelism; i++) {
			conf.put("spout." + i + ".filepath", inputFilePrefix + i + ".csv");
		}
		for (int i = 0; i < stateless_parallelism; i++) {
			conf.put("sink.0.filepath", outputFilePrefix + i + ".csv");
		}

		if (!local) {
			conf.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());

			Thread.sleep(80000);

			cluster.shutdown();
		}

	}
}
