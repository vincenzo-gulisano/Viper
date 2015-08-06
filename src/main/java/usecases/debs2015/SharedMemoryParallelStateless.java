package usecases.debs2015;

import java.util.ArrayList;
import java.util.List;

import operator.csvSink.CSVFileWriter;
import operator.csvSink.CSVSink;
import operator.csvSpout.CSVFileReader;
import operator.csvSpout.CSVReaderSpout;
import operator.sink.Sink;
import operator.viperBolt.BoltFunction;
import operator.viperBolt.BoltFunctionBase;
import statelessOperator.BoltFunctionFactory;
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

public class SharedMemoryParallelStateless {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		boolean local = Boolean.valueOf(args[0]);
		boolean logStats = Boolean.valueOf(args[1]);
		String statsPath = args[2];
		final int spout_parallelism = Integer.valueOf(args[3]);
		final int stateless_parallelism = Integer.valueOf(args[4]);
		String topologyName = args[5];
		String inputFilePrefix = args[6];
		boolean logOut = Boolean.valueOf(args[7]);
		String outputFile = args[8];

		ViperTopologyBuilder builder = new ViperTopologyBuilder();

		builder.setSpout("spout", new CSVReaderSpout(new CSVFileReader() {

			@Override
			protected Values convertLineToTuple(String line) {
				return new Values(ViperUtils.getTsFromText(
						"yyyy-MM-dd HH:mm:ss", line.split(",")[3]), line);
			}

		}, new Fields("tuple_ts", "line")), spout_parallelism);

		Fields outFields = new Fields("license", "pickUpTS", "pickUpDate",
				"dropOffTS", "dropOffDate", "startCellQ1", "endCellQ1",
				"startCellQ2", "endCellQ2", "amount");
		builder.addViperStatelessBolt("convert", stateless_parallelism,
				outFields, new BoltFunctionFactory() {

					@Override
					public BoltFunction getBoltFunction() {
						return new BoltFunctionBase() {

							public List<Values> process(Tuple t) {

								String[] split = t.getStringByField("line")
										.split(",");
								String hackLicense = split[1];
								long pickUpDate = ViperUtils.getTsFromText(
										"yyyy-MM-dd HH:mm:ss", split[2]);
								long dropOffDate = ViperUtils.getTsFromText(
										"yyyy-MM-dd HH:mm:ss", split[3]);
								double pickupLongitude = Double
										.valueOf(split[6]);
								if (pickupLongitude >= -74.916578
										&& pickupLongitude < -73.120778) {
									double pickupLatitude = Double
											.valueOf(split[7]);
									if (pickupLatitude <= 41.47718278
											&& pickupLatitude > 40.12971598) {
										double dropOffLongitude = Double
												.valueOf(split[8]);
										if (dropOffLongitude >= -74.916578
												&& dropOffLongitude < -73.120778) {
											double dropOffLatitude = Double
													.valueOf(split[9]);
											if (dropOffLatitude <= 41.47718278
													&& dropOffLatitude > 40.12971598) {
												double amount = Double
														.valueOf(split[11])
														+ Double.valueOf(split[14]);
												int startCellQ1 = (int) (Math
														.floor(((pickupLongitude - (-74.916578)) / 0.005986) + 1) * 1000 + (Math
														.floor((41.47718 - pickupLatitude) / 0.004491556) + 1));
												int endCellQ1 = (int) ((Math
														.floor((dropOffLongitude - (-74.916578)) / 0.005986) + 1) * 1000 + (Math
														.floor((41.47718 - dropOffLatitude) / 0.004491556) + 1));
												int startCellQ2 = (int) ((Math
														.floor((pickupLongitude - (-74.916578))
																/ (0.005986 / 2)) + 1) * 1000 + (Math
														.floor((41.47718 - pickupLatitude)
																/ (0.004491556 / 2)) + 1));
												int endCellQ2 = (int) ((Math
														.floor((dropOffLongitude - (-74.916578))
																/ (0.005986 / 2)) + 1) * 1000 + (Math
														.floor((41.47718 - dropOffLatitude)
																/ (0.004491556 / 2)) + 1));

												List<Values> result = new ArrayList<Values>();

												result.add(new Values(
														hackLicense,
														pickUpDate, split[2],
														dropOffDate, split[3],
														startCellQ1, endCellQ1,
														startCellQ2, endCellQ2,
														amount));

												return result;

											}
										}
									}
								}

								return null;
							}

						};
					}
				}, "tuple_ts", "spout");

		if (logOut) {
			builder.setBolt("sink", new CSVSink(new CSVFileWriter() {

				@Override
				protected String convertTupleToLine(Tuple t) {
					return t.getStringByField("license") + ";"
							+ t.getLongByField("pickUpTS") + ";"
							+ t.getStringByField("pickUpDate") + ";"
							+ t.getLongByField("dropOffTS") + ";"
							+ t.getStringByField("dropOffDate") + ";"
							+ t.getIntegerByField("startCellQ1") + ";"
							+ t.getIntegerByField("endCellQ1") + ";"
							+ t.getIntegerByField("startCellQ2") + ";"
							+ t.getIntegerByField("endCellQ2") + ";"
							+ t.getDoubleByField("amount");
				}

			}), 1).shuffleGrouping("convert");
		} else {
			builder.addParallelStatelessBolt("sink", new Sink(), 1, "convert",
					outFields, "dropOffTS");
		}

		Config conf = new Config();
		conf.setDebug(false);

		conf.put("log.statistics", logStats);
		conf.put("log.statistics.path", statsPath);
		for (int i = 0; i < spout_parallelism; i++) {
			conf.put("spout." + i + ".filepath", inputFilePrefix + i + ".csv");
		}
		conf.put("sink.0.filepath", outputFile);
		//
		// conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, new Integer(
		// 16384));
		// conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, new
		// Integer(16384));
		// conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, new Integer(32));

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
