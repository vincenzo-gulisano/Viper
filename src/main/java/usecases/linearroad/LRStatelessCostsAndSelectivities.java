package usecases.linearroad;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Values;

public class LRStatelessCostsAndSelectivities {

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

		try {

			PrintWriter out = new PrintWriter(new FileWriter(statsPath
					+ File.separator + "log.txt"));

			ArrayList<String> input_tuples = new ArrayList<String>();
			FileInputStream fstream = new FileInputStream(input_data);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fstream));

			String strLine;
			// Read File Line By Line
			while ((strLine = br.readLine()) != null) {
				input_tuples.add(strLine);
			}
			br.close();

			double repetitions = 100000000;

			// Convert and Filter
			double outputs = 0;
			long before = System.currentTimeMillis();
			for (int i = 0; i < repetitions; i++) {
				LRTuple lrTuple = new LRTuple(input_tuples.get(i
						% input_tuples.size()));
				if (lrTuple.type == 0) {
					outputs++;
					Values v = new Values(lrTuple);
				}
			}
			long after = System.currentTimeMillis();
			out.println("Convert and Filter - cost: " + (after - before)
					/ repetitions + " selectivity: " + (outputs / repetitions));

			// Convert and Filter low selectivity
			outputs = 0;
			before = System.currentTimeMillis();
			for (int i = 0; i < repetitions; i++) {
				LRTuple lrTuple = new LRTuple(input_tuples.get(i
						% input_tuples.size()));
				if (lrTuple.type == 0 && lrTuple.speed == 0) {
					outputs++;
					Values v = new Values(lrTuple);
				}
			}
			after = System.currentTimeMillis();
			out.println("Convert and Filter low selectivity - cost: "
					+ (after - before) / repetitions + " selectivity: "
					+ (outputs / repetitions));

			ArrayList<LRTuple> inputTuplesConverted = new ArrayList<LRTuple>();
			for (String s : input_tuples) {
				inputTuplesConverted.add(new LRTuple(s));
			}

			// Only Filter
			outputs = 0;
			before = System.currentTimeMillis();
			for (int i = 0; i < repetitions; i++) {
				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());
				if (lrTuple.type == 0) {
					outputs++;
					Values v = new Values(lrTuple);
				}
			}
			after = System.currentTimeMillis();
			out.println("Only Filter low selectivity - cost: "
					+ (after - before) / repetitions + " selectivity: "
					+ (outputs / repetitions));

			out.flush();
			out.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// ViperTopologyBuilder builder = new ViperTopologyBuilder();
		//
		// builder.setSpout("spout", new ViperSpout(new SpoutFunction() {
		//
		// private long startTimestamp;
		// private ArrayList<String> input_tuples;
		// int index = 0;
		//
		// @SuppressWarnings("rawtypes")
		// @Override
		// public void prepare(Map stormConf, TopologyContext context) {
		// startTimestamp = System.currentTimeMillis();
		//
		// input_tuples = new ArrayList<String>();
		//
		// // Read input data
		// try {
		// // Open the file
		// FileInputStream fstream = new FileInputStream(input_data);
		// BufferedReader br = new BufferedReader(
		// new InputStreamReader(fstream));
		//
		// String strLine;
		//
		// // Read File Line By Line
		// while ((strLine = br.readLine()) != null) {
		// input_tuples.add(strLine);
		// }
		//
		// // Close the input stream
		// br.close();
		// } catch (FileNotFoundException e) {
		// e.printStackTrace();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		//
		// }
		//
		// @Override
		// public boolean hasNext() {
		// return (System.currentTimeMillis() - startTimestamp) < duration *
		// 1000;
		// }
		//
		// @Override
		// public Values getTuple() {
		// Values result = new Values(input_tuples.get(index));
		// index = (index + 1) % input_tuples.size();
		// return result;
		// }
		// }, new Fields("posrep")), spout_parallelism);
		//
		// class Filter extends BoltFunctionBase {
		//
		// @Override
		// public List<Values> process(Tuple arg0) {
		// List<Values> results = new ArrayList<Values>();
		// LRTuple lrTuple = new LRTuple(arg0.getStringByField("posrep"));
		// if (lrTuple.type == 0)
		// results.add(new Values(lrTuple));
		// return results;
		// }
		//
		// }
		//
		// BoltDeclarer opBolt = builder.setBolt("op", new ViperBolt(new Fields(
		// "posrep"), new Filter()), op_parallelism);
		// if (useOptimizedQueues) {
		// opBolt.customGrouping("spout", new ViperShuffleInternalQueues(
		// new SharedQueuesParams(true, 100, 50, 2000, 1000)));
		// } else {
		// opBolt.customGrouping("spout", new ViperShuffle());
		// }
		//
		// BoltDeclarer sinkBolt = builder.setBolt("sink", new Sink(),
		// sink_parallelism);
		//
		// if (useOptimizedQueues) {
		// sinkBolt.customGrouping("op", new ViperShuffleInternalQueues(
		// new SharedQueuesParams(true, 100, 50, 2000, 1000)));
		// } else {
		// sinkBolt.customGrouping("op", new ViperShuffle());
		// }
		//
		// Config conf = new Config();
		// conf.setDebug(false);
		//
		// conf.put("log.statistics", logStats);
		// conf.put("log.statistics.path", statsPath);
		//
		// conf.put("internal.queues", useOptimizedQueues);
		//
		// if (!local) {
		// conf.setNumWorkers(workers);
		// StormSubmitter.submitTopologyWithProgressBar(topologyName, conf,
		// builder.createTopology());
		// } else {
		//
		// LocalCluster cluster = new LocalCluster();
		// cluster.submitTopology(topologyName, conf, builder.createTopology());
		//
		// Thread.sleep(600000);
		//
		// cluster.shutdown();
		// }

	}
}
