package operator.viperSpout;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import statistics.AvgStat;
import statistics.CountStat;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
//import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import core.TupleType;
import core.ViperUtils;

public class ViperSpoutInternalOnly extends BaseRichSpout {

	private static final long serialVersionUID = 1178144110062379252L;
	public static Logger LOG = LoggerFactory
			.getLogger(ViperSpoutInternalOnly.class);

	private SpoutFunction udf;
	private Fields outFields;
	// private SpoutOutputCollector collector;
	private boolean flushSent = false;
	private boolean keepStats;
	private String statsPath;
	private CountStat countStat;
	private CountStat invocationsStat;
	private AvgStat costStat;

	private String id;
	private long counter = 0;

	private boolean veryFirstTuple = true;

	private SpeedRegulator speedRegulator;

	InternalQueuesShuffle iq;

	public ViperSpoutInternalOnly(SpoutFunction udf, Fields outFields,
			InternalQueuesShuffle iq) {

		this.udf = udf;
		this.outFields = ViperUtils.enrichWithBaseFields(outFields);
		this.iq = iq;

	}

	public void nextTuple() {

		if (veryFirstTuple) {
			LOG.info("veryFirstTuple for spout " + id + " Sleeping 10 seconds");
			Utils.sleep(10000);
			veryFirstTuple = false;
		}

		if (!flushSent)
			speedRegulator.regulateSpeed();

		long start = System.nanoTime();
		if (keepStats) {
			invocationsStat.increase(1);
		}
		if (udf.hasNext()) {

			Values v = udf.getTuple();
			if (v != null) {
				v.add(0, TupleType.REGULAR);
				v.add(1, System.currentTimeMillis());

				iq.emit(id, v);
				counter++;

				if (keepStats) {
					countStat.increase(1);
					costStat.add((System.nanoTime() - start));
				}
			}

		} else if (!flushSent) {
			iq.emit(id, ViperUtils.getFlushTuple(this.outFields.size() - 2));

			LOG.info("Spout " + id + " sending FLUSH tuple, " + counter
					+ " tuples sent");

			flushSent = true;

			if (keepStats) {
				Utils.sleep(2000); // Just wait for latest statistics to be
									// written
				countStat.stopStats();
				costStat.stopStats();
				invocationsStat.stopStats();
				try {
					countStat.join();
					costStat.join();
					invocationsStat.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				countStat.writeStats();
				costStat.writeStats();
				invocationsStat.writeStats();
			}

		} else {
			Utils.sleep(1000);
		}

	}

	@SuppressWarnings({ "rawtypes" })
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// collector = arg2;

		Object temp = arg0.get("log.statistics");
		this.keepStats = temp != null ? (Boolean) temp : false;

		temp = arg0.get("log.statistics.path");
		this.statsPath = temp != null ? (String) temp : "";

		id = arg1.getThisComponentId() + "." + arg1.getThisTaskIndex();

		long parallelism = arg1.getComponentTasks(arg1.getThisComponentId())
				.size();
		speedRegulator = new SpeedRegulator(id, 10000 / parallelism,
				800000 / parallelism, 300, 1000);

		if (keepStats) {
			countStat = new CountStat("", statsPath + File.separator
					+ arg0.get(Config.TOPOLOGY_NAME) + "_" + id + ".rate.csv",
					false);
			countStat.start();
			costStat = new AvgStat("", statsPath + File.separator
					+ arg0.get(Config.TOPOLOGY_NAME) + "_" + id + ".cost.csv",
					false);
			costStat.start();
			invocationsStat = new CountStat("", statsPath + File.separator
					+ arg0.get(Config.TOPOLOGY_NAME) + "_" + id
					+ ".invocations.csv", false);
			invocationsStat.start();
		}

		udf.prepare(arg0, arg1);

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(this.outFields);
	}

}
