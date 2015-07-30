package operator.viperSpout;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import statistics.CountStat;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import core.TupleType;
import core.ViperUtils;

public class ViperSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1178144110062379252L;
	public static Logger LOG = LoggerFactory.getLogger(ViperSpout.class);

	private SpoutFunction udf;
	private Fields outFields;
	private SpoutOutputCollector collector;
	private boolean flushSent = false;
	private boolean writelogSent = false;
	private boolean keepStats;
	private String statsPath;
	private CountStat countStat;

	private String id;
	private long counter = 0;
	private long ackGap = 0;
	private long failCounter = 0;

	public ViperSpout(SpoutFunction udf, Fields outFields) {

		this.udf = udf;
		this.outFields = ViperUtils.enrichWithBaseFields(outFields);

	}

	public void nextTuple() {
		if (udf.hasNext()) {

			if (ackGap < 1000) {
				
				Values v = udf.getTuple();
				v.add(0, TupleType.REGULAR);
				v.add(1, System.currentTimeMillis());
				v.add(2, id);
				collector.emit(v, counter);
				counter++;
				if (keepStats) {
					countStat.increase(1);
				}

			}
		} else if (!flushSent) {
			collector.emit(ViperUtils.getFlushTuple(this.outFields.size() - 2));

			LOG.info("Spout " + id + " sending FLUSH tuple");

			flushSent = true;
		} else if (!writelogSent) {
			collector
					.emit(ViperUtils.getWriteLogTuple(this.outFields.size() - 2));
			writelogSent = true;

			if (keepStats) {
				Utils.sleep(2000); // Just wait for latest stats to be written
				countStat.stopStats();
				try {
					countStat.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				countStat.writeStats();
			}

		} else {
			Utils.sleep(1000);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		collector = arg2;

		this.keepStats = (Boolean) arg0.getOrDefault("log.statistics", false);
		this.statsPath = (String) arg0.getOrDefault("log.statistics.path", "");

		id = arg1.getThisComponentId() + "." + arg1.getThisTaskIndex();

		if (keepStats) {
			countStat = new CountStat("", statsPath + File.separator + id
					+ ".rate.csv", false);
			countStat.start();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(this.outFields);
	}

	@Override
	public void ack(Object msgId) {
		ackGap = counter - (Long) msgId;
		if (ackGap % 100 == 0) {
			System.out.println("ack: " + ackGap);
		}
	}

	@Override
	public void fail(Object msgId) {
		failCounter++;
		if (failCounter % 1000 == 0) {
			System.out.println("fail: " + failCounter);
		}
	}
}
