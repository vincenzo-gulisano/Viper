package spout;

import java.util.List;
import java.util.Map;

import statistics.CountStat;
import core.TupleType;
import core.ViperUtils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class SimpleSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1178144110062379252L;

	private GetTuple udf;
	private Fields outFields;
	private SpoutOutputCollector collector;
	private long tuplesToSend;
	private boolean flushSent = false;
	private boolean writelogSent = false;
	private boolean keepStats;
	private String statsPath;
	private CountStat countStat;

	public SimpleSpout(GetTuple udf, Fields outFields, long tuplesToSend,
			boolean keepStats, String statsPath) {

		this.udf = udf;
		// Add the type and ts fields to the output fields
		List<String> fieldsList = outFields.toList();
		fieldsList.add(0, "type");
		fieldsList.add(1, "ts");
		this.outFields = new Fields(fieldsList);
		this.tuplesToSend = tuplesToSend;
		this.keepStats = keepStats;
		this.statsPath = statsPath;

	}

	@Override
	public void nextTuple() {
		if (tuplesToSend > 0) {
			tuplesToSend--;
			List<Object> toSend = udf.getTuple().getValues();
			toSend.add(0, TupleType.REGULAR);
			toSend.add(1, System.currentTimeMillis());
			collector.emit(toSend);
			if (keepStats) {
				countStat.increase(1);
			}
		} else if (!flushSent) {
			collector.emit(ViperUtils.getFlushTuple(this.outFields.size() - 2));
			flushSent = true;
		} else if (!writelogSent) {
			collector
					.emit(ViperUtils.getWriteLogTuple(this.outFields.size() - 2));
			writelogSent = true;

			if (keepStats) {
				countStat.stopStats();
				try {
					countStat.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				countStat.writeStats();
			}

		} else {
			Utils.sleep(10000);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		collector = arg2;

		if (keepStats) {
			// TODO Check the id to give to the spout
			countStat = new CountStat("", statsPath + "spout.rate.txt", false);
			countStat.start();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(this.outFields);
	}
}
