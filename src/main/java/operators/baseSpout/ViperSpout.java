package operators.baseSpout;

import java.io.File;
import java.util.Map;

import statistics.CountStat;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import core.ViperUtils;

public class ViperSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1178144110062379252L;

	private SpoutFunction udf;
	private Fields outFields;
	private SpoutOutputCollector collector;
	private boolean flushSent = false;
	private boolean writelogSent = false;
	private boolean keepStats;
	private String statsPath;
	private CountStat countStat;

	private String componentId;
	private int taskIndex;

	public ViperSpout(SpoutFunction udf, Fields outFields, boolean keepStats,
			String statsPath) {

		this.udf = udf;
		this.outFields = ViperUtils.enrichWithBaseFields(outFields);
		this.keepStats = keepStats;
		this.statsPath = statsPath;

	}

	@Override
	public void nextTuple() {
		if (udf.hasNext()) {
			collector
					.emit(ViperUtils.enrichListWithBasicFields(udf.getTuple()));
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

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		collector = arg2;

		componentId = arg1.getThisComponentId();
		taskIndex = arg1.getThisTaskIndex();

		if (keepStats) {
			countStat = new CountStat("", statsPath + File.separator
					+ componentId + "." + taskIndex + ".rate.csv", false);
			countStat.start();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(this.outFields);
	}
}
