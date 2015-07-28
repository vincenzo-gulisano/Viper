package operators.BaseBolt;

import java.io.File;
import java.util.Map;

import statistics.CountStat;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import core.TupleType;
import core.ViperUtils;

public class ViperBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8693720878488229181L;

	private Fields outFields;
	private OutputCollector collector;
	private boolean keepStats;
	private boolean statsWritten = false;
	private String statsPath;
	private CountStat countStat;
	private BoltFunction f;

	private String componentId;
	private int taskIndex;

	public ViperBolt(Fields outFields, boolean keepStats, String statsPath,
			BoltFunction boltFunction) {

		this.outFields = ViperUtils.enrichWithBaseFields(outFields);
		this.keepStats = keepStats;
		this.statsPath = statsPath;
		this.f = boltFunction;

	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

		componentId = context.getThisComponentId();
		taskIndex = context.getThisTaskIndex();

		if (keepStats) {
			// TODO Check the id to give to the spout
			countStat = new CountStat("", statsPath + File.separator
					+ componentId + "." + taskIndex + ".rate.csv", false);
			countStat.start();
		}

	}

	public void execute(Tuple input) {

		TupleType ttype = (TupleType) input.getValueByField("type");
		if (ttype.equals(TupleType.REGULAR)) {

			for (Values t : f.process(input)) {
				collector.emit(ViperUtils.enrichListWithBasicFields(t));
				if (keepStats) {
					countStat.increase(1);
				}
			}
		} else if (ttype.equals(TupleType.FLUSH)) {
			f.receivedFlush(input);
		} else if (ttype.equals(TupleType.WRITELOG)) {
			f.receivedWriteLog(input);

			if (keepStats && !statsWritten) {
				statsWritten = true;
				Utils.sleep(2000); // Just wait for latest stats to be written
				countStat.stopStats();
				try {
					countStat.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				countStat.writeStats();
			}

		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.outFields);
	}

}
