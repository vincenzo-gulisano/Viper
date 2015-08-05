package operator.sink;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import operator.viperBolt.BoltFunctionBase;
import statistics.AvgStat;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SinkFunction extends BoltFunctionBase {

	private static final long serialVersionUID = 1L;
	private AvgStat latencyStat;
	private boolean keepStats;
	private String statsPath;
	private boolean statsWritten = false;

	public SinkFunction() {
	}

	@Override
	public List<Values> process(Tuple t) {
		if (keepStats) {
			latencyStat
					.add(System.currentTimeMillis() - t.getLongByField("ts"));
		}
		return null;
	}

	@Override
	public List<Values> receivedFlush(Tuple t) {
		if (keepStats && !statsWritten) {
			statsWritten = true;
			Utils.sleep(2000); // Just wait for latest statistics to be
								// stored
			latencyStat.stopStats();
			try {
				latencyStat.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			latencyStat.writeStats();
		}
		return new ArrayList<Values>();
	}

//	@Override
//	public void receivedWriteLog(Tuple t) {
//
//	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		Object temp = stormConf.get("log.statistics");
		this.keepStats = temp != null ? (Boolean) temp : false;

		temp = stormConf.get("log.statistics.path");
		this.statsPath = temp != null ? (String) temp : "";

		if (keepStats) {

			String componentId = context.getThisComponentId();
			int taskIndex = context.getThisTaskIndex();

			latencyStat = new AvgStat("", statsPath + File.separator
					+ stormConf.get(Config.TOPOLOGY_NAME) + "_" + componentId
					+ "." + taskIndex + ".latency.csv", false);
			latencyStat.start();
		}
	}

}
