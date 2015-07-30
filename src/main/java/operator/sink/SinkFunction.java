package operator.sink;

import java.io.File;
import java.util.List;
import java.util.Map;

import operator.viperBolt.BoltFunction;
import statistics.AvgStat;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SinkFunction implements BoltFunction {

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
	public void receivedFlush(Tuple t) {
	}

	@Override
	public void receivedWriteLog(Tuple t) {
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
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		this.keepStats = (Boolean) stormConf.getOrDefault("log.statistics",
				false);
		this.statsPath = (String) stormConf.getOrDefault("log.statistics.path",
				"");

		if (keepStats) {

			String componentId = context.getThisComponentId();
			int taskIndex = context.getThisTaskIndex();

			latencyStat = new AvgStat("", statsPath + File.separator
					+ componentId + "." + taskIndex + ".latency.csv", false);
			latencyStat.start();
		}
	}

}
