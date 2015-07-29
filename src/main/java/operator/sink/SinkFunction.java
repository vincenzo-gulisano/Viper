package operator.sink;

import java.io.File;
import java.util.List;

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

	public SinkFunction(boolean keepStats, String statsPath) {
		this.keepStats = keepStats;
		this.statsPath = statsPath;
	}

	@Override
	public List<Values> process(Tuple t) {
		if (keepStats) {
			latencyStat.add(System.currentTimeMillis()
					- t.getLongByField("ts"));
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

	@Override
	public void prepare(TopologyContext context) {
		if (keepStats) {

			String componentId = context.getThisComponentId();
			int taskIndex = context.getThisTaskIndex();

			latencyStat = new AvgStat("", statsPath + File.separator
					+ componentId + "." + taskIndex + ".latency.csv", false);
			latencyStat.start();
		}
	}

}
