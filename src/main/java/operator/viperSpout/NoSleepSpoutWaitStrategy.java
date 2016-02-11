package operator.viperSpout;

import java.util.Map;

import backtype.storm.spout.ISpoutWaitStrategy;

public class NoSleepSpoutWaitStrategy implements ISpoutWaitStrategy {

	@Override
	public void prepare(Map conf) {

	}

	@Override
	public void emptyEmit(long streak) {

	}
}
