package statelessOperator;

import java.util.concurrent.ConcurrentLinkedQueue;

import operator.merger.Merger;
import operator.merger.MergerEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StatelessOpReadyThread implements Runnable {

	public static Logger LOG = LoggerFactory
			.getLogger(StatelessOpReadyThread.class);

	ConcurrentLinkedQueue<Values> outQueue;
	Merger merger;

	boolean goOn = true;

	public StatelessOpReadyThread(ConcurrentLinkedQueue<Values> outQueue,
			Merger merger) {
		this.outQueue = outQueue;
		this.merger = merger;
	}

	public void stop() {
		goOn = false;
	}

	@Override
	public void run() {

		while (goOn) {

			MergerEntry entry = merger.getNextReady();
			if (entry == null)
				Utils.sleep(1);

			while (entry != null) {
				// IF WE ARE SEEING THE FLUSH TUPLE
				if (entry.getTs() == Long.MAX_VALUE) {
					goOn = false;
					LOG.info("Ready thread just added FLUSH tuple, breaking");
					break;
				}
				outQueue.add((Values) entry.getO());
				entry = merger.getNextReady();
			}

		}

	}

}
