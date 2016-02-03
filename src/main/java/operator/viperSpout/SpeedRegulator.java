package operator.viperSpout;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

public class SpeedRegulator {

	public static Logger LOG = LoggerFactory.getLogger(SpeedRegulator.class);

	public SpeedRegulator(String id, double initialRate, double finalRate,
			double duration, double batchsize) {

		this.id = id;

		this.initialRate = initialRate;
		this.finalRate = finalRate;
		this.duration = duration;
		this.batchSize = batchsize;

		this.sleepProb = 1.0 / this.batchSize;
		this.secondsPassed = 0;
		this.firstInvocation = true;
		// this.rand = new Random();

		tupleCounter = 0;

	}

	private String id;

	private double initialRate;
	private double finalRate;
	private double duration;
	private double batchSize;

	private double currentRate;
	private double sleepProb;
	private double sleepPeriod;

	private double prevSecond;
	private double secondsPassed;

	private boolean firstInvocation;

	// private Random rand;
	private long tupleCounter;

	private boolean aSecondPassed() {
		boolean result = System.currentTimeMillis() / 1000 - prevSecond >= 1;
		if (result) {
			prevSecond = System.currentTimeMillis() / 1000;
			secondsPassed++;
			LOG.info("Speed regulator for " + id + ": a second has passed... ("
					+ secondsPassed + " in total)");
		}
		return result;
	}

	private void computeCurrentRate() {
		currentRate = initialRate + (finalRate - initialRate) / duration
				* secondsPassed;
		LOG.info("Speed regulator for " + id + ": current rate is "
				+ currentRate + " t/s");
	}

	private void computeCurrentBatchSize() {
		batchSize = currentRate / 10;
		LOG.info("Speed regulator for " + id + ": current batch size is "
				+ batchSize);
	}

	private void computeCurrentSleepProb() {
		sleepProb = 1.0 / batchSize;
		LOG.info("Speed regulator for " + id + ": current sleep prob is "
				+ sleepProb);
	}

	private void computeSleepPeriod() {
		this.sleepPeriod = 1000 / (currentRate / batchSize);
		LOG.info("Speed regulator for " + id + ": current sleep period is "
				+ sleepPeriod + " ms");
	}

	public void regulateSpeed() {

		if (firstInvocation) {
			firstInvocation = false;
			prevSecond = System.currentTimeMillis() / 1000;
			computeCurrentRate();
			computeCurrentBatchSize();
			computeCurrentSleepProb();
			computeSleepPeriod();
			tupleCounter = 0;
		}

		if (aSecondPassed()) {
			computeCurrentRate();
			computeCurrentBatchSize();
			computeCurrentSleepProb();
			computeSleepPeriod();
			tupleCounter = 0;
		}

		// if (rand.nextDouble() <= sleepProb)
		if (tupleCounter >= batchSize) {
			tupleCounter = 0;
			Utils.sleep((long) (sleepPeriod * 0.9));
		}

		tupleCounter++;

	}
}
