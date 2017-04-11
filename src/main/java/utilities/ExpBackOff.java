package utilities;

import java.util.Random;

public class ExpBackOff {

	final int minDelay, maxDelay;
	int currentLimit;
	Random rand;
	
	/*
	 * Exponential backoff that sleeps for delay chosen uniformly at random, from (0,min). The interval exponentially increases until (0,max).
	 */
	public ExpBackOff(int min, int max) {
		minDelay = min;
		maxDelay = max;
		rand = new Random();
		currentLimit = min;
	}
	
	public int backoff() throws InterruptedException {
		int delay = rand.nextInt(currentLimit);
		currentLimit = (2 * currentLimit < maxDelay) ? 2 * currentLimit : maxDelay;
		Thread.sleep(delay);
		return delay;
	}
}
