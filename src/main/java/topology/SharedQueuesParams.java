package topology;

import java.io.Serializable;

public class SharedQueuesParams implements Serializable {

	private static final long serialVersionUID = -1336645449595137935L;

	public boolean shouldUseInternalQueues;
	public int workerQueuePeriodSize;
	public long workerQueueTimeout;
	public int workerQueueMaxSize;
	public int workerQueueMinThresholdSize;

	public SharedQueuesParams() {
		this.shouldUseInternalQueues = false;
	}

	public SharedQueuesParams(boolean shouldUseInternalQueues,
			int workerQueuePeriodSize, long workerQueueTimeout,
			int workerQueueMaxSize, int workerQueueMinThresholdSize) {
		super();
		this.shouldUseInternalQueues = shouldUseInternalQueues;
		this.workerQueuePeriodSize = workerQueuePeriodSize;
		this.workerQueueTimeout = workerQueueTimeout;
		this.workerQueueMaxSize = workerQueueMaxSize;
		this.workerQueueMinThresholdSize = workerQueueMinThresholdSize;
	}

}