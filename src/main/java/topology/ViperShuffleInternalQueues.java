package topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import core.TupleType;

public class ViperShuffleInternalQueues implements CustomStreamGrouping,
		Serializable {

	public class TargetManager {

		private String id;
		private boolean shouldUseInternalQueues;

		private int workerQueuePeriodSize;
		private long workerQueueTimeout;
		private int workerQueueMaxSize;
		private int workerQueueMinThresholdSize;

		private boolean isInSizeControlMode;
		private int tuplesAddedSinceLastNormallySentTuple;
		private long timestampOfNormallySentTuple;

		public TargetManager(WorkerTopologyContext context, String id,
				int sourceTask, int targetTask, SharedQueuesParams params) {

			this.id = id;
			this.shouldUseInternalQueues = params.shouldUseInternalQueues
					&& context.getThisWorkerTasks().contains(targetTask);

			if (this.shouldUseInternalQueues) {

				SharedQueues.registerQueue(this.id);
				this.isInSizeControlMode = false;
				this.tuplesAddedSinceLastNormallySentTuple = 0;
				this.timestampOfNormallySentTuple = System.currentTimeMillis();

				this.workerQueuePeriodSize = params.workerQueuePeriodSize;
				this.workerQueueTimeout = params.workerQueueTimeout;
				this.workerQueueMaxSize = params.workerQueueMaxSize;
				this.workerQueueMinThresholdSize = params.workerQueueMinThresholdSize;
			}

		}

		public boolean shouldSendViaSharedQueue() {
			boolean result = false;

			// Condition to send via shared queue: should use internal queue,
			// number of tuples added is less than
			// tuplesAddedSinceLastNormallySentTuple, the max size has not been
			// exceeded, if it is control mode the minimum size has been reached
			// and the timeout time has not expired
			if (shouldUseInternalQueues) {
				if (tuplesAddedSinceLastNormallySentTuple <= workerQueuePeriodSize)
					if (SharedQueues.size(id) <= workerQueueMaxSize)
						if (!isInSizeControlMode
								|| (isInSizeControlMode && SharedQueues
										.size(id) <= workerQueueMinThresholdSize))
							if (System.currentTimeMillis()
									- timestampOfNormallySentTuple <= workerQueueTimeout)
								result = true;
			}

			if (result) {
				tuplesAddedSinceLastNormallySentTuple++;
				isInSizeControlMode = false;
			} else {
				tuplesAddedSinceLastNormallySentTuple = 0;
				isInSizeControlMode = SharedQueues.size(id) > workerQueueMaxSize;
				timestampOfNormallySentTuple = System.currentTimeMillis();
			}

			return result;
		}
	}

	private static final long serialVersionUID = 3014404246770284550L;

	int index = 0;
	List<Integer> targetTasks;
	WorkerTopologyContext context;
	SharedQueuesParams params;
	HashMap<String, TargetManager> targetManagers;

	public ViperShuffleInternalQueues() {
		this.params = new SharedQueuesParams();
	}

	public ViperShuffleInternalQueues(SharedQueuesParams params) {
		this.params = params;
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		this.context = context;
		this.targetTasks = targetTasks;
		targetManagers = new HashMap<String, ViperShuffleInternalQueues.TargetManager>();
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		ArrayList<Integer> result = new ArrayList<Integer>();
		TupleType type = (TupleType) values.get(0);
		if (type.equals(TupleType.REGULAR)) {
			int task = targetTasks.get(index);
			String id = taskId + ":" + task;
			if (!targetManagers.containsKey(id))
				targetManagers.put(id, new TargetManager(context, id, taskId,
						task, params));
			SharedQueues.registerQueue(taskId + ":" + task);
			if (targetManagers.get(id).shouldSendViaSharedQueue()) {
				SharedQueues.add(id, values);
			} else {
				result.add(task);
			}
			index = (index + 1) % targetTasks.size();
			return result;
		} else if (type.equals(TupleType.FLUSH)) {
			return targetTasks;
		}
		return null;
	}
}
