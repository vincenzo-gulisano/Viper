package topology;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
 * Should have generic container for queue, not just ConcurrentLinkedQueue
 * Also, other methods for the flushing? Maybe not needed
 */
public class SharedQueues {

	private static HashMap<String, ConcurrentLinkedQueue<List<Object>>> queues = new HashMap<String, ConcurrentLinkedQueue<List<Object>>>();

	public SharedQueues() {
		// TODO Auto-generated constructor stub
	}

	public static void registerQueue(String id) {
		if (queues.containsKey(id))
			throw new IllegalArgumentException(
					"Cannot register queue for task " + id
							+ " already registered!");
		queues.put(id, new ConcurrentLinkedQueue<List<Object>>());
	}

	public static void add(String id, List<Object> values) {
		if (!queues.containsKey(id))
			throw new IllegalArgumentException("Cannot add to queue for task "
					+ id + " not registered!");
		queues.get(id).add(values);
	}

	public static List<Object> pollNextTuple(String id) {
		if (!queues.containsKey(id))
			throw new IllegalArgumentException(
					"Cannot get from queue for task " + id + " not registered!");
		return queues.get(id).poll();
	}

	public static List<Object> peekNextTuple(String id) {
		if (!queues.containsKey(id))
			throw new IllegalArgumentException(
					"Cannot get from queue for task " + id + " not registered!");
		return queues.get(id).peek();
	}
}
