package scalegate;

import java.util.concurrent.ConcurrentLinkedQueue;

public class SingleQueueScaleGate implements ScaleGate {

	ConcurrentLinkedQueue<SGTuple> queue;

	public SingleQueueScaleGate() {
		queue = new ConcurrentLinkedQueue<SGTuple>();
	}

	public SGTuple getNextReadyTuple(int readerID) {
		return queue.poll();
	}

	public void addTuple(SGTuple tuple, int writerID) {
		queue.add(tuple);
	}

}
