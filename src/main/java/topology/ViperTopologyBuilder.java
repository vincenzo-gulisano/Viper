package topology;

import operator.merger.ViperMerger;
import statelessOperator.BoltFunctionFactory;
import statelessOperator.SharedMemoryStateless;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Fields;
import de.hub.cs.dbis.aeolus.batching.api.AeolusBuilder;

public class ViperTopologyBuilder extends AeolusBuilder {

	public ViperTopologyBuilder() {
	}

	// TODO Should we just assume the previous bolt has at least 2 tasks too?
	// Otherwise, the merger is not needed...
	public void addParallelStatelessBolt(String id, IRichBolt bolt,
			Number parallelism_hint, String prevId, Fields prevFields,
			String tsField, int batchSize) {

		super.setBolt(id + "_merger", new ViperMerger(prevFields, tsField),
				parallelism_hint, batchSize).customGrouping(prevId,
				new ViperShuffle());

		super.setBolt(id, bolt, parallelism_hint, batchSize).directGrouping(
				id + "_merger");

	}

	// TODO Should we just assume the previous bolt has at least 2 tasks too?
	// Otherwise, the merger is not needed...
	// This name does not say much, maybe SMStateless?
	public void addViperStatelessBolt(String id, int processingThreads,
			Fields outFields, BoltFunctionFactory factory, String tsField,
			String prevId, int batchSize) {

		super.setBolt(
				id,
				new SharedMemoryStateless(outFields, factory,
						processingThreads, tsField), 1, batchSize)
				.shuffleGrouping(prevId);
		// TODO Notice that here we use shuffle because we know everything runs
		// in the same machine, but what if we use multiple nodes?

	}
}
