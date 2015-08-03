package topology;

import operator.merger.ViperMerger;
import statelessOperator.BoltFunctionFactory;
import statelessOperator.SharedMemoryStateless;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class ViperTopologyBuilder extends TopologyBuilder {

	public ViperTopologyBuilder() {
	}

	// TODO Should we just assume the previous bolt has at least 2 tasks too?
	// Otherwise, the merger is not needed...
	public void addParallelStatelessBolt(String id, IRichBolt bolt,
			Number parallelism_hint, String prevId, Fields prevFields) {

		if (parallelism_hint.intValue() < 2)
			throw new RuntimeException(
					"parallelism hint needs to be at least 2 for a parallel operator!");

		setBolt(id + "_merger", new ViperMerger(prevFields, "ts"),
				parallelism_hint).customGrouping(prevId, new ViperShuffle());

		setBolt(id, bolt, parallelism_hint).directGrouping(id + "_merger");

	}

	// TODO Should we just assume the previous bolt has at least 2 tasks too?
	// Otherwise, the merger is not needed...
	// This name does not say much, maybe SMStateless?
	public void addViperStatelessBolt(String id, int processingThreads,
			Fields outFields, BoltFunctionFactory factory, String tsField,
			String prevId) {

		setBolt(
				id,
				new SharedMemoryStateless(outFields, factory,
						processingThreads, tsField), 1).shuffleGrouping(prevId);
		// TODO Notice that here we use shuffle because we know everything runs
		// in the same machine, but what if we use multiple nodes?

	}
}
