package topology;

import operator.merger.ViperMerger;
import operator.viperBolt.BoltFunction;
import statelessOperator.StatelessBolt;
import statelessOperator.StatelessSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class ViperTopologyBuilder extends TopologyBuilder {

	public ViperTopologyBuilder() {
	}

	// TODO Should we just assume the previous bolt has at least 2 tasks too?
	// Otherwise, the merger is not needed...
	public void addParallelStatelessBolt(String id, IRichBolt bolt,
			Number parallelism_hint, String prevId, Fields prevFields,
			String tsField) {

		setBolt(id + "_merger", new ViperMerger(prevFields, tsField),
				parallelism_hint).customGrouping(prevId, new ViperShuffle());

		setBolt(id, bolt, parallelism_hint).directGrouping(id + "_merger");

	}

	// TODO Should we just assume the previous bolt has at least 2 tasks too?
	// Otherwise, the merger is not needed...
	// This name does not say much, maybe SMStateless?
	public void addViperStatelessBolt(String id, int processingThreads,
			Fields outFields, BoltFunction f, String tsField, String prevId) {

		setBolt(id, new StatelessBolt(outFields, f, tsField, id),
				processingThreads).customGrouping(prevId, new ViperShuffle());
//		setSpout(id, new StatelessSpout(outFields, id));
	}
}
