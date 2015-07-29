package operator.merger;

import operator.viperBolt.ViperBolt;
import backtype.storm.tuple.Fields;

public class ViperMerger extends ViperBolt {

	private static final long serialVersionUID = -3556782352568567327L;

	public ViperMerger(Fields outFields, boolean keepStats, String statsPath) {
		super(outFields, keepStats, statsPath, new ViperMergerFunction());
		// we are just forwarding tuples, no need to re-add metadata
		addMetadata = false;
	}

}
