package operator.aggregate;

import operator.viperBolt.ViperBolt;
import backtype.storm.tuple.Fields;

public class ViperAggregate<T extends AggregateWindow> extends ViperBolt {

	private static final long serialVersionUID = -912367983226248473L;

	public ViperAggregate(Fields outFields, String timestampFieldID,
			String groupbyFieldID, long windowSize, long windowAdvance, T win) {
		super(outFields, new AggregateCore<T>(timestampFieldID, groupbyFieldID,
				windowSize, windowAdvance, win));
	}

}
