package statelessOperator;

import operator.viperBolt.BoltFunction;
import operator.viperBolt.ViperBolt;
import backtype.storm.tuple.Fields;

public class StatelessBolt extends ViperBolt {

	private static final long serialVersionUID = 299360053008956300L;

	public StatelessBolt(Fields outFields, BoltFunction f,
			String tsField, String mergerId) {
		super(outFields, new StatelessBoltFunctionWrapper(f, tsField, mergerId));
	}

}
