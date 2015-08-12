package statelessOperator;

import operator.viperBolt.ViperBolt;
import backtype.storm.tuple.Fields;

public class SharedMemoryStateless extends ViperBolt {

	private static final long serialVersionUID = 299360053008956300L;

	public SharedMemoryStateless(Fields outFields, BoltFunctionFactory factory,
			int numberOfThreads, String tsField) {
		super(outFields, new StatelessBoltFunctionWrapper(factory,
				numberOfThreads, tsField));
	}

}
