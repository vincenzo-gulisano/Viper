package statelessOperator;

import operator.viperSpout.ViperSpout;
import backtype.storm.tuple.Fields;

public class StatelessSpout extends ViperSpout {

	private static final long serialVersionUID = 8830935027450437181L;

	public StatelessSpout(Fields outFields,String mergerId) {
		super(new StatelessSpoutFunctionWrapper(mergerId), outFields);
	}

}
