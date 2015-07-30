package operator.sink;

import operator.viperBolt.ViperBolt;
import backtype.storm.tuple.Fields;

public class Sink extends ViperBolt {

	private static final long serialVersionUID = 1L;

	public Sink() {
		super(new Fields(), new SinkFunction());
	}

}
