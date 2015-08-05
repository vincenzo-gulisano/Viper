package core;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ViperValues extends Values {

	private static final long serialVersionUID = 4298328617519483517L;

	public ViperValues() {
	}

	public ViperValues(Object... vals) {
		super(vals);
	}

	public ViperValues(Tuple t) {
		for (Object o : t.getValues()) {
			add(o);
		}
	}


}
