package operators.baseSpout;

import java.io.Serializable;

import backtype.storm.tuple.Values;

public interface SpoutFunction extends Serializable {
	Values getTuple();
	boolean hasNext();
}
