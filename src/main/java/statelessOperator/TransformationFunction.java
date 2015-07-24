package statelessOperator;

import backtype.storm.tuple.Tuple;

public interface TransformationFunction {

	public Tuple transform(Tuple t);
	
}
