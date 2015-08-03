package statelessOperator;

import java.io.Serializable;

import operator.viperBolt.BoltFunction;

public interface BoltFunctionFactory extends Serializable {

	public BoltFunction getBoltFunction();
	
}
