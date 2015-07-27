package core;

import java.util.LinkedList;
import java.util.List;

import clojure.lang.Obj;
import backtype.storm.tuple.Fields;

public class ViperUtils {

	public static List<Object> getFlushTuple(int numberOfFields) {
		List<Object> result = new LinkedList<Object>();
		result.add(TupleType.FLUSH);
		result.add(System.currentTimeMillis());
		for (int i = 0; i < numberOfFields; i++) {
			result.add(null);
		}
		return result;
	}
	
	public static List<Object> getWriteLogTuple(int numberOfFields) {
		List<Object> result = new LinkedList<Object>();
		result.add(TupleType.WRITELOG);
		result.add(System.currentTimeMillis());
		for (int i = 0; i < numberOfFields; i++) {
			result.add(null);
		}
		return result;
	}
	
	public static Fields enrichWithBaseFields(Fields fields) {
		List<String> fieldsList = fields.toList();
		fieldsList.add(0, "type");
		fieldsList.add(1, "ts");
		return new Fields(fieldsList);
	}
	
	public static List<Object> enrichListWithBasicFields(List<Object> values) {
		values.add(0, TupleType.REGULAR);
		values.add(1, System.currentTimeMillis());
		return values;
	}
	
}
