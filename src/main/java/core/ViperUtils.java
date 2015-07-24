package core;

import java.util.LinkedList;
import java.util.List;

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
	
}
