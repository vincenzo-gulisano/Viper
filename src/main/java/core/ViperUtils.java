package core;

import java.util.LinkedList;
import java.util.List;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ViperUtils {

	public static List<Object> getFlushTuple(int numberOfFields) {
		List<Object> result = new LinkedList<Object>();
		result.add(TupleType.FLUSH);
		result.add(Long.MAX_VALUE);
		for (int i = 0; i < numberOfFields; i++) {
			result.add(null);
		}
		return result;
	}

//	public static List<Object> getWriteLogTuple(int numberOfFields, String id) {
//		List<Object> result = new LinkedList<Object>();
//		result.add(TupleType.WRITELOG);
//		result.add(System.currentTimeMillis());
//		result.add(id);
//		for (int i = 0; i < numberOfFields; i++) {
//			result.add(null);
//		}
//		return result;
//	}

	public static Fields enrichWithBaseFields(Fields fields) {
		List<String> fieldsList = fields.toList();
		fieldsList.add(0, "type");
		fieldsList.add(1, "ts");
		//fieldsList.add(2, "sourceID");
		return new Fields(fieldsList);
	}

	public static long getTsFromText(String format, String ts) {
		return DateTimeFormat.forPattern(format).withZone(DateTimeZone.UTC)
				.parseDateTime(ts).getMillis();
	}

	public static boolean isFlushTuple(Tuple t) {
		return TupleType.FLUSH == (TupleType) t.getValueByField("type");
	}

}
