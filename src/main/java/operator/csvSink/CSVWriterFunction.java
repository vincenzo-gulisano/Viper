package operator.csvSink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import operator.viperBolt.BoltFunctionBase;

public class CSVWriterFunction extends BoltFunctionBase {

	private static final long serialVersionUID = 5030520764307462089L;
	private CSVFileWriter writer;

	public CSVWriterFunction(CSVFileWriter writer) {
		this.writer = writer;
	}

	@Override
	public List<Values> process(Tuple t) {
		writer.write(writer.convertTupleToLine(t));
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		String filePath = (String) stormConf.get(context.getThisComponentId()
				+ "." + context.getThisTaskIndex() + ".filepath");
		writer.setup(filePath);
	}

	@Override
	public List<Values> receivedFlush(Tuple t) {
		writer.close();
		return new ArrayList<Values>();
	}

}
