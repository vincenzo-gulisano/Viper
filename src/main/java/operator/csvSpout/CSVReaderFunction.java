package operator.csvSpout;

import java.util.Map;

import core.ViperValues;
import operator.viperSpout.SpoutFunction;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

public class CSVReaderFunction implements SpoutFunction {

	private static final long serialVersionUID = 2612121254607940790L;
	private CSVFileReader reader;
	private String filePath;

	public CSVReaderFunction(CSVFileReader reader) {
		this.reader = reader;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		filePath = (String) stormConf.get(context.getThisComponentId() + "."
				+ context.getThisTaskIndex() + ".filepath");
		reader.setup(filePath);
	}

	@Override
	public Values getTuple() {
		return reader.getNextTuple();
	}

	@Override
	public boolean hasNext() {
		return reader.hasNext();
	}

}
