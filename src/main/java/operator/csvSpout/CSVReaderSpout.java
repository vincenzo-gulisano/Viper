package operator.csvSpout;

import operator.viperSpout.ViperSpout;
import backtype.storm.tuple.Fields;

public class CSVReaderSpout extends ViperSpout {

	private static final long serialVersionUID = 4560902349034444575L;

	public CSVReaderSpout(CSVFileReader reader, Fields outFields) {
		super(new CSVReaderFunction(reader), outFields);
	}

}
