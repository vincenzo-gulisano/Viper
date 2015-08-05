package operator.csvSink;

import operator.viperBolt.ViperBolt;
import backtype.storm.tuple.Fields;

public class CSVSink extends ViperBolt {

	private static final long serialVersionUID = 3738180711237548160L;

	public CSVSink(CSVFileWriter writer) {
		super(new Fields(), new CSVWriterFunction(writer));
	}

}
