package operator.csvSpout;

import operator.viperSpout.ViperSpout;
import backtype.storm.tuple.Fields;

public class FixedRateCSVReaderSpout extends ViperSpout {

	private static final long serialVersionUID = 4560902349034444575L;

	public FixedRateCSVReaderSpout(CSVFileReader reader, Fields outFields,
			long desiredRate, long maxDuration) {
		super(new FixedRateCSVReaderFunction(reader, desiredRate, maxDuration),
				outFields);
	}

}
