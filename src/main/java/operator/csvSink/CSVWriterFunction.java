package operator.csvSink;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import operator.viperBolt.BoltFunctionBase;

public class CSVWriterFunction extends BoltFunctionBase {

	private static final long serialVersionUID = 5030520764307462089L;
	private CSVFileWriter writer;
	private boolean keepStats; // TODO does it make sense to have this
								// duplicated here?
	private String statsPath; // TODO does it make sense to have this duplicated
								// here?
	private String topologyID; // TODO does it make sense to have this
								// duplicated here?
	private int taskIndex;

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
		this.topologyID = (String) stormConf.get(Config.TOPOLOGY_NAME);

		Object temp = stormConf.get("log.statistics");
		keepStats = temp != null ? (Boolean) temp : false;
		temp = stormConf.get("log.statistics.path");
		statsPath = temp != null ? (String) temp : "";

		taskIndex = context.getThisTaskIndex();

	}

	@Override
	public List<Values> receivedFlush(Tuple t) {
		writer.close();
		if (keepStats) {
			String messageFilePath = statsPath + File.separator + topologyID
					+ "." + taskIndex;
			try {
				PrintWriter pw = new PrintWriter(
						new FileWriter(messageFilePath));
				pw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return new ArrayList<Values>();
	}
}
