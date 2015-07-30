package operator.viperBolt;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import statistics.CountStat;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import core.TupleType;
import core.ViperUtils;

public class ViperBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8693720878488229181L;

	public static Logger LOG = LoggerFactory.getLogger(ViperBolt.class);

	private Fields outFields;
	protected OutputCollector collector;
	private boolean keepStats;
	private boolean statsWritten = false;
	private String statsPath;
	private CountStat countStat;
	private BoltFunction f;
	protected int thisTaskIndex;
	protected String id;

	public ViperBolt(Fields outFields, BoltFunction boltFunction) {

		this.outFields = ViperUtils.enrichWithBaseFields(outFields);
		this.f = boltFunction;

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

		this.keepStats = (Boolean) stormConf.getOrDefault("log.statistics",
				false);
		this.statsPath = (String) stormConf.getOrDefault("log.statistics.path",
				"");

		LOG.info("Bolt preparation, component id: "
				+ context.getThisComponentId() + ", task id: "
				+ context.getThisTaskId() + ", task index: "
				+ context.getThisTaskIndex());

		thisTaskIndex = context.getThisTaskIndex();
		id = context.getThisComponentId() + "." + context.getThisTaskIndex();
		if (keepStats) {

			countStat = new CountStat("", statsPath + File.separator + id
					+ ".rate.csv", false);
			countStat.start();
		}

		f.prepare(stormConf, context);

		childPrepare(stormConf, context, collector);

	}

	@SuppressWarnings("rawtypes")
	protected void childPrepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

	}

	protected void emit(Tuple input, Values t) {

		t.add(0, TupleType.REGULAR);
		t.add(1, input.getLongByField("ts"));
		t.add(2, id);

		collector.emit(t);
	}

	protected void emitFlush(Tuple t) {
		collector.emit(t.getValues());
	}

	protected void emitWriteLog(Tuple t) {
		collector.emit(t.getValues());
	}

	public void execute(Tuple input) {

		TupleType ttype = (TupleType) input.getValueByField("type");
		if (ttype.equals(TupleType.REGULAR)) {

			// LOG.info("Bolt " + id + " received tuple " + input);

			List<Values> result = f.process(input);
			if (result != null)
				for (Values t : result) {
					emit(input, t);
					if (keepStats) {
						countStat.increase(1);
					}
				}
			collector.ack(input);
		} else if (ttype.equals(TupleType.FLUSH)) {
			f.receivedFlush(input);
			emitFlush(input);
		} else if (ttype.equals(TupleType.WRITELOG)) {
			f.receivedWriteLog(input);

			if (keepStats && !statsWritten) {
				statsWritten = true;
				Utils.sleep(2000); // Just wait for latest stats to be written
				countStat.stopStats();
				try {
					countStat.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				countStat.writeStats();
			}

			emitFlush(input);

		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.outFields);
	}

}
