package operator.viperBolt;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import operator.merger.MergerEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import statistics.AvgStat;
import statistics.CountStat;
import topology.SharedChannelsScaleGate;
import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
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
	private AvgStat costStat;
	private CountStat invocationsStat;
	protected BoltFunction f;
	protected int thisTaskIndex;
	protected int thisTask;
	protected String id;
	private int counter = 0;

	private boolean stillNeedToConfigure = true;

	TopologyContext context;
	String streamId;

	private boolean useInternalQueues;
	private SharedChannelsScaleGate sharedChannels;
	private String channelID;

	// temp
	// private Values lastEmit;

	public ViperBolt(Fields outFields, BoltFunction boltFunction) {

		this.outFields = ViperUtils.enrichWithBaseFields(outFields);
		this.f = boltFunction;

	}

	@SuppressWarnings({ "rawtypes" })
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.context = context;
		this.collector = collector;

		Object temp = stormConf.get("log.statistics");
		this.keepStats = temp != null ? (Boolean) temp : false;

		temp = stormConf.get("log.statistics.path");
		this.statsPath = temp != null ? (String) temp : "";

		temp = stormConf.get("internal.queues");
		this.useInternalQueues = temp != null ? (Boolean) temp : false;

		LOG.info("Bolt preparation, component id: "
				+ context.getThisComponentId() + ", task id: "
				+ context.getThisTaskId() + ", task index: "
				+ context.getThisTaskIndex());

		thisTask = context.getThisTaskId();
		streamId = context.getThisComponentId();

		thisTaskIndex = context.getThisTaskIndex();
		id = context.getThisComponentId() + "." + context.getThisTaskIndex();
		if (keepStats) {
			countStat = new CountStat("", statsPath + File.separator
					+ stormConf.get(Config.TOPOLOGY_NAME) + "_" + id
					+ ".rate.csv", false);
			costStat = new AvgStat("", statsPath + File.separator
					+ stormConf.get(Config.TOPOLOGY_NAME) + "_" + id
					+ ".cost.csv", false);
			invocationsStat = new CountStat("", statsPath + File.separator
					+ stormConf.get(Config.TOPOLOGY_NAME) + "_" + id
					+ ".invocations.csv", false);
			countStat.start();
			costStat.start();
			invocationsStat.start();
		}

		f.prepare(stormConf, context);

		childPrepare(stormConf, context, collector);

		if (this.useInternalQueues) {

			LOG.info("Bolt " + id + " starting internal queues setup");

			sharedChannels = SharedChannelsScaleGate.factory(keepStats,
					statsPath, (String) stormConf.get(Config.TOPOLOGY_NAME));

			// TODO Should check whether there's only 1 source!!!
			GlobalStreamId globalStreamId = (GlobalStreamId) context
					.getThisSources().keySet().toArray()[0];
			List<Integer> componentIds = context
					.getComponentTasks(globalStreamId.get_componentId());

			LinkedList<String> sources = new LinkedList<String>();
			LinkedList<String> destinations = new LinkedList<String>();

			channelID = "" + thisTask;

			destinations.add(channelID);
			LOG.info("Bolt " + id + " is fed by:");

			for (Integer i : componentIds) {
				LOG.info("" + i);
				sources.add("" + i);
				channelID += "_" + i;
			}

			sharedChannels.registerQueue(sources, destinations, channelID);

			LOG.info("Bolt " + id + " internal queues setup completed");

		}

		stillNeedToConfigure = false;

	}

	@SuppressWarnings("rawtypes")
	protected void childPrepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
	}

	// TODO this will not work for stateful operators like the aggregate that
	// already set a timestamp, right?
	protected void emit(Tuple input, Values t) {
		t.add(0, TupleType.REGULAR);
		t.add(1, input.getLongByField("ts"));
		collector.emit(t);
	}

	protected void emitFlush(Tuple t) {
		collector.emit(ViperUtils.getFlushTuple(this.outFields.size() - 2));
	}

	// protected void emitDummy(Tuple t) {
	// collector.emit(ViperUtils.getDummyTuple(this.outFields.size() - 2));
	// }

	// startTS is for cost estimation
	private void process(Tuple t) {
		counter++;
		List<Values> result = f.process(t);
		if (result != null)
			for (Values out : result) {
				emit(t, out);
				if (keepStats) {
					countStat.increase(1);
				}
			}
		// if (keepStats) {
		// costStat.add((System.nanoTime() - startTS));
		// }
	}

	public Map<String, Object> getComponentConfiguration() {

		// if (stillNeedToConfigure)
		// LOG.error("Getting Viper bolt configuration but configuration did not start yet...");
		// else {
		// LOG.info("getComponentConfiguration correctly called once configuration has been initiated");
		// }

		// configure how often a tick tuple will be sent to our bolt
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 0.001D);
		return conf;

	}

//	List<Long> last1000ticks = new LinkedList<Long>();

	protected boolean isTickTuple(Tuple tuple) {

		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(
						Constants.SYSTEM_TICK_STREAM_ID);
	}

	@SuppressWarnings("unchecked")
	private void takeFromInternalBuffer() {

		MergerEntry nextReady = sharedChannels.getNextReadyObj("" + thisTask,
				channelID);
		while (nextReady != null) {
			// LOG.info("ViperBolt " + id
			// + " received tuple from internal channel");
			// Tuple t = new TupleImpl(context, (List<Object>) nextReady.getO(),
			// input.getSourceTask(), input.getSourceStreamId());
			// LOG.info("ViperBolt " + id + " received tuple: " + t);

			process(new TupleImpl(context, (List<Object>) nextReady.getO(),
					nextReady.getTaskId(), nextReady.getStreamId()));
			nextReady = sharedChannels
					.getNextReadyObj("" + thisTask, channelID);
		}

		// if (keepStats) {
		// costStat.add((System.nanoTime() - startTS));
		// }

	}

	public void execute(Tuple input) {

		if (stillNeedToConfigure) {
			LOG.info("!!! Calling execute on a bolt that still needs to complete configuration !!!");
			while (stillNeedToConfigure) {
				Utils.sleep(100);
			}
		}

		long start = System.nanoTime();
		if (keepStats)
			invocationsStat.increase(1);

		if (useInternalQueues && isTickTuple(input))
			takeFromInternalBuffer();

		if (!isTickTuple(input)) {
			TupleType ttype = (TupleType) input.getValueByField("type");

			// if (ttype.equals(TupleType.SHAREDQUEUEDUMMY)) {
			//
			// emitDummy(input);
			//
			// if (keepStats) {
			// costStat.add((System.nanoTime() - start));
			// }
			//
			// } else
			if (ttype.equals(TupleType.REGULAR)) {

				process(input);

				if (keepStats) {
					costStat.add((System.nanoTime() - start));
				}

			} else if (ttype.equals(TupleType.FLUSH)) {

				LOG.info("ViperBolt " + id + " received FLUSH from "
						+ input.getSourceComponent() + ":"
						+ input.getSourceTask());

				List<Values> result = f.receivedFlush(input);
				if (result != null) {

					for (Values t : result) {
						if (t != null) {
							emit(input, t);
							if (keepStats) {
								countStat.increase(1);
							}
						}
					}

					LOG.info("ViperBolt " + id + " received " + counter
							+ " tuples before sending FLUSH");
					emitFlush(input);

					if (keepStats && !statsWritten) {
						statsWritten = true;
						Utils.sleep(2000); // Just wait for latest stats to be
											// written
						countStat.stopStats();
						costStat.stopStats();
						invocationsStat.stopStats();
						try {
							countStat.join();
							costStat.join();
							invocationsStat.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						countStat.writeStats();
						costStat.writeStats();
						invocationsStat.writeStats();
						keepStats = false;

						if (this.useInternalQueues)
							sharedChannels.turnOff();

					}
				}
			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.outFields);
	}

}
