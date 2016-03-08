package operator.viperSpout;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import operator.merger.MergerEntry;
import operator.viperBolt.BoltFunction;
import operator.viperBolt.ViperBolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import statistics.AvgStat;
import statistics.CountStat;
import topology.SharedChannelsScaleGate;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import core.TupleType;
import core.ViperUtils;

/*
 * THIS IS REALLY UGLY CODE AND NO STATS MANAGED HERE
 */
public class ViperSpoutBoltWrapper extends BaseRichSpout {

	private static final long serialVersionUID = 8693720878488229181L;

	public static Logger LOG = LoggerFactory.getLogger(ViperBolt.class);

	private Fields outFields;
	// protected SpoutOutputCollector collector;
	private boolean keepStats;
	private String statsPath;
	private CountStat countStat;
	private AvgStat costStat;
	private CountStat invocationsStat;
	protected BoltFunction f;
	protected int thisTaskIndex;
	protected int thisTask;
	protected String id;

	private boolean stillNeedToConfigure = true;

	TopologyContext context;
	String streamId;

	// private boolean useInternalQueues;
	private SharedChannelsScaleGate sharedChannels;
	private String channelID;

	InternalQueuesShuffle iq;
	String prevOp;

	// int consecutiveCalls = 10;
	// int thisCall = 0;

	public ViperSpoutBoltWrapper(Fields outFields, BoltFunction boltFunction,
			InternalQueuesShuffle iq, String prevOp) {

		this.outFields = ViperUtils.enrichWithBaseFields(outFields);
		this.f = boltFunction;
		this.iq = iq;
		this.prevOp = prevOp;

	}

	@SuppressWarnings("rawtypes")
	protected void childPrepare(Map stormConf, TopologyContext context,
			SpoutOutputCollector collector) {
	}

	protected void emit(List<Object> input, Values t) {
		t.add(0, TupleType.REGULAR);
		t.add(1, input.get(1));
		this.iq.emit(id, t);
	}

	// startTS is for cost estimation
	private void process(List<Object> v) {
		List<Values> result = f.process(v);
		if (result != null)
			for (Values out : result) {
				emit(v, out);
				if (keepStats) {
					countStat.increase(1);
				}
			}
	}

	@SuppressWarnings("unchecked")
	private void takeFromInternalBuffer() {

		MergerEntry nextReady = sharedChannels.getNextReadyObj(id, channelID);
		while (nextReady != null) {

			process((List<Object>) nextReady.getO());
			nextReady = sharedChannels.getNextReadyObj(id, channelID);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.outFields);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.context = context;

		Object temp = conf.get("log.statistics");
		this.keepStats = temp != null ? (Boolean) temp : false;

		temp = conf.get("log.statistics.path");
		this.statsPath = temp != null ? (String) temp : "";

		// temp = conf.get("internal.queues");
		// this.useInternalQueues = temp != null ? (Boolean) temp : false;

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
					+ conf.get(Config.TOPOLOGY_NAME) + "_" + id + ".rate.csv",
					false);
			costStat = new AvgStat("", statsPath + File.separator
					+ conf.get(Config.TOPOLOGY_NAME) + "_" + id + ".cost.csv",
					false);
			invocationsStat = new CountStat("", statsPath + File.separator
					+ conf.get(Config.TOPOLOGY_NAME) + "_" + id
					+ ".invocations.csv", false);
			// countStat.start();
			// costStat.start();
			// invocationsStat.start();
		}

		f.prepare(conf, context);

		childPrepare(conf, context, collector);

		// if (this.useInternalQueues) {

		LOG.info("Bolt " + id + " starting internal queues setup");

		sharedChannels = SharedChannelsScaleGate.factory(keepStats, statsPath,
				(String) conf.get(Config.TOPOLOGY_NAME));

		LinkedList<String> sources = new LinkedList<String>();
		LinkedList<String> destinations = new LinkedList<String>();

		channelID = "" + id;

		destinations.add(channelID);
		LOG.info("Bolt " + id + " is fed by:");

		LOG.info("Previous operator " + prevOp + " has "
				+ context.getComponentTasks(prevOp).size() + " instances ");
		for (int j = 0; j < context.getComponentTasks(prevOp).size(); j++) {
			sources.add(prevOp + "." + j);
			channelID += "_" + prevOp + "." + j;
		}

		sharedChannels.registerQueue(sources, destinations, channelID);

		LOG.info("Bolt " + id + " internal queues setup completed");

		// }

		stillNeedToConfigure = false;

	}

	@Override
	public void nextTuple() {

		if (stillNeedToConfigure) {
			LOG.info("!!! Calling execute on a bolt that still needs to complete configuration !!!");
			while (stillNeedToConfigure) {
				Utils.sleep(100);
			}
		}

		long start = System.nanoTime();
		if (keepStats)
			invocationsStat.increase(1);

		takeFromInternalBuffer();

		if (keepStats) {
			costStat.add((System.nanoTime() - start));
		}

		// thisCall++;
		// if (thisCall == consecutiveCalls)
		// thisCall = 0;
		// else
		// nextTuple();
	}

}
