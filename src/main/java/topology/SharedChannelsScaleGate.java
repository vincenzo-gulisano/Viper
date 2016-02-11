package topology;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import operator.merger.MergerEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scalegate.SGTupleContainer;
import scalegate.ScaleGate;
import scalegate.ScaleGateAArrImpl;
import statistics.CountStat;
import backtype.storm.Config;

public class SharedChannelsScaleGate implements SharedChannels {

	public static Logger LOG = LoggerFactory
			.getLogger(SharedChannelsScaleGate.class);

	private static SharedChannelsScaleGate thisSharedChannels;
	private static Lock l = new ReentrantLock();

	private Map<String, ScaleGate> channels = new HashMap<String, ScaleGate>();
	private Map<String, Map<String, Integer>> channelsSourcesMap = new HashMap<String, Map<String, Integer>>();
	private Map<String, Map<String, Integer>> channelsDestinationsMap = new HashMap<String, Map<String, Integer>>();
	// private Map<String, Integer> channelsSizes = new HashMap<String,
	// Integer>();
	private Map<String, CountStat> channelSize = new HashMap<String, CountStat>();
	private boolean keepStats;
	private String statsPath;
	private String topologyName;

	// For support method
	private Map<String, Map<String, String>> destinationSouceMapping = new HashMap<String, Map<String, String>>();

	private SharedChannelsScaleGate() {
	}

	public static SharedChannelsScaleGate factory(boolean keepStats,
			String statsPath, String topologyName) {
		if (thisSharedChannels == null) {
			l.lock();

			try {
				thisSharedChannels = new SharedChannelsScaleGate();

				thisSharedChannels.keepStats = keepStats;

				thisSharedChannels.statsPath = statsPath;

				thisSharedChannels.topologyName = topologyName;

			} finally {
				l.unlock();
			}
		}
		return thisSharedChannels;
	}

	@Override
	public void registerQueue(List<String> sources, List<String> destinations,
			String id) {
		l.lock();
		try {

			LOG.info("Registering channel for sources: " + sources
					+ ", destinations: " + destinations + " and id " + id);

			if (this.channels.containsKey(id))
				throw new RuntimeException(
						"Registering already existing channel " + id);

			// Create ScaleGate
			ScaleGate sg = new ScaleGateAArrImpl(3, sources.size(),
					destinations.size());

			channelsSourcesMap.put(id, new HashMap<String, Integer>());
			int i = 0;
			for (String s : sources) {
				channelsSourcesMap.get(id).put(s, i);
				sg.addTuple(new SGTupleContainer(), i);
				i++;
			}

			channelsDestinationsMap.put(id, new HashMap<String, Integer>());
			i = 0;
			for (String d : destinations) {
				channelsDestinationsMap.get(id).put(d, i);
				i++;
			}

			for (String d : destinations) {

				if (!destinationSouceMapping.containsKey(destinations))
					destinationSouceMapping.put(d,
							new HashMap<String, String>());

				for (String s : sources) {

					if (!destinationSouceMapping.get(d).containsKey(s))
						destinationSouceMapping.get(d).put(s, id);
					else
						throw new RuntimeException("Cannot register channel "
								+ id + " for destination " + d + " and source "
								+ s + "... already registered as "
								+ destinationSouceMapping.get(d).get(s));

					LOG.info("Source " + s + " to destination " + d
							+ " associated to " + id);

				}
			}

			// channelsSizes.put(id, 0);
			if (keepStats) {
				channelSize.put(id, new CountStat("", statsPath
						+ File.separator + topologyName + "_" + id
						+ ".rate.csv", true));
				channelSize.get(id).start();
			}
			channels.put(id, sg);

		} finally {
			l.unlock();
		}
	}

	@Override
	public void addObj(String source, String id, MergerEntry me) {

		if (!this.channels.containsKey(id)) {
			for (String s : this.channels.keySet())
				LOG.info("known channel: " + s);
			throw new RuntimeException("Adding to unkown channel " + id);
		}

		if (!this.channelsSourcesMap.get(id).containsKey(source))
			throw new RuntimeException("Unknown source " + source
					+ " adding to channel " + id);

		// // TODO HARDCODED!
		// if (getSize(id) > 1000)
		// Utils.sleep(1);

		this.channels.get(id).addTuple(new SGTupleContainer(me),
				this.channelsSourcesMap.get(id).get(source));

		if (keepStats)
			channelSize.get(id).increase(+1);

	}

	@Override
	public MergerEntry getNextReadyObj(String source, String id) {

		if (!this.channels.containsKey(id))
			throw new RuntimeException("Getting from unkown channel " + id);

		if (!this.channelsDestinationsMap.get(id).containsKey(source))
			throw new RuntimeException("Unknown source " + source
					+ " getting from channel " + id);

		SGTupleContainer t = (SGTupleContainer) this.channels.get(id)
				.getNextReadyTuple(
						this.channelsDestinationsMap.get(id).get(source));

		if (t == null) {
			return null;
		}

		// This is not optimal. At this point, t could be a dummy tuple inserted
		// in the beginning at we could try to get the next one. In principle,
		// nevertheless, this will create some extra latency only in the very
		// beginning...

		if (keepStats)
			channelSize.get(id).increase(-1);

		return t.getME();

	}

	// public int getSize(String id) {
	//
	// if (!this.channels.containsKey(id))
	// throw new RuntimeException("Asking size of unkown channel " + id);
	//
	// return channelsSizes.get(id);
	//
	// }

	@Override
	public String getChannelsID(String destination, String source) {

		if (destinationSouceMapping.containsKey(destination)
				&& destinationSouceMapping.get(destination).containsKey(source))
			return destinationSouceMapping.get(destination).get(source);

		throw new RuntimeException("Unkown channel for destination "
				+ destination + " and source " + source);

	}

	@Override
	public void turnOff() {
		if (keepStats) {
			for (String id : channels.keySet()) {
				channelSize.get(id).stopStats();
				try {
					channelSize.get(id).join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				channelSize.get(id).writeStats();
			}
		}

	}

}
