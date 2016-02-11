package operator.viperSpout;

import java.io.Serializable;
import java.util.List;

import operator.merger.MergerEntry;
import topology.SharedChannelsScaleGate;

public abstract class InternalQueuesShuffle implements Serializable {

	private static final long serialVersionUID = 3014404246770284550L;

	private SharedChannelsScaleGate sharedChannels;

	boolean keepStats;
	String statsPath;
	String topologyName;

	public InternalQueuesShuffle(boolean keepStats, String statsPath,
			String topologyName) {

		this.keepStats = keepStats;
		this.statsPath = statsPath;
		this.topologyName = topologyName;
	}

	public abstract long getTS(List<Object> v);

	public abstract String getChannelID(List<Object> v);

	public void emit(String id, List<Object> v) {

		if (sharedChannels == null)
			sharedChannels = SharedChannelsScaleGate.factory(keepStats,
					statsPath, topologyName);

		sharedChannels
				.addObj(id, getChannelID(v), new MergerEntry(getTS(v), v));

	}

}
