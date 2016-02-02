package topology;

import java.util.List;

import operator.merger.MergerEntry;

public interface SharedChannels {

	// public SharedChannels factory();

	public void registerQueue(List<String> sources, List<String> destinations,
			String id);

	public void addObj(String source, String id, MergerEntry me);

	public MergerEntry getNextReadyObj(String destination, String id);

	// The following are support methods (because of Storm...)
	public String getChannelsID(String destination, String source);

	// public int getSize(String id);

	public void turnOff();

}