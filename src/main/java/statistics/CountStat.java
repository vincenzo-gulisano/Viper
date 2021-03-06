package statistics;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.TreeMap;

public class CountStat extends Thread implements Serializable {

	private static final long serialVersionUID = 2415520958777993198L;

	private long count;
	private TreeMap<Long, Long> countStats;

	private boolean stop;
	private long sleepms;

	String id;

	PrintWriter out;
	boolean immediateWrite;

	public CountStat(String id, String outputFile, boolean immediateWrite) {
		this.id = id;
		this.count = 0;
		this.countStats = new TreeMap<Long, Long>();
		this.stop = false;
		this.sleepms = 1000;
		this.immediateWrite = immediateWrite;

		FileWriter outFile;
		try {
			outFile = new FileWriter(outputFile);
			out = new PrintWriter(outFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void increase(long v) {
		count += v;
	}

	@Override
	public void run() {
		while (!stop) {

			long time = System.currentTimeMillis();

			if (immediateWrite) {
				out.println(time + "," + count);
				out.flush();
			} else {
				this.countStats.put(time, count);
			}

			count = 0;

			long stop = System.currentTimeMillis();
			if ((sleepms - (stop - time)) > 0)
				try {
					Thread.sleep((sleepms - (stop - time)));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

		}
	}

	public void stopStats() {
		this.stop = true;
	}

	public void writeStats() {
		if (!immediateWrite) {
			try {
				for (Entry<Long, Long> stat : countStats.entrySet()) {

					long time = stat.getKey();
					long counter = stat.getValue();

					out.println(time + "," + counter);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		out.flush();
		out.close();

	}

}
