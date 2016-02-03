package statistics;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.TreeMap;

public class AvgStat extends Thread implements Serializable {

	private static final long serialVersionUID = 2415520958777993198L;

	private boolean switch_ = false;
	private long sumA;
	private long countA;
	private long sumB;
	private long countB;
	private TreeMap<Long, Long> stats;

	private boolean stop;
	private long sleepms;

	String id;

	PrintWriter out;
	boolean immediateWrite;

	public AvgStat(String id, String outputFile, boolean immediateWrite) {
		this.id = id;
		this.sumA = 0;
		this.countA = 0;
		this.sumB = 0;
		this.countB = 0;
		this.stats = new TreeMap<Long, Long>();
		this.stop = false;
		this.sleepms = 1000;
		this.immediateWrite = true;// immediateWrite;

		FileWriter outFile;
		try {
			outFile = new FileWriter(outputFile);
			out = new PrintWriter(outFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void add(long v) {
		if (switch_) {
			sumA += v;
			countA++;
		} else {
			sumB += v;
			countB++;
		}
	}

	public long getSum() {
		if (switch_)
			return sumA;
		else
			return sumB;
	}

	@Override
	public void run() {
		while (!stop) {

			long time = System.currentTimeMillis();

			switch_ = !switch_;
			if (switch_) {

				if (immediateWrite) {
					out.println(time + "," + (countB != 0 ? sumB / countB : -1));
					out.flush();
				} else {
					this.stats.put(time, countB != 0 ? sumB / countB : -1);
				}

				sumB = 0;
				countB = 0;

			} else {

				if (immediateWrite) {
					out.println(time + "," + (countA != 0 ? sumA / countA : -1));
					out.flush();
				} else {
					this.stats.put(time, countA != 0 ? sumA / countA : -1);
				}

				sumA = 0;
				countA = 0;

			}

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
				for (Entry<Long, Long> stat : stats.entrySet()) {

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
