package utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

// TODO should we check for duplicates too?
public class OutputComparer {

	private LineExtractor extractorA;
	private LineExtractor extractorB;
	private String fileA;
	private String fileB;

	public OutputComparer(LineExtractor extractorA, LineExtractor extractorB,
			String fileA, String fileB) {
		this.extractorA = extractorA;
		this.extractorB = extractorB;
		this.fileA = fileA;
		this.fileB = fileB;
	}

	private TreeMap<Long, HashSet<String>> getData(LineExtractor extractorA,
			String fileA) throws IOException {
		TreeMap<Long, HashSet<String>> data = new TreeMap<Long, HashSet<String>>();
		FileInputStream fstream = new FileInputStream(fileA);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		String strLine;
		boolean firstLine = true;
		long prevTS = 0;
		String prevTuple = "";
		while ((strLine = br.readLine()) != null) {
			long ts = extractorA.getTS(strLine);
			String tuple = extractorA.getStringRepresentation(strLine);
			if (!firstLine) {
				if (ts < prevTS) {
					System.out.println("Timestamp decreased between tuple "
							+ prevTuple + " and tuple " + tuple + " !!!");
				}
			}
			firstLine = false;
			prevTS = ts;
			prevTuple = tuple;
			if (!data.containsKey(ts))
				data.put(ts, new HashSet<String>());
			data.get(ts).add(tuple);
		}
		br.close();

		return data;
	}

	private void compare(String fileA, TreeMap<Long, HashSet<String>> A,
			String fileB, TreeMap<Long, HashSet<String>> B) {
		for (long ts : A.keySet()) {
			if (!B.containsKey(ts)) {
				System.out.println("All the tuples referring to second " + ts
						+ " (" + A.get(ts).size() + ") contained in " + fileA
						+ " are not contained in file " + fileB);
			} else {
				for (String tuple : A.get(ts)) {
					if (!B.get(ts).contains(tuple))
						System.out.println("Tuple " + tuple + "contained in "
								+ fileA + " is not in file " + fileB);
				}
			}

		}
	}

	public void compareFiles() throws IOException {

		System.out.println("Loading file " + fileA);
		TreeMap<Long, HashSet<String>> dataA = getData(extractorA, fileA);
		System.out.println("Loading file " + fileB);
		TreeMap<Long, HashSet<String>> dataB = getData(extractorB, fileB);
		System.out.println("Comparing " + fileA + " --> " + fileB);
		compare(fileA, dataA, fileB, dataB);
		System.out.println("Comparing " + fileB + " --> " + fileA);
		compare(fileB, dataB, fileA, dataA);

	}

	public static void main(String[] args) throws IOException {

		OutputComparer oc = new OutputComparer(
				new LineExtractor() {

					@Override
					public long getTS(String line) {
						return Long.valueOf(line.split(";")[3]);
					}

					@Override
					public String getStringRepresentation(String line) {
						return line;
					}
				},
				new LineExtractor() {

					@Override
					public long getTS(String line) {
						return Long.valueOf(line.split(";")[3]);
					}

					@Override
					public String getStringRepresentation(String line) {
						return line;
					}
				},
				"/Users/vinmas/repositories/viper_experiments/debs2015/sm2_result.csv",
				"/Users/vinmas/repositories/viper_experiments/debs2015/sn4_result.csv");
		oc.compareFiles();

	}
}
