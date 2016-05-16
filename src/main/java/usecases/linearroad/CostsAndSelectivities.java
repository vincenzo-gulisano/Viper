package usecases.linearroad;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import usecases.linearroad.DetectAccidentOperator.UpdateSegmentAnswer;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Values;

public class CostsAndSelectivities {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		String statsPath = args[2];
		final String input_data = args[8];

		try {

			// Preparation
			DecimalFormat myFormatter = new DecimalFormat("###.##########");
			PrintWriter out = new PrintWriter(new FileWriter(statsPath
					+ File.separator + "log.txt"));

			ArrayList<String> input_tuples = new ArrayList<String>();
			FileInputStream fstream = new FileInputStream(input_data);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fstream));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				input_tuples.add(strLine);
			}
			br.close();
			double repetitions = 100000000;
			double outputs = 0;
			ArrayList<LRTuple> inputTuplesConverted = new ArrayList<LRTuple>();
			for (String s : input_tuples) {
				inputTuplesConverted.add(new LRTuple(s));
			}

			// /////////////// STATEFULVEHICLEDETECTACCIDENT ///////////////

			DetectAccidentOperator detectAccOperator = new DetectAccidentOperator();

			long before = System.currentTimeMillis();
			for (int i = 0; i < repetitions; i++) {
				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());

				List<Values> results = new ArrayList<Values>();
				if (lrTuple.type == 0) {

					UpdateSegmentAnswer result = detectAccOperator.run(
							lrTuple.xway, lrTuple.seg, lrTuple.time,
							lrTuple.vid, lrTuple.speed, lrTuple.lane,
							lrTuple.pos);

					results.add(new Values(lrTuple.type, lrTuple.time,
							lrTuple.vid, lrTuple.speed, lrTuple.xway,
							lrTuple.lane, lrTuple.dir, lrTuple.seg,
							lrTuple.pos, result.newacc, result.cleared));
					outputs++;

				}
			}
			long after = System.currentTimeMillis();
			out.println("StatefulVehicleDetectAccident - cost: "
					+ myFormatter.format((after - before) / repetitions)
					+ " selectivity: "
					+ myFormatter.format((outputs / repetitions)));

			// Spout
			outputs = 0;
			long repetition = 0;
			long timeStep = 60 * 60 * 3;
			before = System.currentTimeMillis();
			for (int i = 0; i < repetitions; i++) {
				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());

				Values result = new Values(lrTuple.type, lrTuple.time
						+ repetition * timeStep, lrTuple.vid, lrTuple.speed,
						lrTuple.xway, lrTuple.lane, lrTuple.dir, lrTuple.seg,
						lrTuple.pos);

				// Force time to increase even when looping on input tuples.
				if (i % input_tuples.size() == 0)
					repetition++;

				// Just to prevent compiler optimizations (hopefully)
				result.size();
				outputs++;

			}
			after = System.currentTimeMillis();
			out.println("Spout - cost: "
					+ myFormatter.format((after - before) / repetitions)
					+ " selectivity: "
					+ myFormatter.format((outputs / repetitions)));

			// StatefulVehicleEnteringNewSegment
			repetition = 0;
			DetectNewVehicles detectNewVehicles = new DetectNewVehicles();
			outputs = 0;
			before = System.currentTimeMillis();
			for (int i = 0; i < repetitions; i++) {
				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());

				List<Values> results = new ArrayList<Values>();
				if (lrTuple.type == 0) {
					results.add(new Values(lrTuple.type, lrTuple.time
							+ repetition * timeStep, lrTuple.vid,
							lrTuple.speed, lrTuple.xway, lrTuple.lane,
							lrTuple.dir, lrTuple.seg, lrTuple.pos,
							detectNewVehicles.isThisANewVehicle(lrTuple.time
									+ repetition * timeStep, lrTuple.xway,
									lrTuple.seg, lrTuple.vid)));
					outputs++;
				}

				// Just to prevent compiler optimizations (hopefully)
				results.size();

				if ((i + 1) % input_tuples.size() == 0)
					repetition++;

			}
			after = System.currentTimeMillis();
			out.println("StatefulVehicleEnteringNewSegment - cost: "
					+ myFormatter.format((after - before) / repetitions)
					+ " selectivity: "
					+ myFormatter.format((outputs / repetitions)));

			// StatelessForwardPositionReportsOnly
			outputs = 0;
			before = System.currentTimeMillis();
			for (int i = 0; i < repetitions; i++) {
				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());

				List<Values> results = new ArrayList<Values>();
				if (lrTuple.type == 0) {
					results.add(new Values(lrTuple.type, lrTuple.time,
							lrTuple.vid, lrTuple.speed, lrTuple.xway,
							lrTuple.lane, lrTuple.dir, lrTuple.seg, lrTuple.pos));
					outputs++;
				}

				// Just to prevent compiler optimizations (hopefully)
				results.size();

			}
			after = System.currentTimeMillis();
			out.println("StatelessForwardPositionReportsOnly - cost: "
					+ myFormatter.format((after - before) / repetitions)
					+ " selectivity: "
					+ myFormatter.format((outputs / repetitions)));

			// StatelessForwardStoppedCarsOnly
			outputs = 0;
			before = System.currentTimeMillis();
			for (int i = 0; i < repetitions; i++) {
				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());

				List<Values> results = new ArrayList<Values>();
				if (lrTuple.type == 0 && lrTuple.speed == 0) {
					results.add(new Values(lrTuple.type, lrTuple.time,
							lrTuple.vid, lrTuple.speed, lrTuple.xway,
							lrTuple.lane, lrTuple.dir, lrTuple.seg, lrTuple.pos));
					outputs++;
				}

				// Just to prevent compiler optimizations (hopefully)
				results.size();

			}
			after = System.currentTimeMillis();
			out.println("StatelessForwardStoppedCarsOnly - cost: "
					+ myFormatter.format((after - before) / repetitions)
					+ " selectivity: "
					+ myFormatter.format((outputs / repetitions)));
			out.flush();
			out.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
