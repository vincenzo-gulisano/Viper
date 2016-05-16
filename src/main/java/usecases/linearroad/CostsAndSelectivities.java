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

			long repetition = 0;
			long timeStep = 60 * 60 * 3;

			// /////////////// STATEFULVEHICLEDETECTACCIDENT ///////////////
			DetectAccidentOperator detectAccOperator = new DetectAccidentOperator();

			long duration = 0;
			for (int i = 0; i < repetitions; i++) {

				if (i > 0 && i % input_tuples.size() == 0)
					repetition++;

				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());

				lrTuple.time += repetition * timeStep;

				long before = System.nanoTime();
				List<Values> results = new ArrayList<Values>();
				if (lrTuple.type == 0) {

					UpdateSegmentAnswer result = detectAccOperator.run(
							lrTuple.xway, lrTuple.seg, lrTuple.time,
							lrTuple.vid, lrTuple.speed, lrTuple.lane,
							lrTuple.pos);

					if (result.cleared || result.newacc) {
						results.add(new Values(lrTuple.type, lrTuple.time,
								lrTuple.vid, lrTuple.speed, lrTuple.xway,
								lrTuple.lane, lrTuple.dir, lrTuple.seg,
								lrTuple.pos, result.newacc, result.cleared));
						outputs++;
					}

				}
				duration += System.nanoTime() - before;
			}
			out.println("StatefulVehicleDetectAccident - cost: "
					+ myFormatter.format(duration / repetitions)
					+ " selectivity: "
					+ myFormatter.format((outputs / repetitions)));

			// /////////////// STATEFULVEHICLEENTERINGNEWSEGMENT ///////////////
			DetectNewVehicles detectNewVehicles = new DetectNewVehicles();

			duration = 0;
			repetition = 0;
			outputs = 0;
			for (int i = 0; i < repetitions; i++) {

				if (i > 0 && i % input_tuples.size() == 0)
					repetition++;

				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());

				lrTuple.time += repetition * timeStep;

				long before = System.nanoTime();
				List<Values> results = new ArrayList<Values>();
				if (lrTuple.type == 0) {

					boolean newVehicle = detectNewVehicles.isThisANewVehicle(
							lrTuple.time, lrTuple.xway, lrTuple.seg,
							lrTuple.vid);
					if (newVehicle) {
						results.add(new Values(lrTuple.type, lrTuple.time,
								lrTuple.vid, lrTuple.speed, lrTuple.xway,
								lrTuple.lane, lrTuple.dir, lrTuple.seg,
								lrTuple.pos, newVehicle));
						outputs++;
					}

				}
				duration += System.nanoTime() - before;
			}
			out.println("StatefulVehicleEnteringNewSegment - cost: "
					+ myFormatter.format(duration / repetitions)
					+ " selectivity: "
					+ myFormatter.format((outputs / repetitions)));

			// /////////////// STATELESSFORWARDPOSITIONREPORTSONLY
			// ///////////////

			duration = 0;
			repetition = 0;
			outputs = 0;
			for (int i = 0; i < repetitions; i++) {

				if (i > 0 && i % input_tuples.size() == 0)
					repetition++;

				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());

				lrTuple.time += repetition * timeStep;

				long before = System.nanoTime();
				List<Values> results = new ArrayList<Values>();
				if (lrTuple.type == 0) {
					results.add(new Values(lrTuple.type, lrTuple.time,
							lrTuple.vid, lrTuple.speed, lrTuple.xway,
							lrTuple.lane, lrTuple.dir, lrTuple.seg, lrTuple.pos));
					outputs++;

				}
				duration += System.nanoTime() - before;
			}
			out.println("StatelessForwardPositionReportsOnly - cost: "
					+ myFormatter.format(duration / repetitions)
					+ " selectivity: "
					+ myFormatter.format((outputs / repetitions)));

			// /////////////// STATELESSFORWARDSTOPPEDCARSONLY
			// ///////////////

			duration = 0;
			repetition = 0;
			outputs = 0;
			for (int i = 0; i < repetitions; i++) {

				if (i > 0 && i % input_tuples.size() == 0)
					repetition++;

				LRTuple lrTuple = inputTuplesConverted.get(i
						% input_tuples.size());

				lrTuple.time += repetition * timeStep;

				long before = System.nanoTime();
				List<Values> results = new ArrayList<Values>();
				if (lrTuple.type == 0 && lrTuple.speed == 0) {
					results.add(new Values(lrTuple.type, lrTuple.time,
							lrTuple.vid, lrTuple.speed, lrTuple.xway,
							lrTuple.lane, lrTuple.dir, lrTuple.seg, lrTuple.pos));
					outputs++;

				}
				duration += System.nanoTime() - before;
			}
			out.println("StatelessForwardStoppedCarsOnly - cost: "
					+ myFormatter.format(duration / repetitions)
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
