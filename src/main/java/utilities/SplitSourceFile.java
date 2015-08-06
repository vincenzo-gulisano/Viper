package utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SplitSourceFile {

	public static void main(String[] args) throws IOException {

		String inputFile = args[0];
		int outputFiles = Integer.valueOf(args[1]);
		String outputFilePrefix = args[2];
		String outputFileSuffix = args[3];

		Random r = new Random();

		List<BufferedWriter> writers = new ArrayList<BufferedWriter>();
		for (int i = 0; i < outputFiles; i++) {
			writers.add(new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(outputFilePrefix + i
							+ outputFileSuffix)))));
		}

		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			for (String line; (line = br.readLine()) != null;) {
				int nextWriter = r.nextInt(outputFiles);
				writers.get(nextWriter).write(line);
				writers.get(nextWriter).newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (BufferedWriter w : writers) {
			w.flush();
			w.close();
		}

	}
}
