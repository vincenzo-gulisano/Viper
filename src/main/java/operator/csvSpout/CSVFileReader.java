package operator.csvSpout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Values;

public abstract class CSVFileReader implements Serializable {

	private static final long serialVersionUID = 731805990867001770L;
	private BufferedReader br;
	private String nextLine;
	private boolean hasNext;

	public CSVFileReader() {
		this.nextLine = "";
		this.hasNext = true;
	}

	public void setup(String fileName) {
		try {
			this.br = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public boolean hasNext() {
		if (hasNext)
			try {
				if ((nextLine = br.readLine()) == null) {
					br.close();
					hasNext = false;
					System.out.println("Input file has been read!!!");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		return hasNext;
	}

	public Values getNextTuple() {
		return convertLineToTuple(nextLine);
	}

	public void close() {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected abstract Values convertLineToTuple(String line);

}
