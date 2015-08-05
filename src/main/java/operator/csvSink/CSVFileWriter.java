package operator.csvSink;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Tuple;

public abstract class CSVFileWriter implements Serializable {

	private static final long serialVersionUID = -1692435045526854126L;
	private boolean closed;

	public void setForceFlushing() {
	}

	private PrintWriter pw;

	public CSVFileWriter() {
		closed = false;
	}

	public void setup(String fileName) {
		try {
			this.pw = new PrintWriter(new FileWriter(fileName));
			closed = false;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void write(String s) {
		pw.println(s);
	}

	public void close() {
		if (!closed) {
			pw.flush();
			pw.close();
			closed = true;
		}
	}

	protected abstract String convertTupleToLine(Tuple t);

	protected void flushReceived() {

	}

}
