package statelessOperator;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import operator.Merger;
import operator.MergerSequential;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class StatelessOperator extends BaseRichBolt {

	private static final long serialVersionUID = 1113062442466819520L;

	private TransformationFunction transF;
	private OutputCollector collector;
	private ConcurrentLinkedQueue<Tuple> internalQueue;
	private Merger merger;
	
	public StatelessOperator(TransformationFunction transF) {
		this.transF = transF;
	}

	public void execute(Tuple arg0) {

		internalQueue.add(arg0);
		
		Tuple out = merger.getNextReady();
		while (out != null) {
			collector.emit(out.getValues());
		}

	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {

		internalQueue = new ConcurrentLinkedQueue<Tuple>();
		merger  = new MergerSequential();

		collector = arg2;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
