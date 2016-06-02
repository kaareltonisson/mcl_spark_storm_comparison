package sparkStormBenchmark.tests.repeatedHashing;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import jline.internal.Log;

/**
 * Simple Storm bolt that takes in strings and outputs them to System.out.
 * @author Kaarel
 *
 */
public class TextBoltRepeatedHashing implements IRichBolt {
	OutputCollector _collector;
	static Logger log = Logger.getLogger(TextBoltRepeatedHashing.class.getName());
	Random rnd;		
			
	@Override

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
		rnd = new Random();

        System.out.println("TextBolt - prepared");
    }

	@Override
    public void execute(Tuple tuple) {
//        System.out.println("TextBolt - got msg : "+tuple.getString(0));
		String word = tuple.getString(0);
		byte[] wordBytes = word.getBytes();
		for (int i=0 ; i < 1000000; i++ ){
			int randInt = rnd.nextInt(wordBytes.length);
			wordBytes[randInt]+=randInt;
		}
		System.out.println(wordBytes.toString());
		_collector.emit(tuple, new Values(wordBytes.toString()));
        _collector.ack(tuple);
//        log.info("TextBolt - got msg: " + tuple + ", at time " + System.nanoTime());
    }

	@Override
	public void cleanup() {
		// NONE

	}

	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

	@Override
    public Map getComponentConfiguration() {
        return null;
    }

}
