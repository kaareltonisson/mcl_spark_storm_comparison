package sparkStormBenchmark.tests.simpleString;

import java.util.Map;

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
public class TextBoltSimpleString implements IRichBolt {
	OutputCollector _collector;
	static Logger log = Logger.getLogger(TextBoltSimpleString.class.getName());
			
			
	@Override

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        System.out.println("TextBolt - prepared");
    }

	@Override
    public void execute(Tuple tuple) {
//        System.out.println("TextBolt - got msg : "+tuple.getString(0));
        _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
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
