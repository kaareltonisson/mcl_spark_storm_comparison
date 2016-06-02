package sparkStormBenchmark.tests.simpleString;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * Collects data and outputs to file.
 * @author Kaarel
 *
 */
public class OutputBolt implements IRichBolt{
	OutputCollector _collector;
	static Logger log = Logger.getLogger(TextBoltSimpleString.class.getName());
	//TODO: get from conf instead
	String outputDir = "/home/kaareltonisson/storm_output";
	long time;
	
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		time = System.nanoTime();
        System.out.println("OutputBolt - prepared");

	}

	@Override
	public void execute(Tuple tuple) {
//		System.out.println("OutputBolt - got msg : "+tuple.getString(0));
		try{
			
			File file = new File(outputDir + "/" + time + "/SimpleStringStorm.out");
			if (!file.exists()){
				file.getParentFile().mkdirs();
				file.createNewFile();
				System.out.println("Created file: "+file.getAbsolutePath());
			}
			BufferedWriter output = new BufferedWriter(new
				FileWriter(file, true));
			output.newLine();
			output.append(tuple.getString(0));
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		_collector.ack(tuple);
//		System.out.println("OutputBolt - finished : "+tuple.getString(0));

	}
	
	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

}
