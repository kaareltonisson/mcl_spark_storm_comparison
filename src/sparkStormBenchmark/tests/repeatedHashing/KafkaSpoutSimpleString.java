package sparkStormBenchmark.tests.repeatedHashing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.storm.hack.shade.com.google.common.collect.Iterables;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import kafkaProducers.StringProducer;


/**
 * Storm spout which listens on the specified socket for input
 * and emits the messages it reads.
 * @author Kaarel
 *
 */
public class KafkaSpoutSimpleString implements IRichSpout {
	//The O/P collector
	static SpoutOutputCollector _collector;
	//The socket
	static Socket _clientSocket;
	static int _port;
	static String _host;

	static Logger log = Logger.getLogger(KafkaSpoutSimpleString.class.getName());
	private KafkaConsumer consumer;
	private ConsumerRecords<String, String> records;
	private int recordIx = 0;

	public KafkaSpoutSimpleString(String host, int port){
		_host=host;
		_port=port;

	}

	public void open(Map conf,TopologyContext context, SpoutOutputCollector collector){
		System.out.println("SocketSpout - Starting");
		_collector=collector;
		//TODO: connect to Kafka
		Properties properties = new Properties();
		try {
			properties.load(StringProducer.class.getResourceAsStream("/kafka.properties"));
			properties.setProperty("group.id", "simpleString");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		consumer = new KafkaConsumer(properties);
		consumer.subscribe(Arrays.asList("simpleString"));
		records = consumer.poll(1000);
		System.out.println("KafkaSpout - Ready");
	}   

	public void nextTuple() {
		//		System.out.println("Spout nextTuple start");
//		System.out.println("KafkaSpout - Starting nextTuple()");

		int recordCount = 0;
		try {
			recordCount = records.count();
		}catch(NullPointerException e){
			// allow, do nothing
		}
//		System.out.println("record count - "+ recordCount+ ", recordIx - " + recordIx);

		// if used index has reached last, get new records
		while(recordIx >= recordCount){
				records = consumer.poll(1000);
				recordIx = 0;
				recordCount = records.count();
		}
		ConsumerRecord<String,String> record = Iterables.get(records, recordIx);
		recordIx += 1;
		_collector.emit(new Values(record.value()));

//		System.out.println("KafkaSpout - emitted " +record.value());

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
