package sparkStormBenchmark.tests.simpleString;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


/**
 * Storm spout which listens on the specified socket for input
 * and emits the messages it reads.
 * @author Kaarel
 *
 */
public class SocketSpoutSimpleString implements IRichSpout {
	//The O/P collector
	static SpoutOutputCollector _collector;
	//The socket
	static Socket _clientSocket;
	static int _port;
	static String _host;

	static Logger log = Logger.getLogger(SocketSpoutSimpleString.class.getName());



	public SocketSpoutSimpleString(String host, int port){
		_host=host;
		_port=port;
	}

	public void open(Map conf,TopologyContext context, SpoutOutputCollector collector){
		System.out.println("SocketSpout - Starting");
		_collector=collector;
		while(_clientSocket==null){
			try {
				_clientSocket=new Socket(_host,_port);
				System.out.println("SocketSpout - Connected");

			} catch (IOException e) {
//				e.printStackTrace();
				log.info("Unable to connect to "+ _host +":"+ _port);
			}
		}
		System.out.println("SocketSpout - Ready");
	}   

	public void nextTuple() {
		//		System.out.println("Spout nextTuple start");

		try {
			BufferedReader in =
					new BufferedReader(
							new InputStreamReader(_clientSocket.getInputStream()));
			String line = null;
			while(line == null){
				line = in.readLine();
				log.info("SocketSpout - got msg "+ line + ", time "+ System.nanoTime());
			}
			_collector.emit(new Values(line));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//		System.out.println("Spout nextTuple end");

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
