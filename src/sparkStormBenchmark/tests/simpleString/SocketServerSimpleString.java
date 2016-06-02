package sparkStormBenchmark.tests.simpleString;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import sparkStormBenchmark.BenchmarkCore;
import sparkStormBenchmark.SocketServerCore;
import sparkStormBenchmark.StormCore;

/**
 * Feeder which writes <Test+i> into a writer (i=0...199).
 * @author Kaarel
 *
 */
public class SocketServerSimpleString extends SocketServerCore{

	public SocketServerSimpleString(String host, int port) {
		super(host, port);
	}

	/**
	 * Feed <Test+i> Sring to the provided writer. i = 0...199 
	 * @param socketOutWriter
	 * @param log
	 */
	public void feed(PrintWriter socketOutWriter) {
		Logger log = Logger.getLogger(BenchmarkCore.class.getName());
		int i = 0;
		log.info("SimpleStringFeeder - started batch, time " + System.nanoTime());
		while (true){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String msg = "Test"+i;
			socketOutWriter.println(msg);
			
			log.info("SimpleStringFeeder - wrote msg " + msg + ", time "+System.nanoTime());
			i+=1;
		}
		
//		log.info("SimpleStringFeeder - finished batch, time " + System.nanoTime());	
	}

	public static void main(String[] args){
		String host="localhost";
		try {
			host = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int port = 12000;
		SocketServerSimpleString server = new SocketServerSimpleString(host, port);
		server.run();
	}

}
