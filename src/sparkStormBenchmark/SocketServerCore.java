package sparkStormBenchmark;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

import sparkStormBenchmark.tests.simpleString.SocketServerSimpleString;

/** Abstract class for generating input data and hosting it on
 * a socket.
 * @author Kaarel
 *
 */
public abstract class SocketServerCore implements Runnable{
	static String host;
	static int port;
	static Logger log = Logger.getLogger(SocketServerCore.class.getName());

	public SocketServerCore(String host, int port){
		this.host = host;
		this.port = port;
	}


	@Override
	public void run() {

		System.out.println("SocketFeeder - starting at "+host+":"+port);

		// -- Wait for Spark/Storm to start--

		
		ServerSocket serverSocket = null;
		PrintWriter socketOutWriter = null;
		try {
			serverSocket = new ServerSocket(port);
			Socket clientSocket;
			clientSocket = serverSocket.accept();
			socketOutWriter = new PrintWriter(clientSocket.getOutputStream(), true);
			System.out.println("SocketFeeder - connected to client");
		} catch (IOException e) {
			e.printStackTrace();
		}
		log.info("SocketFeeder - starting at "+host+":"+port +", waiting 10 s");

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		System.out.println("SocketFeeder - Running");
		
		// Feed data into the writer
		
		feed(socketOutWriter);
		

		// close socket
		try{
			serverSocket.close();
		} catch(Exception e){
			//nothing
		}

	}
	
	/**
	 * Feeds data into the socketWriter. Must be overriden by extending class.
	 * @param socketOutWriter
	 */
	abstract public void feed(PrintWriter socketOutWriter);
	
//	public static void main(String[] args) throws Exception {
//		String host = InetAddress.getLocalHost().getHostAddress();
//		int port = 12000;
//
//		new Thread(new SocketServerCore(host,port)).start();
//	}
}