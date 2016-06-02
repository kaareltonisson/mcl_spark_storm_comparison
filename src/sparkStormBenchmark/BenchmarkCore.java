package sparkStormBenchmark;

import java.io.IOException;
import java.net.InetAddress;

import sparkStormBenchmark.tests.repeatedHashing.SparkRepeatedHashing;
import sparkStormBenchmark.tests.repeatedHashing.StormRepeatedHashing;
import sparkStormBenchmark.tests.simpleString.SocketServerSimpleString;
import sparkStormBenchmark.tests.simpleString.SparkSimpleString;
import sparkStormBenchmark.tests.simpleString.StormSimpleString;

/**
 * init socket stream
 * run Storm benchmarking
 * re-init socket stream
 * run Spark benchmarking
 * <compare results>
 * 
 * @author Kaarel
 *
 */
public class BenchmarkCore {
	String host;
	int port;
	String testName;

	static boolean doFeeder = false;
	static boolean doStorm = true;
	static boolean doSpark = false;
	
	/**
	 * Creates new BenchmarkCore.
	 * @param host
	 * @param port
	 * @param testName
	 */
	public BenchmarkCore(String host, int port, String testName){
		this.host = host;
		this.port = port;
		this.testName = testName;
	}

	/**
	 * Starts a socket feeder server in a new thread.
	 * @return Thread running the feeder server.
	 */
	private Thread startSocketFeeder(String testName){
		Thread SocketFeederThread = null;
		if (testName == "simpleString"){
			Runnable SocketFeeder = new SocketServerSimpleString(host,port);
		    SocketFeederThread = new Thread(SocketFeeder,"SimpleStringSocketFeederThread");
		}
		SocketFeederThread.start();
		return SocketFeederThread;
	}

	/**
	 * Starts a new thread which executes the Storm topology.
	 * @return Thread executing the Storm topology.
	 */
	private Thread startStormCore(String testName) {
		Thread stormThread = null;
		if (testName == "simpleString"){
			Runnable stormRunnable = new StormSimpleString(host,port);
			stormThread = new Thread(stormRunnable,"StormSimpleStringThread");
		}
		else if (testName == "repeatedHashing"){
			Runnable stormRunnable = new StormRepeatedHashing(host,port);
			stormThread = new Thread(stormRunnable,"StormRepeatedHashingThread");
		}
		else{
			//TODO: terminate
		}
		stormThread.start();	
		return stormThread;
	}

	/**
	 * Starts a new thread which executes Spark Streaming tasks.
	 */
	private void startSparkCore(String testName){
//		Thread sparkThread = null;
		if (testName == "simpleString"){
			SparkSimpleString.main(null);
//			Runnable sparkCore = new SparkSimpleString(host,port);
//			sparkThread = new Thread(sparkCore,"SparkSimpleStringThread");
		}
		else if (testName == "repeatedHashing"){
			SparkRepeatedHashing.main(null);
//			Runnable sparkCore = new SparkSimpleString(host,port);
//			sparkThread = new Thread(sparkCore,"SparkSimpleStringThread");
		}
		else{
			//TODO: terminate
		}
//		sparkThread.start();
//		return sparkThread;
	}

	/**
	 * Starts a new socket feeder, then deploys Storm and Spark to read from it.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException{
		String host = InetAddress.getLocalHost().getHostAddress();
//		String host = "10.132.0.2";
		int port = 12000;
		String testName = "repeatedHashing";
		
//		String host = args[0];
//		int port = Integer.parseInt(args[1]);
//		String testName = args[2];

		if (args.length != 1){
			System.out.println("Usage: 'BenchmarkCore spark' or 'BenchmarkCore storm'");
			return;
		}
		
		if (args[0].equals("storm")){
			doSpark = false;
			doStorm = true;
		}
		if (args[0].equals("spark")){
			doSpark = true;
			doStorm = false;
		}
		
		BenchmarkCore benchmarkCore = new BenchmarkCore(host,port,testName);

		// -- Start socket data feeder server --
		if (doFeeder){
			Thread socketFeederThread = benchmarkCore.startSocketFeeder(testName);
		}
		// -- Run Storm tests --
		if (doStorm){
			Thread stormCoreThread = benchmarkCore.startStormCore(testName);
		}
		// -- Run Spark Streaming tests --
		if (doSpark){
			benchmarkCore.startSparkCore(testName);
		}

		//TODO -- terminate automatically? --
		// what if processing takes a longer time?
//		while(socketFeederThread.isAlive()){
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			System.out.println("BenchmarkCore - SocketFeeder is alive");
//			//			StormCore.isRunning = false;
//		};

		System.out.println("BenchmarkCore - Reached End");
	}


}
