package sparkStormBenchmark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import javax.sound.sampled.Line;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import sparkStormBenchmark.tests.simpleString.SparkSimpleString;

import org.apache.spark.streaming.kafka.*;
/**
 * DEPRECATED Core class for running Spark Streaming tests.
 * @author Kaarel
 *
 */
public abstract class SparkCore implements Runnable, Serializable {
	protected static final Pattern SPACE = Pattern.compile(" ");
	protected static String host;
	protected static int port;
	protected static String[] args;
	protected static JavaStreamingContext ssc;
	protected static SparkConf sparkConf;

	Logger log = Logger.getLogger(SparkCore.class.getName());

	public SparkCore(String host, int port) {
		this.host = host;
		this.port = port;
		// Create the context with a 1 second batch size
//		sparkConf = new SparkConf()
//				.setAppName("SparkTestA001");
////				.setMaster("local[2]");
//		ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
	}



	@Override
	public void run() {

		createAndRunSpark(this.ssc);

		System.out.println("SparkCore -  Starting");
		ssc.start();
		log.info("SparkCore -  Started, time " + System.nanoTime());
		ssc.awaitTermination();
		log.info("SparkCore -  Finished, time " + System.nanoTime());
		System.out.println("SparkCore -  Finished");	
	}	
	
	
public abstract void createAndRunSpark(JavaStreamingContext ssc);
		
//		public static void main(String[] args) {
//		if (args.length < 2) {
//			System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
//			System.exit(1);
//		}
//		SparkCore.args = args;
//		SparkCore sparkCore = new SparkCore(host,port,"simpleString");
//
//	}

}