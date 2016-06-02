package sparkStormBenchmark.tests.repeatedHashing;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import scala.Tuple2;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import sparkStormBenchmark.SparkCore;

public class SparkRepeatedHashing {
	static int repeatCount = 10;
	/**
	 * Creates and runs Spark Streaming task.
	 */
	public static void main(String[] args) {
		// Create the context with a 1 second batch size
		System.out.println("Starting config");

		SparkConf sparkConf = new SparkConf()
				.setAppName("SparkRepeatedHashing");
//				.setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));
		
		int numThreads = 1;
		Set<String> topicsSet = new HashSet<>(Arrays.asList("simpleString"));
		Map<String, String> kafkaParams = new HashMap<>();
	    kafkaParams.put("metadata.broker.list", "instance-1:9092");

	    JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(
				ssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class,
				kafkaParams, 
				topicsSet
		);
		
		
//		// -- create Kafka stream --
//		JavaPairReceiverInputDStream<String, String> kafkaStream = 
//			     KafkaUtils.createStream(ssc,
//			     "instance-1:2181", "simpleString", topicMap);

		// -- get values from Kafka stream, discard topic --
	    JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, String>, String>() {
	        @Override
	        public String call(Tuple2<String, String> tuple2) {
	          return tuple2._2();
	        }
	      });
		
	    // -- perform actions on each value
	    for (int j = 0; j < repeatCount; j++){
		JavaDStream<String> mapped = lines.map(new Function<String, String>() {
	        @Override
	        public String call(String str) {
//	        	Logger log = Logger.getLogger(SparkRepeatedHashing.class.getName());
//	        	log.info("SparkSimpleString - got line : " + str + ", time " + System.nanoTime());
//	        	
	    		byte[] wordBytes = str.getBytes();
	    		Random rnd = new Random();
	    		for (int i=0 ; i < 1000000; i++ ){
	    			int randInt = rnd.nextInt(wordBytes.length);
	    			wordBytes[randInt]+=randInt;
	    		}
	        	System.out.println(wordBytes.toString());
	          return wordBytes.toString();
	        }
	      });
		mapped.print();

	    }
		
	    // --
//		mapped.foreachRDD(
//				new VoidFunction<JavaRDD<String>>(){
//					public void call(JavaRDD<String> rdd) throws Exception {
////			        	Logger log = Logger.getLogger(SparkCore.class.getName());
////						log.info("SparkSimpleString - got rdd : " + rdd + ", time " + System.nanoTime());
//						rdd.foreach(
//								new VoidFunction<String>() {
//									public void call(String str) throws Exception {
//										Logger log = Logger.getLogger(SparkCore.class.getName());
//										log.info("SparkSimpleString - got str : " + str + ", time " + System.nanoTime());
//									}
//								}
//						);
//					}
//				}
//		);
		System.out.println("Starting ssc");
		ssc.start();
		System.out.println("ssc started");
		ssc.awaitTermination();
		System.out.println("ssc terminated");

	}
	
}
