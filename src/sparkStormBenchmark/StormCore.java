package sparkStormBenchmark;


import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import sparkStormBenchmark.tests.simpleString.StormSimpleString;

/**
 * Abstract class for creating and running Storm topologies.
 * @author Kaarel
 *
 */
public abstract class StormCore implements Runnable{
	protected static String host;
	protected static int port;
	
	protected static volatile boolean isRunning = true;

	protected static Logger log = Logger.getLogger(StormCore.class.getName());

	
	protected StormCore(String host, int port){
		this.host = host;
		this.port = port;
}

	public void startTopology(TopologyBuilder builder, String topologyName){
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(20);
		conf.setMaxSpoutPending(5000);

		StormTopology topology = builder.createTopology();

		try {
			StormSubmitter.submitTopology(topologyName, conf, topology);
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}
	
	@Override
	public void run() {
		System.out.println("StormCore - Starting");

/*
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("textSpout1", new SocketSpout(this.host,this.port));
		builder.setBolt("textBolt1", new TextBolt(), 1)
		.shuffleGrouping("textSpout1");
*/

		//        if (args != null && args.length > 0) {
		//            conf.setNumWorkers(1);
		//            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		//        } else {
//		System.out.println("StormCore - local mode starting");
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("test", conf, topology);


		// -- create and run topology, implemented by extending class --
		createAndRunTopology();
		
		while(isRunning){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
//		cluster.killTopology("test");
//		cluster.shutdown();
		//        }
		System.out.println("StormCore - Finished");
	}
	
	/**
	 * Creates and runs the topology. Must be overriden by extending class.
	 */
	abstract public void createAndRunTopology();
	
	
//	public static void main(String[] args){
//		if (args.length < 3){
//			System.out.println("Arguments: host port testName");
//		}
//		String host = args[0];
//		int port = Integer.parseInt(args[1]);
//		String testName = args[2];
//		StormCore core = new StormCore(host,port,testName);
//		TopologyBuilder builder = new StormSimpleString(core.host,core.port);
//		String topologyName = "simpleStringTopology";
//		core.startTopology(builder, topologyName);
//	}
}
