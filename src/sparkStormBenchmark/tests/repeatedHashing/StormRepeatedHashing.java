package sparkStormBenchmark.tests.repeatedHashing;

import org.apache.storm.topology.TopologyBuilder;

import sparkStormBenchmark.StormCore;

public class StormRepeatedHashing extends StormCore{

	static int repeatCount = 1;
	
	
	public StormRepeatedHashing(String host, int port) {
		super(host, port);
	}


	/**
	 * Creates the topology.
	 * @param host
	 * @param port
	 * @return
	 */
	public TopologyBuilder createTopology(String host, int port){
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("textSpout1", new KafkaSpoutSimpleString(host, port), 1);
		builder.setBolt("textHashBolt0", new TextBoltRepeatedHashing(), 3)
			.shuffleGrouping("textSpout1");
//		builder.setBolt("outputBolt", new OutputBolt(), 1)
//			.shuffleGrouping("textHashBolt1");
		String prevBoltName = "textHashBolt0";
		for (int i=1 ; i < repeatCount; i++){
			String boltName = "textHashBolt"+i;
			builder.setBolt(boltName, new TextBoltRepeatedHashing(), 2)
			.shuffleGrouping(prevBoltName);
			prevBoltName = boltName;
		}
		
		return builder;
	}
	
	/**
	 * Calls topology creation and starts the topology.
	 */
	public void createAndRunTopology(){
		TopologyBuilder builder = createTopology(this.host,this.port);
		String topologyName = "repeatedHashingTopology";
		startTopology(builder, topologyName);	
	}
}
